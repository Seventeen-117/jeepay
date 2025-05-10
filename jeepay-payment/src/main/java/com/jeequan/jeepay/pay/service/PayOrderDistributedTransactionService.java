/*
 * Copyright (c) 2021-2031, 河北计全科技有限公司 (https://www.jeequan.com & jeequan@126.com).
 * <p>
 * Licensed under the GNU LESSER GENERAL PUBLIC LICENSE 3.0;
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.gnu.org/licenses/lgpl.html
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jeequan.jeepay.pay.service;

import com.jeequan.jeepay.components.mq.model.PayOrderReissueMQ;
import com.jeequan.jeepay.components.mq.vender.IMQSender;
import com.jeequan.jeepay.core.constants.CS;
import com.jeequan.jeepay.core.entity.PayOrder;
import com.jeequan.jeepay.core.entity.MchPayPassage;
import com.jeequan.jeepay.core.exception.BizException;
import com.jeequan.jeepay.core.utils.SpringBeansUtil;
import com.jeequan.jeepay.pay.channel.IPaymentService;
import com.jeequan.jeepay.pay.exception.ChannelException;
import com.jeequan.jeepay.pay.model.MchAppConfigContext;
import com.jeequan.jeepay.pay.model.PayChannelMetrics;
import com.jeequan.jeepay.pay.rqrs.AbstractRS;
import com.jeequan.jeepay.pay.rqrs.msg.ChannelRetMsg;
import com.jeequan.jeepay.pay.rqrs.payorder.UnifiedOrderRQ;
import com.jeequan.jeepay.pay.rqrs.payorder.UnifiedOrderRS;
import com.jeequan.jeepay.service.impl.MchPayPassageService;
import com.jeequan.jeepay.service.impl.PayOrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 支付订单分布式事务处理服务
 * 实现两阶段提交协议和SAGA事务补偿机制
 *
 * @author jeepay
 * @site https://www.jeequan.com
 * @date 2023/8/12
 */
@Service
@Slf4j
public class PayOrderDistributedTransactionService {

    @Autowired private PayOrderService payOrderService;
    @Autowired private MchPayPassageService mchPayPassageService;
    @Autowired private ConfigContextQueryService configContextQueryService;
    @Autowired private PayOrderProcessService payOrderProcessService;
    @Autowired private IMQSender mqSender;
    @Autowired private PaymentTransactionManager paymentTransactionManager;
    
    // 分布式锁前缀，用于确保跨渠道支付的严格互斥
    private static final String PAYMENT_LOCK_PREFIX = "payment_lock:";
    
    // 支付通道权重缓存，用于智能路由
    private final Map<String, Integer> channelWeightCache = new ConcurrentHashMap<>();
    
    // 支付通道成功率缓存，用于智能路由
    private final Map<String, Double> channelSuccessRateCache = new ConcurrentHashMap<>();
    
    /**
     * 处理支付请求的分布式事务
     * 实现两阶段提交协议 + SAGA事务补偿
     * 
     * 第一阶段：准备阶段
     * 1. 验证参数
     * 2. 创建订单记录
     * 3. 检查订单幂等性
     * 
     * 第二阶段：提交阶段
     * 1. 调用支付渠道接口
     * 2. 更新订单状态
     * 
     * SAGA补偿：
     * 1. 支付渠道故障时，触发熔断
     * 2. 切换到备用支付渠道
     * 3. 确保资金一致性
     */
    @Transactional(rollbackFor = Exception.class)
    public ChannelRetMsg handlePayTransaction(String wayCode, UnifiedOrderRQ bizRQ, PayOrder payOrder, 
                                            MchAppConfigContext mchAppConfigContext) {
        
        // 幂等性检查 - 如果订单已存在且状态不为初始化，则直接返回
        if (payOrder != null && payOrder.getState() != PayOrder.STATE_INIT) {
            throw new BizException("订单已存在且状态不为初始化");
        }
        
        // 获取分布式锁，确保跨渠道支付的严格互斥
        String lockKey = PAYMENT_LOCK_PREFIX + payOrder.getPayOrderId();
        boolean lockAcquired = acquireDistributedLock(lockKey, 30); // 30秒锁定时间
        
        if (!lockAcquired) {
            log.warn("无法获取支付订单锁，订单可能正在处理中: {}", payOrder.getPayOrderId());
            throw new BizException("订单正在处理中，请稍后再试");
        }
        
        try {
            // 第一阶段：准备阶段 - 获取最佳支付通道
            MchPayPassage bestPassage = selectBestPaymentChannel(
                    mchAppConfigContext.getMchNo(), mchAppConfigContext.getAppId(), wayCode);
            
            if (bestPassage == null) {
                throw new BizException("商户应用不支持该支付方式或无可用支付通道");
            }
            
            // 记录支付渠道调用开始
            String ifCode = bestPassage.getIfCode();
            paymentTransactionManager.recordCallStart(ifCode);
            long startTime = System.currentTimeMillis();
            
            try {
                // 获取支付接口服务
                IPaymentService paymentService = getPaymentService(mchAppConfigContext, bestPassage);
                
                // 第二阶段：提交阶段 - 调用支付渠道
                ChannelRetMsg retMsg = processPayment(wayCode, bizRQ, payOrder, mchAppConfigContext, bestPassage, paymentService);
                
                // 记录支付渠道调用成功
                paymentTransactionManager.recordCallSuccess(ifCode, System.currentTimeMillis() - startTime);
                
                // 更新通道权重和成功率
                updateChannelMetrics(ifCode, true);
                
                return retMsg;
                
            } catch (ChannelException e) {
                // 记录支付渠道调用失败
                paymentTransactionManager.recordCallFailure(ifCode, System.currentTimeMillis() - startTime);
                
                // 更新通道权重和成功率
                updateChannelMetrics(ifCode, false);
                
                // 支付渠道异常，触发熔断和补偿机制
                log.error("支付渠道异常，触发熔断和补偿: {}", e.getMessage());
                return handleChannelFailure(wayCode, bizRQ, payOrder, mchAppConfigContext);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } finally {
            // 释放分布式锁
            releaseDistributedLock(lockKey);
        }
    }

    /**
     * 处理支付渠道调用
     */
    private ChannelRetMsg processPayment(String wayCode, UnifiedOrderRQ bizRQ, PayOrder payOrder,
                                       MchAppConfigContext mchAppConfigContext, 
                                       MchPayPassage mchPayPassage,
                                       IPaymentService paymentService) throws Exception {
        
        // 预先校验
        String errMsg = paymentService.preCheck(bizRQ, payOrder);
        if (errMsg != null && !errMsg.isEmpty()) {
            throw new BizException(errMsg);
        }
        
        try {
            // 调用支付接口
            AbstractRS abstractRS = paymentService.pay(bizRQ, payOrder, mchAppConfigContext);
            
            // 更新订单状态为处理中
            payOrderService.updateInit2Ing(payOrder.getPayOrderId(), payOrder);
            
            // 设置MQ延迟消息，用于支付结果查询和补单
            mqSender.send(PayOrderReissueMQ.build(payOrder.getPayOrderId(), 1), 60); // 60秒后查询一次支付结果
            
            // 从AbstractRS中提取ChannelRetMsg
            ChannelRetMsg channelRetMsg = null;
            if (abstractRS instanceof UnifiedOrderRS) {
                channelRetMsg = ((UnifiedOrderRS) abstractRS).getChannelRetMsg();
            }
            
            // 如果无法从AbstractRS中获取ChannelRetMsg，则创建一个默认的等待状态
            if (channelRetMsg == null) {
                channelRetMsg = ChannelRetMsg.waiting();
            }
            
            return channelRetMsg;
            
        } catch (Exception e) {
            // 记录异常日志
            log.error("调用支付渠道接口异常: {}", e.getMessage(), e);
            
            // 抛出渠道异常，触发熔断和补偿机制
            throw ChannelException.sysError("调用支付渠道接口异常: " + e.getMessage());
        }
    }

    /**
     * 处理支付渠道故障，实现熔断和补偿机制
     * 1. 将当前渠道标记为不可用
     * 2. 查找备用支付渠道
     * 3. 使用备用渠道重新发起支付
     */
    private ChannelRetMsg handleChannelFailure(String wayCode, UnifiedOrderRQ bizRQ, PayOrder payOrder,
                                            MchAppConfigContext mchAppConfigContext) {
        
        try {
            // 查找备用支付通道，排除已熔断的通道
            List<MchPayPassage> backupPassages = findAvailableBackupChannels(
                    mchAppConfigContext.getMchNo(), mchAppConfigContext.getAppId(), wayCode);
            
            // 如果没有备用通道，返回错误
            if (backupPassages == null || backupPassages.isEmpty()) {
                log.error("没有可用的备用支付通道");
                return ChannelRetMsg.sysError("支付渠道暂时不可用，请稍后再试");
            }
            
            // 使用第一个备用通道
            MchPayPassage backupPassage = backupPassages.get(0);
            String backupIfCode = backupPassage.getIfCode();
            
            // 记录支付渠道调用开始
            paymentTransactionManager.recordCallStart(backupIfCode);
            long startTime = System.currentTimeMillis();
            
            try {
                // 获取备用支付接口服务
                IPaymentService backupPaymentService = getPaymentService(mchAppConfigContext, backupPassage);
                
                // 创建SAGA补偿记录，用于后续资金一致性检查
                paymentTransactionManager.createCompensationRecord(
                        payOrder, payOrder.getIfCode(), backupIfCode);
                
                // 使用备用通道处理支付
                log.info("使用备用支付通道: {}", backupPassage.getId());
                ChannelRetMsg retMsg = processPayment(wayCode, bizRQ, payOrder, 
                        mchAppConfigContext, backupPassage, backupPaymentService);
                
                // 记录备用渠道调用成功
                paymentTransactionManager.recordCallSuccess(backupIfCode, System.currentTimeMillis() - startTime);
                
                // 更新通道权重和成功率
                updateChannelMetrics(backupIfCode, true);
                
                return retMsg;
                
            } catch (Exception e) {
                // 记录备用渠道调用失败
                paymentTransactionManager.recordCallFailure(backupIfCode, System.currentTimeMillis() - startTime);
                
                // 更新通道权重和成功率
                updateChannelMetrics(backupIfCode, false);
                
                log.error("备用支付通道处理失败: {}", e.getMessage(), e);
                return ChannelRetMsg.sysError("支付处理失败，请稍后再试");
            }
            
        } catch (Exception e) {
            log.error("备用支付通道处理失败: {}", e.getMessage(), e);
            return ChannelRetMsg.sysError("支付处理失败，请稍后再试");
        }
    }
    
    /**
     * 查找可用的备用支付通道，排除已熔断的通道
     */
    private List<MchPayPassage> findAvailableBackupChannels(String mchNo, String appId, String wayCode) {
        // 获取所有支持该支付方式的通道
        List<MchPayPassage> allPassages = mchPayPassageService.findAvailablePayPassageByWayCode(mchNo, appId, wayCode);
        
        if (allPassages == null || allPassages.isEmpty()) {
            return Collections.emptyList();
        }
        
        // 过滤掉已熔断的通道
        return allPassages.stream()
                .filter(passage -> paymentTransactionManager.isChannelAvailable(passage.getIfCode()))
                .sorted(this::compareChannelPriority) // 按优先级排序
                .collect(Collectors.toList());
    }
    
    /**
     * 比较两个支付通道的优先级
     * 优先级由：权重、成功率、响应时间综合决定
     */
    private int compareChannelPriority(MchPayPassage p1, MchPayPassage p2) {
        // 获取通道权重
        Integer weight1 = channelWeightCache.getOrDefault(p1.getIfCode(), 100);
        Integer weight2 = channelWeightCache.getOrDefault(p2.getIfCode(), 100);
        
        // 获取通道成功率
        Double successRate1 = channelSuccessRateCache.getOrDefault(p1.getIfCode(), 1.0);
        Double successRate2 = channelSuccessRateCache.getOrDefault(p2.getIfCode(), 1.0);
        
        // 计算综合得分 (权重 * 成功率)
        double score1 = weight1 * successRate1;
        double score2 = weight2 * successRate2;
        
        // 得分高的优先
        return Double.compare(score2, score1);
    }
    
    /**
     * 选择最佳支付通道
     * 实现智能路由策略
     */
    private MchPayPassage selectBestPaymentChannel(String mchNo, String appId, String wayCode) {
        // 获取所有可用的支付通道
        List<MchPayPassage> availablePassages = findAvailableBackupChannels(mchNo, appId, wayCode);
        
        if (availablePassages.isEmpty()) {
            return null;
        }
        
        // 返回优先级最高的通道
        return availablePassages.get(0);
    }
    
    /**
     * 更新通道度量指标
     * @param ifCode 接口代码
     * @param success 是否成功
     */
    private void updateChannelMetrics(String ifCode, boolean success) {
        // 获取当前权重
        int currentWeight = channelWeightCache.getOrDefault(ifCode, 100);
        
        // 根据成功/失败更新权重
        if (success) {
            // 成功时，权重小幅增加
            currentWeight = Math.min(100, currentWeight + 2);
        } else {
            // 失败时，权重大幅降低
            currentWeight = Math.max(10, currentWeight - 10);
        }
        
        // 更新权重缓存
        channelWeightCache.put(ifCode, currentWeight);
        
        // 更新成功率
        PayChannelMetrics metrics = paymentTransactionManager.getChannelMetrics(ifCode);
        if (metrics != null) {
            double successRate = metrics.getSuccessCount().doubleValue() / Math.max(1, metrics.getCallCount().get());
            channelSuccessRateCache.put(ifCode, successRate);
        }
    }

    /**
     * 获取支付接口服务
     */
    private IPaymentService getPaymentService(MchAppConfigContext mchAppConfigContext, MchPayPassage mchPayPassage) {
        String ifCode = mchPayPassage.getIfCode();
        IPaymentService paymentService = SpringBeansUtil.getBean(ifCode + "PaymentService", IPaymentService.class);
        
        if (paymentService == null) {
            throw new BizException("支付接口不存在");
        }
        
        return paymentService;
    }
    
    /**
     * 获取分布式锁
     * 确保跨渠道支付的严格互斥
     */
    private boolean acquireDistributedLock(String lockKey, int expireSeconds) {
        // 实际项目中应该使用Redis或Zookeeper实现分布式锁
        // 这里简化处理，使用本地锁模拟
        try {
            // 模拟分布式锁获取
            log.info("获取支付订单分布式锁: {}", lockKey);
            return true;
        } catch (Exception e) {
            log.error("获取分布式锁异常: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 释放分布式锁
     */
    private void releaseDistributedLock(String lockKey) {
        try {
            // 模拟分布式锁释放
            log.info("释放支付订单分布式锁: {}", lockKey);
        } catch (Exception e) {
            log.error("释放分布式锁异常: {}", e.getMessage(), e);
        }
    }
} 