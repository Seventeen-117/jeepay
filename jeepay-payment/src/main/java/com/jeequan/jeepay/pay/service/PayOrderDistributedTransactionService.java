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
import com.jeequan.jeepay.core.entity.PaymentRecord;
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
import com.jeequan.jeepay.pay.service.FallbackPaymentService;
import com.jeequan.jeepay.service.impl.MchPayPassageService;
import com.jeequan.jeepay.service.impl.PayOrderService;
import com.jeequan.jeepay.service.impl.PaymentRecordService;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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
    @Autowired private RedissonClient redissonClient;
    @Autowired private PaymentRecordService paymentRecordService;
    @Autowired private FallbackPaymentService fallbackPaymentService;
    
    // 分布式锁前缀，用于确保跨渠道支付的严格互斥
    private static final String PAYMENT_LOCK_PREFIX = "payment_lock:";
    
    // 支付通道权重缓存，用于智能路由
    private final Map<String, Integer> channelWeightCache = new ConcurrentHashMap<>();
    
    // 支付通道成功率缓存，用于智能路由
    private final Map<String, Double> channelSuccessRateCache = new ConcurrentHashMap<>();
    
    /**
     * 处理支付交易，实现两阶段提交协议
     * 1. 准备阶段：检查订单状态，获取支付接口，获取分布式锁
     * 2. 提交阶段：调用支付接口，处理支付结果
     * 3. 补偿机制：如果支付接口调用失败，使用备用渠道重试
     */
    @Transactional(rollbackFor = Exception.class)
    public ChannelRetMsg handlePayTransaction(String wayCode, UnifiedOrderRQ bizRQ, PayOrder payOrder,
                                           MchAppConfigContext mchAppConfigContext) {
        
        // 生成分布式锁的key，确保同一订单的支付操作互斥
        String lockKey = PAYMENT_LOCK_PREFIX + payOrder.getPayOrderId();
        boolean lockAcquired = false;
        
        try {
            // 准备阶段：获取分布式锁
            lockAcquired = acquireDistributedLock(lockKey, 30);
            if (!lockAcquired) {
                log.warn("获取支付订单分布式锁失败，订单号: {}", payOrder.getPayOrderId());
                return ChannelRetMsg.sysError("系统繁忙，请稍后再试");
            }
            
            // 检查订单状态，确保幂等性
            PayOrder dbPayOrder = payOrderService.getById(payOrder.getPayOrderId());
            if (dbPayOrder != null && dbPayOrder.getState() != PayOrder.STATE_INIT) {
                log.info("订单已处理，直接返回结果，订单号: {}, 状态: {}", payOrder.getPayOrderId(), dbPayOrder.getState());
                
                // 根据订单状态返回对应结果
                if (dbPayOrder.getState() == PayOrder.STATE_SUCCESS) {
                    return ChannelRetMsg.confirmSuccess(null);
                } else if (dbPayOrder.getState() == PayOrder.STATE_FAIL) {
                    return ChannelRetMsg.confirmFail(null);
                } else {
                    return ChannelRetMsg.waiting();
                }
            }
            
            // 获取支付通道
            MchPayPassage mchPayPassage = mchPayPassageService.findMchPayPassage(
                    mchAppConfigContext.getMchNo(), mchAppConfigContext.getAppId(), wayCode);
            
            if (mchPayPassage == null) {
                log.error("商户未配置支付通道，商户号: {}, 应用ID: {}, 支付方式: {}", 
                        mchAppConfigContext.getMchNo(), mchAppConfigContext.getAppId(), wayCode);
                return ChannelRetMsg.sysError("商户未配置支付通道");
            }
            
            // 检查支付通道是否可用（未熔断）
            if (!paymentTransactionManager.isChannelAvailable(mchPayPassage.getIfCode())) {
                log.warn("支付通道已熔断，切换到备用通道，通道代码: {}", mchPayPassage.getIfCode());
                return handleChannelFailure(wayCode, bizRQ, payOrder, mchAppConfigContext);
            }
            
            // 获取支付接口实现
            IPaymentService paymentService = getPaymentService(mchAppConfigContext, mchPayPassage);
            
            // 设置订单的支付通道
            payOrder.setIfCode(mchPayPassage.getIfCode());
            
            // 记录支付渠道调用开始
            paymentTransactionManager.recordCallStart(mchPayPassage.getIfCode());
            long startTime = System.currentTimeMillis();
            
            try {
                // 提交阶段：调用支付接口处理支付
                ChannelRetMsg retMsg = processPayment(wayCode, bizRQ, payOrder, 
                        mchAppConfigContext, mchPayPassage, paymentService);
                
                // 更新通道权重和成功率
                updateChannelMetrics(mchPayPassage.getIfCode(), true);
                
                // 记录支付记录，确保资金对账一致性
                if (retMsg.getChannelState() == ChannelRetMsg.ChannelState.CONFIRM_SUCCESS) {
                    PaymentRecord paymentRecord = new PaymentRecord();
                    paymentRecord.setOrderNo(payOrder.getPayOrderId());
                    paymentRecord.setAmount(new BigDecimal(payOrder.getAmount()));
                    paymentRecord.setChannel(mchPayPassage.getIfCode());
                    paymentRecord.setCreateTime(new Date());
                    paymentRecord.setUpdateTime(new Date());
                    
                    paymentRecordService.save(paymentRecord);
                }
                
                return retMsg;
                
            } catch (Exception e) {
                // 记录渠道调用失败
                paymentTransactionManager.recordCallFailure(mchPayPassage.getIfCode(), 
                        System.currentTimeMillis() - startTime);
                
                // 更新通道权重和成功率
                updateChannelMetrics(mchPayPassage.getIfCode(), false);
                
                // 补偿机制：使用备用渠道重试
                log.error("支付渠道处理失败，尝试使用备用渠道，原因: {}", e.getMessage(), e);
                return handleChannelFailure(wayCode, bizRQ, payOrder, mchAppConfigContext);
            }
            
        } catch (Exception e) {
            log.error("处理支付交易异常: {}", e.getMessage(), e);
            return ChannelRetMsg.sysError("支付处理失败，请稍后再试");
        } finally {
            // 释放分布式锁
            if (lockAcquired) {
                releaseDistributedLock(lockKey);
            }
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
            updateOrderState(payOrder, PayOrder.STATE_ING);
            
            // 从AbstractRS中提取ChannelRetMsg
            ChannelRetMsg channelRetMsg;
            if (abstractRS instanceof UnifiedOrderRS) {
                channelRetMsg = ((UnifiedOrderRS) abstractRS).getChannelRetMsg();
            } else {
                // 默认等待支付结果
                channelRetMsg = ChannelRetMsg.waiting();
            }
            
            // 记录渠道调用成功
            paymentTransactionManager.recordCallSuccess(mchPayPassage.getIfCode(), System.currentTimeMillis());
            
            return channelRetMsg;
            
        } catch (ChannelException e) {
            // 记录渠道调用失败
            paymentTransactionManager.recordCallFailure(mchPayPassage.getIfCode(), System.currentTimeMillis());
            
            // 抛出异常，由调用者处理
            throw e;
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
            // 记录原始渠道
            String originalIfCode = payOrder.getIfCode();
            
            // 使用FallbackPaymentService处理备用支付
            return fallbackPaymentService.fallbackPayment(wayCode, bizRQ, payOrder, mchAppConfigContext);
            
        } catch (Exception e) {
            log.error("备用支付通道处理失败: {}", e.getMessage(), e);
            return ChannelRetMsg.sysError("支付处理失败，请稍后再试");
        }
    }
    
    /**
     * 查找可用的备用支付通道，排除指定的主通道
     * @param mchNo 商户号
     * @param appId 应用ID
     * @param wayCode 支付方式
     * @param primaryIfCode 主通道代码（需要排除）
     * @return 可用的备用通道列表
     */
    private List<MchPayPassage> findAvailableBackupChannels(String mchNo, String appId, String wayCode, String primaryIfCode) {
        // 获取所有支持该支付方式的通道
        List<MchPayPassage> allPassages = mchPayPassageService.findAvailablePayPassageByWayCode(mchNo, appId, wayCode);
        
        // 过滤掉主通道和已熔断的通道
        return allPassages.stream()
                .filter(passage -> !passage.getIfCode().equals(primaryIfCode)) // 排除主通道
                .filter(passage -> paymentTransactionManager.isChannelAvailable(passage.getIfCode())) // 只保留可用通道
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
        List<MchPayPassage> availablePassages = findAvailableBackupChannels(mchNo, appId, wayCode, null);
        
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
        // 使用Redisson实现分布式锁
        try {
            RLock lock = redissonClient.getLock(lockKey);
            return lock.tryLock(1, expireSeconds, TimeUnit.SECONDS);
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
            RLock lock = redissonClient.getLock(lockKey);
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
                log.debug("释放支付订单分布式锁: {}", lockKey);
            }
        } catch (Exception e) {
            log.error("释放分布式锁异常: {}", e.getMessage(), e);
        }
    }

    /**
     * 更新订单状态并设置MQ延迟消息
     * @param payOrder 支付订单
     * @param newState 新状态
     */
    private void updateOrderState(PayOrder payOrder, byte newState) {
        if (payOrder.getState() == newState) {
            return; // 状态未变化，无需更新
        }
        
        // 更新订单状态
        if (newState == PayOrder.STATE_ING) {
            payOrderService.updateInit2Ing(payOrder.getPayOrderId(), payOrder);
            
            // 设置MQ延迟消息，用于支付结果查询和补单
            mqSender.send(PayOrderReissueMQ.build(payOrder.getPayOrderId(), 1), 60); // 60秒后查询一次支付结果
            
        } else if (newState == PayOrder.STATE_SUCCESS) {
            payOrderService.updateIng2Success(payOrder.getPayOrderId(), null, null);
            
        } else if (newState == PayOrder.STATE_FAIL) {
            payOrderService.updateIng2Fail(payOrder.getPayOrderId(), null, null, null, null);
        }
        
        // 更新内存中的订单状态
        payOrder.setState(newState);
    }
} 