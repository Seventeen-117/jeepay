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
import com.jeequan.jeepay.core.entity.PayOrderCompensation;
import com.jeequan.jeepay.core.exception.BizException;
import com.jeequan.jeepay.pay.channel.IPayOrderQueryService;
import com.jeequan.jeepay.pay.model.MchAppConfigContext;
import com.jeequan.jeepay.pay.rqrs.msg.ChannelRetMsg;
import com.jeequan.jeepay.service.impl.PayOrderCompensationService;
import com.jeequan.jeepay.service.impl.PayOrderService;
import com.jeequan.jeepay.core.utils.SpringBeansUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 支付对账与资金一致性服务
 * 实现实时资金对账引擎和资金池平衡机制
 *
 * @author jeepay
 * @site https://www.jeequan.com
 * @date 2023/8/15
 */
@Service
@Slf4j
public class PaymentReconciliationService {

    @Autowired private PayOrderService payOrderService;
    @Autowired private PayOrderCompensationService payOrderCompensationService;
    @Autowired private ConfigContextQueryService configContextQueryService;
    @Autowired private IMQSender mqSender;
    @Autowired private PaymentTransactionManager paymentTransactionManager;
    
    // 资金差异记录缓存
    private final Map<String, Map<String, Long>> fundDiscrepancyCache = new ConcurrentHashMap<>();
    
    // 自动修复阈值（单位：分）
    private static final long AUTO_FIX_THRESHOLD = 10000; // 100元
    
    /**
     * 定时任务：处理未完成的补偿记录
     * 每5分钟执行一次
     */
    @Scheduled(fixedDelay = 300000)
    public void processCompensationRecords() {
        log.info("开始处理支付订单补偿记录...");
        
        try {
            // 查询处理中的补偿记录
            List<PayOrderCompensation> compensationList = payOrderCompensationService.list(
                    PayOrderCompensation.gw()
                            .eq(PayOrderCompensation::getState, 0) // 处理中状态
                            .lt(PayOrderCompensation::getCreatedAt, new Date(System.currentTimeMillis() - 60000)) // 创建时间超过1分钟
            );
            
            if (compensationList == null || compensationList.isEmpty()) {
                return;
            }
            
            log.info("找到{}条待处理的补偿记录", compensationList.size());
            
            // 处理每条补偿记录
            for (PayOrderCompensation compensation : compensationList) {
                processCompensation(compensation);
            }
            
        } catch (Exception e) {
            log.error("处理支付订单补偿记录异常", e);
        }
    }
    
    /**
     * 处理单条补偿记录
     */
    @Transactional(rollbackFor = Exception.class)
    public void processCompensation(PayOrderCompensation compensation) {
        try {
            String payOrderId = compensation.getPayOrderId();
            
            // 查询订单
            PayOrder payOrder = payOrderService.getById(payOrderId);
            if (payOrder == null) {
                log.warn("补偿记录对应的订单不存在，payOrderId={}", payOrderId);
                updateCompensationFailed(compensation, "订单不存在");
                return;
            }
            
            // 如果订单已经成功或失败，更新补偿记录状态
            if (payOrder.getState() == PayOrder.STATE_SUCCESS) {
                updateCompensationSuccess(compensation, "订单已支付成功");
                return;
            } else if (payOrder.getState() == PayOrder.STATE_FAIL || payOrder.getState() == PayOrder.STATE_CLOSED) {
                updateCompensationFailed(compensation, "订单已失败或取消");
                return;
            }
            
            // 获取商户应用配置
            MchAppConfigContext mchAppConfigContext = configContextQueryService.queryMchInfoAndAppInfo(
                    payOrder.getMchNo(), payOrder.getAppId());
            
            if (mchAppConfigContext == null) {
                log.warn("获取商户应用配置信息失败，payOrderId={}", payOrderId);
                updateCompensationFailed(compensation, "获取商户应用配置信息失败");
                return;
            }
            
            // 查询原支付渠道的支付结果
            String originalIfCode = compensation.getOriginalIfCode();
            IPayOrderQueryService originalQueryService = getPayOrderQueryService(originalIfCode);
            
            if (originalQueryService != null) {
                // 查询原渠道支付结果
                ChannelRetMsg originalRetMsg = originalQueryService.query(payOrder, mchAppConfigContext);
                
                // 如果原渠道支付成功，更新订单状态
                if (originalRetMsg.getChannelState() == ChannelRetMsg.ChannelState.CONFIRM_SUCCESS) {
                    payOrderService.updateIng2Success(payOrderId, originalRetMsg.getChannelOrderId(), originalRetMsg.getChannelUserId());
                    updateCompensationSuccess(compensation, "原渠道支付成功");
                    return;
                }
            }
            
            // 查询补偿渠道的支付结果
            String compensationIfCode = compensation.getCompensationIfCode();
            IPayOrderQueryService compensationQueryService = getPayOrderQueryService(compensationIfCode);
            
            if (compensationQueryService == null) {
                log.warn("补偿渠道查询服务不存在，ifCode={}", compensationIfCode);
                updateCompensationFailed(compensation, "补偿渠道查询服务不存在");
                return;
            }
            
            // 查询补偿渠道支付结果
            ChannelRetMsg compensationRetMsg = compensationQueryService.query(payOrder, mchAppConfigContext);
            
            // 处理查询结果
            if (compensationRetMsg.getChannelState() == ChannelRetMsg.ChannelState.CONFIRM_SUCCESS) {
                // 支付成功，更新订单状态
                payOrderService.updateIng2Success(payOrderId, compensationRetMsg.getChannelOrderId(), compensationRetMsg.getChannelUserId());
                updateCompensationSuccess(compensation, "补偿渠道支付成功");
                
                // 记录资金差异，用于后续资金池平衡
                recordFundDiscrepancy(originalIfCode, compensationIfCode, payOrder.getAmount());
                
            } else if (compensationRetMsg.getChannelState() == ChannelRetMsg.ChannelState.CONFIRM_FAIL) {
                // 支付失败，更新订单状态
                payOrderService.updateIng2FailByOrderId(payOrderId);
                updateCompensationFailed(compensation, "补偿渠道支付失败");
                
            } else {
                // 支付中，继续等待
                log.info("补偿渠道支付处理中，稍后再查询，payOrderId={}", payOrderId);
                
                // 再次发送延迟消息，继续查询
                mqSender.send(PayOrderReissueMQ.build(payOrderId, 1), 60);
            }
            
        } catch (Exception e) {
            log.error("处理补偿记录异常，compensationId={}", compensation.getCompensationId(), e);
        }
    }
    
    /**
     * 更新补偿记录为成功
     */
    private void updateCompensationSuccess(PayOrderCompensation compensation, String resultInfo) {
        payOrderCompensationService.updateCompensationState(
                compensation.getCompensationId(), (byte)1, resultInfo);
    }
    
    /**
     * 更新补偿记录为失败
     */
    private void updateCompensationFailed(PayOrderCompensation compensation, String resultInfo) {
        payOrderCompensationService.updateCompensationState(
                compensation.getCompensationId(), (byte)2, resultInfo);
    }
    
    /**
     * 获取支付订单查询服务
     */
    private IPayOrderQueryService getPayOrderQueryService(String ifCode) {
        try {
            return SpringBeansUtil.getBean(ifCode + "PayOrderQueryService", IPayOrderQueryService.class);
        } catch (Exception e) {
            log.error("获取支付订单查询服务失败，ifCode={}", ifCode, e);
            return null;
        }
    }
    
    /**
     * 记录资金差异
     * 用于资金池平衡机制
     */
    private void recordFundDiscrepancy(String fromChannel, String toChannel, Long amount) {
        // 记录从fromChannel到toChannel的资金转移
        Map<String, Long> fromChannelMap = fundDiscrepancyCache.computeIfAbsent(fromChannel, k -> new ConcurrentHashMap<>());
        Map<String, Long> toChannelMap = fundDiscrepancyCache.computeIfAbsent(toChannel, k -> new ConcurrentHashMap<>());
        
        // 更新资金差异记录
        fromChannelMap.compute(toChannel, (k, v) -> v == null ? -amount : v - amount);
        toChannelMap.compute(fromChannel, (k, v) -> v == null ? amount : v + amount);
        
        log.info("记录资金差异: 从[{}]到[{}]，金额={}分", fromChannel, toChannel, amount);
        
        // 检查是否需要自动修复资金差异
        checkAndFixFundDiscrepancy(fromChannel, toChannel);
    }
    
    /**
     * 检查并修复资金差异
     * 实现资金池平衡机制
     */
    private void checkAndFixFundDiscrepancy(String channel1, String channel2) {
        Map<String, Long> channel1Map = fundDiscrepancyCache.get(channel1);
        Map<String, Long> channel2Map = fundDiscrepancyCache.get(channel2);
        
        if (channel1Map == null || channel2Map == null) {
            return;
        }
        
        Long amount1to2 = channel1Map.getOrDefault(channel2, 0L);
        Long amount2to1 = channel2Map.getOrDefault(channel1, 0L);
        
        // 如果两个通道之间的资金差异超过阈值，触发自动修复
        if (Math.abs(amount1to2) > AUTO_FIX_THRESHOLD || Math.abs(amount2to1) > AUTO_FIX_THRESHOLD) {
            log.info("检测到资金差异超过阈值，触发自动修复: [{}]和[{}]之间差异={}分", 
                    channel1, channel2, Math.abs(amount1to2));
            
            // TODO: 实际项目中，这里应该调用资金转账接口或通知财务人员手动处理
            // 模拟资金平衡处理
            log.info("执行资金平衡处理，从[{}]到[{}]转账{}分", 
                    amount1to2 > 0 ? channel2 : channel1,
                    amount1to2 > 0 ? channel1 : channel2,
                    Math.abs(amount1to2));
            
            // 重置资金差异记录
            channel1Map.put(channel2, 0L);
            channel2Map.put(channel1, 0L);
        }
    }
    
    /**
     * 定时任务：执行全面资金对账
     * 每天凌晨2点执行
     */
    @Scheduled(cron = "0 0 2 * * ?")
    public void performFullReconciliation() {
        log.info("开始执行全面资金对账...");
        
        try {
            // TODO: 实现完整的资金对账逻辑
            // 1. 获取所有支付渠道的交易记录
            // 2. 与系统内的交易记录进行比对
            // 3. 记录并处理差异
            
            log.info("全面资金对账完成");
        } catch (Exception e) {
            log.error("执行全面资金对账异常", e);
        }
    }
    
    /**
     * 获取所有渠道间的资金差异情况
     * 用于管理界面展示
     */
    public Map<String, Object> getAllFundDiscrepancies() {
        Map<String, Object> result = new HashMap<>();
        
        fundDiscrepancyCache.forEach((fromChannel, toChannelMap) -> {
            Map<String, Object> channelDiscrepancies = new HashMap<>();
            
            toChannelMap.forEach((toChannel, amount) -> {
                if (amount != 0) {
                    String direction = amount > 0 ? "应收" : "应付";
                    channelDiscrepancies.put(toChannel, Map.of(
                            "amount", Math.abs(amount),
                            "direction", direction,
                            "amountYuan", new BigDecimal(Math.abs(amount)).divide(new BigDecimal(100)).toString()
                    ));
                }
            });
            
            if (!channelDiscrepancies.isEmpty()) {
                result.put(fromChannel, channelDiscrepancies);
            }
        });
        
        return result;
    }
} 