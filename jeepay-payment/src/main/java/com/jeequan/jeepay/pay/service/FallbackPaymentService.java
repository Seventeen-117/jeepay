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

import com.jeequan.jeepay.core.constants.CS;
import com.jeequan.jeepay.core.entity.MchPayPassage;
import com.jeequan.jeepay.core.entity.PayOrder;
import com.jeequan.jeepay.core.entity.PaymentRecord;
import com.jeequan.jeepay.core.exception.BizException;
import com.jeequan.jeepay.pay.exception.ChannelException;
import com.jeequan.jeepay.pay.model.MchAppConfigContext;
import com.jeequan.jeepay.pay.rqrs.msg.ChannelRetMsg;
import com.jeequan.jeepay.pay.rqrs.payorder.UnifiedOrderRQ;
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
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 备用支付服务
 * 处理支付渠道熔断后的备用支付逻辑
 *
 * @author jeepay
 * @site https://www.jeequan.com
 * @date 2023/8/12
 */
@Service
@Slf4j
public class FallbackPaymentService {

    @Autowired private RedissonClient redissonClient;
    @Autowired private PayOrderService payOrderService;
    @Autowired private MchPayPassageService mchPayPassageService;
    @Autowired private ConfigContextQueryService configContextQueryService;
    @Autowired private PaymentRecordService paymentRecordService;
    @Autowired private CircuitBreakerRegistry circuitBreakerRegistry;
    @Autowired private PayOrderDistributedTransactionService payOrderDistributedTransactionService;
    @Autowired private PaymentTransactionManager paymentTransactionManager;

    /**
     * 执行备用支付
     * 使用Resilience4j熔断器包装支付请求
     * @param wayCode 支付方式
     * @param bizRQ 支付请求
     * @param payOrder 支付订单
     * @param mchAppConfigContext 商户配置上下文
     * @return 渠道返回消息
     */
    @Transactional(rollbackFor = Exception.class)
    public ChannelRetMsg fallbackPayment(String wayCode, UnifiedOrderRQ bizRQ, PayOrder payOrder,
                                       MchAppConfigContext mchAppConfigContext) {
        
        // 获取分布式锁，确保同一订单的支付操作互斥
        RLock lock = redissonClient.getLock("order_lock:" + payOrder.getPayOrderId());
        
        try {
            // 尝试获取锁，最多等待1秒，锁定30秒
            if (lock.tryLock(1, 30, TimeUnit.SECONDS)) {
                try {
                    // 检查订单状态，确保可以进行备用支付
                    PayOrder currentOrder = payOrderService.getById(payOrder.getPayOrderId());
                    if (currentOrder == null) {
                        throw new BizException("订单不存在");
                    }
                    
                    // 只有初始化或处理中的订单才能进行备用支付
                    if (currentOrder.getState() != PayOrder.STATE_INIT && currentOrder.getState() != PayOrder.STATE_ING) {
                        throw new BizException("订单状态不允许进行备用支付");
                    }
                    
                    // 查找可用的备用支付通道
                    List<MchPayPassage> backupPassages = findAvailableBackupChannels(
                            mchAppConfigContext.getMchNo(), mchAppConfigContext.getAppId(), wayCode, payOrder.getIfCode());
                    
                    if (backupPassages.isEmpty()) {
                        log.error("没有可用的备用支付通道，订单号: {}", payOrder.getPayOrderId());
                        return ChannelRetMsg.sysError("暂无可用的备用支付通道，请稍后再试");
                    }
                    
                    // 使用第一个备用通道
                    MchPayPassage backupPassage = backupPassages.get(0);
                    String backupIfCode = backupPassage.getIfCode();
                    
                    // 记录备用通道信息
                    payOrder.setBackupIfCode(backupIfCode);
                    payOrderService.updateById(payOrder);
                    
                    // 使用熔断器包装备用通道支付请求
                    CircuitBreaker circuitBreaker = CircuitBreaker.of(
                            "backup-payment-" + backupIfCode,
                            CircuitBreaker.ofDefaults("backup-payment-" + backupIfCode).getCircuitBreakerConfig());
                    
                    // 执行备用通道支付
                    Supplier<ChannelRetMsg> backupPaymentSupplier = () -> {
                        try {
                            return payOrderDistributedTransactionService.handlePayTransaction(
                                    wayCode, bizRQ, payOrder, mchAppConfigContext);
                        } catch (Exception e) {
                            log.error("备用支付渠道处理失败，订单号: {}, 渠道: {}", payOrder.getPayOrderId(), backupIfCode, e);
                            throw new RuntimeException("备用支付渠道处理失败", e);
                        }
                    };
                    
                    // 执行备用支付并返回结果
                    ChannelRetMsg result = circuitBreaker.executeSupplier(backupPaymentSupplier);
                    
                    // 记录支付记录
                    if (result.getChannelState() == ChannelRetMsg.ChannelState.CONFIRM_SUCCESS) {
                        PaymentRecord paymentRecord = new PaymentRecord();
                        paymentRecord.setOrderNo(payOrder.getPayOrderId());
                        paymentRecord.setAmount(new BigDecimal(payOrder.getAmount()));
                        paymentRecord.setChannel(payOrder.getIfCode());
                        paymentRecord.setBackupChannel(backupIfCode);
                        paymentRecord.setCreateTime(new Date());
                        paymentRecord.setUpdateTime(new Date());
                        
                        paymentRecordService.save(paymentRecord);
                    }
                    
                    return result;
                    
                } finally {
                    lock.unlock();
                }
            } else {
                log.warn("无法获取订单锁，可能有其他支付请求正在处理，订单号: {}", payOrder.getPayOrderId());
                return ChannelRetMsg.sysError("订单正在处理中，请稍后再试");
            }
        } catch (BizException e) {
            log.error("备用支付业务异常，订单号: {}", payOrder.getPayOrderId(), e);
            return ChannelRetMsg.sysError(e.getMessage());
        } catch (Exception e) {
            log.error("备用支付处理异常，订单号: {}", payOrder.getPayOrderId(), e);
            return ChannelRetMsg.sysError("备用支付处理失败，请稍后再试");
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
        
        // 获取推荐的备用渠道
        String recommendedIfCode = paymentTransactionManager.getRecommendedBackupChannel(primaryIfCode);
        
        // 如果有推荐的备用渠道，优先使用
        if (recommendedIfCode != null) {
            List<MchPayPassage> recommendedPassages = allPassages.stream()
                    .filter(passage -> passage.getIfCode().equals(recommendedIfCode))
                    .collect(Collectors.toList());
            
            if (!recommendedPassages.isEmpty()) {
                log.info("使用推荐的备用支付渠道: {}", recommendedIfCode);
                return recommendedPassages;
            }
        }
        
        // 如果没有推荐的备用渠道，则过滤掉主通道和已熔断的通道
        return allPassages.stream()
                .filter(passage -> !passage.getIfCode().equals(primaryIfCode)) // 排除主通道
                .filter(passage -> isChannelAvailable(passage.getIfCode())) // 只保留可用通道
                .collect(Collectors.toList());
    }
    
    /**
     * 检查支付通道是否可用（未熔断）
     * @param ifCode 接口代码
     * @return 是否可用
     */
    private boolean isChannelAvailable(String ifCode) {
        try {
            CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(
                    "payment-channel-" + ifCode, 
                    CircuitBreaker.ofDefaults("payment-channel-" + ifCode).getCircuitBreakerConfig());
            
            return circuitBreaker.getState() != CircuitBreaker.State.OPEN;
        } catch (Exception e) {
            // 如果获取熔断器状态失败，默认通道可用
            return true;
        }
    }
    
    /**
     * 引导用户使用备用支付渠道
     * 在熔断期间提供备用渠道的支付方式
     * @param payOrder 支付订单
     * @return 备用支付渠道信息
     */
    public MchPayPassage getRecommendedBackupChannel(PayOrder payOrder) {
        try {
            // 获取商户应用配置
            MchAppConfigContext mchAppConfigContext = configContextQueryService.queryMchInfoAndAppInfo(payOrder.getMchNo(), payOrder.getAppId());
            
            // 查找可用的备用支付通道
            List<MchPayPassage> backupPassages = findAvailableBackupChannels(
                    payOrder.getMchNo(), payOrder.getAppId(), payOrder.getWayCode(), payOrder.getIfCode());
            
            if (!backupPassages.isEmpty()) {
                return backupPassages.get(0); // 返回第一个可用的备用通道
            }
        } catch (Exception e) {
            log.error("获取推荐备用支付渠道失败，订单号: {}", payOrder.getPayOrderId(), e);
        }
        
        return null;
    }
} 