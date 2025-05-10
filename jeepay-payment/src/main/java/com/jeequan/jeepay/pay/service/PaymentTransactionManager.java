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
import com.jeequan.jeepay.pay.model.PayChannelMetrics;
import com.jeequan.jeepay.service.impl.PayOrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;

/**
 * 支付交易管理器
 * 负责支付渠道的熔断、监控和补偿机制
 *
 * @author jeepay
 * @site https://www.jeequan.com
 * @date 2023/8/12
 */
@Service
@Slf4j
public class PaymentTransactionManager {

    @Autowired private PayOrderService payOrderService;
    @Autowired private IMQSender mqSender;
    
    // 支付渠道度量指标 - 用于熔断机制
    private final Map<String, PayChannelMetrics> channelMetricsMap = new ConcurrentHashMap<>();
    
    // 熔断阈值配置
    private static final int ERROR_THRESHOLD = 5; // 错误阈值
    private static final int TIMEOUT_THRESHOLD = 3000; // 超时阈值(毫秒)
    private static final long CIRCUIT_BREAKER_RESET_TIMEOUT = 60000; // 熔断器重置时间(毫秒)
    
    // 渠道可用性状态缓存 - 用于快速判断渠道是否可用
    private final Map<String, Boolean> channelAvailabilityCache = new ConcurrentHashMap<>();
    
    /**
     * 记录支付渠道调用开始
     */
    public void recordCallStart(String ifCode) {
        PayChannelMetrics metrics = getOrCreateMetrics(ifCode);
        metrics.incrementCallCount();
        metrics.setLastCallTime(System.currentTimeMillis());
    }
    
    /**
     * 记录支付渠道调用成功
     */
    public void recordCallSuccess(String ifCode, long duration) {
        PayChannelMetrics metrics = getOrCreateMetrics(ifCode);
        metrics.incrementSuccessCount();
        metrics.updateResponseTime(duration);
        
        // 如果渠道之前被标记为不可用，现在恢复可用
        if (!Boolean.TRUE.equals(channelAvailabilityCache.get(ifCode))) {
            channelAvailabilityCache.put(ifCode, true);
        }
    }
    
    /**
     * 记录支付渠道调用失败
     */
    public void recordCallFailure(String ifCode, long duration) {
        PayChannelMetrics metrics = getOrCreateMetrics(ifCode);
        metrics.incrementErrorCount();
        metrics.updateResponseTime(duration);
        
        // 检查是否需要触发熔断
        checkCircuitBreaker(ifCode, metrics);
    }
    
    /**
     * 检查支付渠道是否可用
     * 实现了熔断器的三种状态：关闭、打开、半开
     */
    public boolean isChannelAvailable(String ifCode) {
        // 首先检查缓存，提高性能
        Boolean cachedAvailability = channelAvailabilityCache.get(ifCode);
        if (cachedAvailability != null && !cachedAvailability) {
            // 如果缓存中标记为不可用，再检查熔断器状态
            PayChannelMetrics metrics = channelMetricsMap.get(ifCode);
            if (metrics == null) {
                // 如果没有度量数据，认为渠道可用
                channelAvailabilityCache.put(ifCode, true);
                return true;
            }
            
            // 检查熔断器状态
            return checkCircuitBreakerState(ifCode, metrics);
        }
        
        // 默认可用
        return true;
    }
    
    /**
     * 检查熔断器状态并决定是否允许请求通过
     */
    private boolean checkCircuitBreakerState(String ifCode, PayChannelMetrics metrics) {
        long now = System.currentTimeMillis();
        
        // 如果熔断器处于打开状态
        if (metrics.isCircuitBreakerOpen()) {
            // 检查是否达到重置时间，如果是则切换到半开状态
            if (now - metrics.getCircuitBreakerOpenTime() > CIRCUIT_BREAKER_RESET_TIMEOUT) {
                log.info("支付渠道[{}]熔断器从打开状态切换到半开状态", ifCode);
                metrics.halfOpenCircuitBreaker();
                // 继续检查半开状态
                return checkHalfOpenState(ifCode, metrics);
            }
            // 熔断器打开状态，不允许请求通过
            return false;
        } 
        // 如果熔断器处于半开状态
        else if (metrics.isCircuitBreakerHalfOpen()) {
            return checkHalfOpenState(ifCode, metrics);
        }
        
        // 熔断器关闭状态，允许请求通过
        return true;
    }
    
    /**
     * 检查半开状态下是否允许请求通过
     */
    private boolean checkHalfOpenState(String ifCode, PayChannelMetrics metrics) {
        // 在半开状态下，只允许有限数量的请求通过
        boolean allowed = metrics.allowRequestInHalfOpenState();
        if (allowed) {
            log.info("支付渠道[{}]处于半开状态，允许测试请求通过", ifCode);
        }
        return allowed;
    }
    
    /**
     * 检查熔断器状态，决定是否触发熔断
     */
    private void checkCircuitBreaker(String ifCode, PayChannelMetrics metrics) {
        // 错误率超过阈值或平均响应时间超过阈值，触发熔断
        if (metrics.getErrorCount().get() >= ERROR_THRESHOLD || 
            metrics.getAverageResponseTime() > TIMEOUT_THRESHOLD) {
            
            log.warn("支付渠道[{}]触发熔断，错误次数:{}, 平均响应时间:{}ms", 
                    ifCode, metrics.getErrorCount().get(), metrics.getAverageResponseTime());
            
            metrics.openCircuitBreaker();
            channelAvailabilityCache.put(ifCode, false);
        }
    }
    
    /**
     * 获取或创建渠道度量指标
     */
    private PayChannelMetrics getOrCreateMetrics(String ifCode) {
        return channelMetricsMap.computeIfAbsent(ifCode, k -> new PayChannelMetrics());
    }
    
    /**
     * 获取渠道度量指标
     * 公开方法，供其他服务使用
     */
    public PayChannelMetrics getChannelMetrics(String ifCode) {
        return getOrCreateMetrics(ifCode);
    }
    
    /**
     * 创建支付补偿记录
     * 用于SAGA事务补偿机制
     */
    @Transactional(rollbackFor = Exception.class)
    public void createCompensationRecord(PayOrder payOrder, String originalIfCode, String compensationIfCode) {
        try {
            // 创建补偿记录
            PayOrderCompensation compensation = new PayOrderCompensation();
            compensation.setPayOrderId(payOrder.getPayOrderId());
            compensation.setMchNo(payOrder.getMchNo());
            compensation.setAppId(payOrder.getAppId());
            compensation.setOriginalIfCode(originalIfCode);
            compensation.setCompensationIfCode(compensationIfCode);
            compensation.setAmount(payOrder.getAmount());
            compensation.setState(CS.YES);
            compensation.setCreatedAt(new Date());
            
            // 保存补偿记录
            // payOrderCompensationService.save(compensation);
            
            // 发送MQ消息，异步处理补偿结果查询
            mqSender.send(PayOrderReissueMQ.build(payOrder.getPayOrderId(), 1), 120);
            
            log.info("创建支付补偿记录成功，订单号:{}, 原渠道:{}, 补偿渠道:{}", 
                    payOrder.getPayOrderId(), originalIfCode, compensationIfCode);
            
        } catch (Exception e) {
            log.error("创建支付补偿记录失败: {}", e.getMessage(), e);
            throw new BizException("创建支付补偿记录失败");
        }
    }
    
    /**
     * 手动重置特定渠道的熔断器
     * 用于管理接口，允许管理员手动恢复渠道
     */
    public void manualResetCircuitBreaker(String ifCode) {
        PayChannelMetrics metrics = channelMetricsMap.get(ifCode);
        if (metrics != null) {
            metrics.resetCircuitBreaker();
            channelAvailabilityCache.put(ifCode, true);
            log.info("手动重置支付渠道[{}]的熔断器", ifCode);
        }
    }
    
    /**
     * 获取所有渠道的熔断状态
     * 用于监控和管理界面展示
     */
    public Map<String, Object> getAllChannelStatus() {
        Map<String, Object> result = new HashMap<>();
        
        channelMetricsMap.forEach((ifCode, metrics) -> {
            Map<String, Object> channelStatus = new HashMap<>();
            channelStatus.put("callCount", metrics.getCallCount().get());
            channelStatus.put("successCount", metrics.getSuccessCount().get());
            channelStatus.put("errorCount", metrics.getErrorCount().get());
            channelStatus.put("averageResponseTime", metrics.getAverageResponseTime());
            
            if (metrics.isCircuitBreakerOpen()) {
                channelStatus.put("status", "OPEN");
                long remainingTime = CIRCUIT_BREAKER_RESET_TIMEOUT - 
                        (System.currentTimeMillis() - metrics.getCircuitBreakerOpenTime());
                channelStatus.put("resetIn", Math.max(0, remainingTime / 1000) + "秒");
            } else if (metrics.isCircuitBreakerHalfOpen()) {
                channelStatus.put("status", "HALF_OPEN");
            } else {
                channelStatus.put("status", "CLOSED");
            }
            
            result.put(ifCode, channelStatus);
        });
        
        return result;
    }
} 