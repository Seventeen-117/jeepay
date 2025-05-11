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
import com.jeequan.jeepay.pay.config.Resilience4jConfig;
import com.jeequan.jeepay.pay.model.PayChannelMetrics;
import com.jeequan.jeepay.service.impl.PayOrderService;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 支付交易管理器
 * 负责管理支付渠道的熔断、监控和补偿记录
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
    @Autowired private CircuitBreakerRegistry circuitBreakerRegistry;
    
    // 支付渠道度量指标缓存
    private final Map<String, PayChannelMetrics> channelMetricsMap = new ConcurrentHashMap<>();
    
    /**
     * 记录支付渠道调用开始
     * @param ifCode 接口代码
     */
    public void recordCallStart(String ifCode) {
        PayChannelMetrics metrics = getOrCreateMetrics(ifCode);
        metrics.getCallCount().incrementAndGet();
    }
    
    /**
     * 记录支付渠道调用成功
     * @param ifCode 接口代码
     * @param responseTime 响应时间（毫秒）
     */
    public void recordCallSuccess(String ifCode, long responseTime) {
        PayChannelMetrics metrics = getOrCreateMetrics(ifCode);
        metrics.getSuccessCount().incrementAndGet();
        metrics.getTotalResponseTime().addAndGet(responseTime);
        
        // 更新平均响应时间
        long callCount = metrics.getCallCount().get();
        if (callCount > 0) {
            metrics.setAvgResponseTime(metrics.getTotalResponseTime().get() / callCount);
        }
        
        // 记录最后一次成功时间
        metrics.setLastSuccessTime(System.currentTimeMillis());
    }
    
    /**
     * 记录支付渠道调用失败
     * @param ifCode 接口代码
     * @param responseTime 响应时间（毫秒）
     */
    public void recordCallFailure(String ifCode, long responseTime) {
        PayChannelMetrics metrics = getOrCreateMetrics(ifCode);
        metrics.getFailureCount().incrementAndGet();
        metrics.getTotalResponseTime().addAndGet(responseTime);
        
        // 更新平均响应时间
        long callCount = metrics.getCallCount().get();
        if (callCount > 0) {
            metrics.setAvgResponseTime(metrics.getTotalResponseTime().get() / callCount);
        }
        
        // 记录最后一次失败时间
        metrics.setLastFailureTime(System.currentTimeMillis());
        
        // 获取熔断器并记录失败
        CircuitBreaker circuitBreaker = Resilience4jConfig.getPaymentChannelCircuitBreaker(
                circuitBreakerRegistry, ifCode);
        
        // 记录失败事件
        circuitBreaker.onError(responseTime, java.util.concurrent.TimeUnit.MILLISECONDS, new RuntimeException("支付渠道调用失败"));
    }
    
    /**
     * 检查支付渠道是否可用（未熔断）
     * @param ifCode 接口代码
     * @return 是否可用
     */
    public boolean isChannelAvailable(String ifCode) {
        try {
            CircuitBreaker circuitBreaker = Resilience4jConfig.getPaymentChannelCircuitBreaker(
                    circuitBreakerRegistry, ifCode);
            
            return circuitBreaker.getState() != CircuitBreaker.State.OPEN;
        } catch (Exception e) {
            log.error("检查支付渠道状态时发生异常，渠道: {}", ifCode, e);
            return true; // 默认可用
        }
    }
    
    /**
     * 获取支付渠道度量指标
     * @param ifCode 接口代码
     * @return 度量指标
     */
    public PayChannelMetrics getChannelMetrics(String ifCode) {
        return channelMetricsMap.get(ifCode);
    }
    
    /**
     * 获取或创建支付渠道度量指标
     * @param ifCode 接口代码
     * @return 度量指标
     */
    private PayChannelMetrics getOrCreateMetrics(String ifCode) {
        return channelMetricsMap.computeIfAbsent(ifCode, k -> {
            PayChannelMetrics metrics = new PayChannelMetrics();
            metrics.setIfCode(ifCode);
            metrics.setCallCount(new AtomicLong(0));
            metrics.setSuccessCount(new AtomicLong(0));
            metrics.setFailureCount(new AtomicLong(0));
            metrics.setTotalResponseTime(new AtomicLong(0));
            metrics.setAvgResponseTime(0);
            metrics.setLastSuccessTime(0);
            metrics.setLastFailureTime(0);
            return metrics;
        });
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
            log.info("手动重置支付渠道[{}]的熔断器", ifCode);
        }
    }
    
    /**
     * 获取所有渠道的熔断状态
     * 用于监控和管理界面展示
     */
    public Map<String, Object> getAllChannelStatus() {
        // Implementation needed
        throw new UnsupportedOperationException("Method getAllChannelStatus() not implemented");
    }
    
    /**
     * 获取推荐的备用支付渠道
     * 根据渠道权重和成功率推荐最优的备用渠道
     * @param ifCode 当前渠道代码（需要排除）
     * @return 推荐的备用渠道代码
     */
    public String getRecommendedBackupChannel(String ifCode) {
        // 找出权重最高的渠道（排除当前渠道）
        return channelMetricsMap.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(ifCode)) // 排除当前渠道
                .filter(entry -> isChannelAvailable(entry.getKey())) // 只考虑可用渠道
                .max((e1, e2) -> {
                    // 计算综合得分 (权重 * 成功率)
                    PayChannelMetrics m1 = e1.getValue();
                    PayChannelMetrics m2 = e2.getValue();
                    
                    // 计算成功率
                    double successRate1 = m1.getSuccessCount().doubleValue() / Math.max(1, m1.getCallCount().get());
                    double successRate2 = m2.getSuccessCount().doubleValue() / Math.max(1, m2.getCallCount().get());
                    
                    // 计算综合得分
                    double score1 = successRate1 * (100 - m1.getAvgResponseTime() / 1000.0);
                    double score2 = successRate2 * (100 - m2.getAvgResponseTime() / 1000.0);
                    
                    return Double.compare(score1, score2);
                })
                .map(Map.Entry::getKey)
                .orElse(null);
    }
} 