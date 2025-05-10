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
package com.jeequan.jeepay.pay.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 支付渠道度量指标
 * 用于支付渠道的熔断机制
 *
 * @author jeepay
 * @site https://www.jeequan.com
 * @date 2023/8/12
 */
@Data
@NoArgsConstructor
public class PayChannelMetrics {
    
    // 总调用次数
    private final AtomicInteger callCount = new AtomicInteger(0);
    
    // 成功次数
    private final AtomicInteger successCount = new AtomicInteger(0);
    
    // 错误次数
    private final AtomicInteger errorCount = new AtomicInteger(0);
    
    // 总响应时间
    private final AtomicLong totalResponseTime = new AtomicLong(0);
    
    // 最后调用时间
    private long lastCallTime;
    
    // 熔断器状态: CLOSED(0), OPEN(1), HALF_OPEN(2)
    private int circuitBreakerState = 0;
    
    // 熔断器打开时间
    private long circuitBreakerOpenTime;
    
    // 半开状态下的连续成功次数
    private final AtomicInteger consecutiveSuccessCount = new AtomicInteger(0);
    
    // 半开状态下允许的尝试请求次数
    private final AtomicInteger halfOpenAllowedRequests = new AtomicInteger(0);
    
    // 半开状态下需要的连续成功次数
    private static final int HALF_OPEN_SUCCESS_THRESHOLD = 3;
    
    /**
     * 增加调用次数
     */
    public void incrementCallCount() {
        callCount.incrementAndGet();
    }
    
    /**
     * 增加成功次数
     */
    public void incrementSuccessCount() {
        successCount.incrementAndGet();
        
        // 如果处于半开状态，增加连续成功次数
        if (isCircuitBreakerHalfOpen()) {
            int consecutive = consecutiveSuccessCount.incrementAndGet();
            
            // 如果连续成功次数达到阈值，关闭熔断器
            if (consecutive >= HALF_OPEN_SUCCESS_THRESHOLD) {
                closeCircuitBreaker();
            }
        }
    }
    
    /**
     * 增加错误次数
     */
    public void incrementErrorCount() {
        errorCount.incrementAndGet();
        
        // 如果处于半开状态，立即重新打开熔断器
        if (isCircuitBreakerHalfOpen()) {
            openCircuitBreaker();
        }
    }
    
    /**
     * 更新响应时间
     */
    public void updateResponseTime(long responseTime) {
        totalResponseTime.addAndGet(responseTime);
    }
    
    /**
     * 获取平均响应时间
     */
    public double getAverageResponseTime() {
        int count = callCount.get();
        if (count == 0) {
            return 0;
        }
        return (double) totalResponseTime.get() / count;
    }
    
    /**
     * 打开熔断器
     */
    public void openCircuitBreaker() {
        this.circuitBreakerState = 1; // OPEN
        this.circuitBreakerOpenTime = System.currentTimeMillis();
        this.consecutiveSuccessCount.set(0);
    }
    
    /**
     * 将熔断器设置为半开状态
     */
    public void halfOpenCircuitBreaker() {
        this.circuitBreakerState = 2; // HALF_OPEN
        this.consecutiveSuccessCount.set(0);
        this.halfOpenAllowedRequests.set(HALF_OPEN_SUCCESS_THRESHOLD);
    }
    
    /**
     * 关闭熔断器
     */
    public void closeCircuitBreaker() {
        this.circuitBreakerState = 0; // CLOSED
        this.consecutiveSuccessCount.set(0);
    }
    
    /**
     * 重置熔断器
     */
    public void resetCircuitBreaker() {
        this.circuitBreakerState = 0; // CLOSED
        this.errorCount.set(0);
        this.callCount.set(0);
        this.successCount.set(0);
        this.totalResponseTime.set(0);
        this.consecutiveSuccessCount.set(0);
    }
    
    /**
     * 检查熔断器是否处于打开状态
     */
    public boolean isCircuitBreakerOpen() {
        return this.circuitBreakerState == 1;
    }
    
    /**
     * 检查熔断器是否处于半开状态
     */
    public boolean isCircuitBreakerHalfOpen() {
        return this.circuitBreakerState == 2;
    }
    
    /**
     * 检查熔断器是否处于关闭状态
     */
    public boolean isCircuitBreakerClosed() {
        return this.circuitBreakerState == 0;
    }
    
    /**
     * 半开状态下是否允许请求通过
     */
    public boolean allowRequestInHalfOpenState() {
        return halfOpenAllowedRequests.decrementAndGet() >= 0;
    }
} 