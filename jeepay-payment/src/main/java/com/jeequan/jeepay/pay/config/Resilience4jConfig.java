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
package com.jeequan.jeepay.pay.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Resilience4j熔断器配置类
 *
 * @author jeepay
 * @site https://www.jeequan.com
 * @date 2023/8/12
 */
@Configuration
public class Resilience4jConfig {

    // 支付渠道熔断器名称前缀
    public static final String PAYMENT_CHANNEL_CIRCUIT_BREAKER_PREFIX = "payment-channel-";
    
    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry() {
        // 创建默认的熔断器配置
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 失败率阈值，超过50%的请求失败时触发熔断
                .slowCallRateThreshold(50) // 慢调用率阈值，超过50%的请求被认为是慢调用时触发熔断
                .slowCallDurationThreshold(Duration.ofSeconds(2)) // 慢调用时间阈值，调用时间超过2秒被视为慢调用
                .permittedNumberOfCallsInHalfOpenState(10) // 半开状态下允许的调用次数
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED) // 基于计数的滑动窗口
                .slidingWindowSize(100) // 滑动窗口大小
                .minimumNumberOfCalls(10) // 最小调用次数，只有达到这个次数才会计算失败率
                .waitDurationInOpenState(Duration.ofSeconds(60)) // 熔断器打开状态持续时间，60秒后变为半开状态
                .automaticTransitionFromOpenToHalfOpenEnabled(true) // 启用从打开到半开的自动转换
                .build();

        // 创建熔断器注册表
        return CircuitBreakerRegistry.of(circuitBreakerConfig);
    }

    /**
     * 创建支付渠道熔断器
     * @param circuitBreakerRegistry 熔断器注册表
     * @param channelCode 渠道代码
     * @return 熔断器实例
     */
    public static CircuitBreaker createPaymentChannelCircuitBreaker(
            CircuitBreakerRegistry circuitBreakerRegistry, String channelCode) {
        
        String circuitBreakerName = PAYMENT_CHANNEL_CIRCUIT_BREAKER_PREFIX + channelCode;
        
        // 为特定渠道创建自定义配置
        CircuitBreakerConfig channelConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(40) // 对支付渠道更敏感，失败率40%就触发熔断
                .slowCallRateThreshold(40)
                .slowCallDurationThreshold(Duration.ofSeconds(5)) // 支付渠道通常响应较慢，设置更长的阈值
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(20)
                .minimumNumberOfCalls(5)
                .waitDurationInOpenState(Duration.ofSeconds(30)) // 熔断时间缩短，便于快速恢复
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();
        
        // 如果熔断器已存在，返回已有的熔断器；否则创建新的熔断器
        return circuitBreakerRegistry.circuitBreaker(circuitBreakerName, channelConfig);
    }
    
    /**
     * 缓存所有已创建的支付渠道熔断器
     */
    private static final Map<String, CircuitBreaker> CHANNEL_CIRCUIT_BREAKERS = new HashMap<>();
    
    /**
     * 获取支付渠道熔断器，如果不存在则创建
     * @param circuitBreakerRegistry 熔断器注册表
     * @param channelCode 渠道代码
     * @return 熔断器实例
     */
    public static synchronized CircuitBreaker getPaymentChannelCircuitBreaker(
            CircuitBreakerRegistry circuitBreakerRegistry, String channelCode) {
        
        String key = PAYMENT_CHANNEL_CIRCUIT_BREAKER_PREFIX + channelCode;
        
        if (!CHANNEL_CIRCUIT_BREAKERS.containsKey(key)) {
            CHANNEL_CIRCUIT_BREAKERS.put(key, createPaymentChannelCircuitBreaker(circuitBreakerRegistry, channelCode));
        }
        
        return CHANNEL_CIRCUIT_BREAKERS.get(key);
    }
} 