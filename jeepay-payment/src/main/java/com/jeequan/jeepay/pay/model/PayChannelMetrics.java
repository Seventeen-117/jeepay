/*
 * Copyright (c) 2021-2031, 江阳科技有限公司
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
 * 用于记录支付渠道的调用情况，为熔断和智能路由提供数据支持
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/8/12
 */
@Data
@NoArgsConstructor
public class PayChannelMetrics {
    
    /**
     * 接口代码
     */
    private String ifCode;
    
    /**
     * 调用总次数
     */
    private AtomicLong callCount;
    
    /**
     * 成功次数
     */
    private AtomicLong successCount;
    
    /**
     * 失败次数
     */
    private AtomicLong failureCount;
    
    /**
     * 总响应时间（毫秒）
     */
    private AtomicLong totalResponseTime;
    
    /**
     * 平均响应时间（毫秒）
     */
    private long avgResponseTime;
    
    /**
     * 最后一次成功时间（毫秒时间戳）
     */
    private long lastSuccessTime;
    
    /**
     * 最后一次失败时间（毫秒时间戳）
     */
    private long lastFailureTime;
    
    /**
     * 计算成功率
     * @return 成功率（0-1之间的小数）
     */
    public double getSuccessRate() {
        if (callCount == null || callCount.get() == 0) {
            return 1.0; // 没有调用记录时，默认成功率为100%
        }
        
        return (double) successCount.get() / callCount.get();
    }
    
    /**
     * 计算失败率
     * @return 失败率（0-1之间的小数）
     */
    public double getFailureRate() {
        if (callCount == null || callCount.get() == 0) {
            return 0.0; // 没有调用记录时，默认失败率为0%
        }
        
        return (double) failureCount.get() / callCount.get();
    }
    
    /**
     * 重置熔断器状态
     * 清空错误计数和熔断状态
     */
    public void resetCircuitBreaker() {
        if (failureCount != null) {
            failureCount.set(0);
        }
    }
} 