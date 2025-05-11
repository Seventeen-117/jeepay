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
package com.jeequan.jeepay.pay.batch;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.jeequan.jeepay.core.entity.PayOrder;
import com.jeequan.jeepay.service.impl.PayOrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * 支付订单读取器
 * 用于批处理中读取需要对账的支付订单
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/8/12
 */
@Component
@Slf4j
public class PayOrderItemReader implements ItemReader<PayOrder> {

    @Autowired private PayOrderService payOrderService;
    
    // 缓存已读取的订单，避免每次都查询数据库
    private Queue<PayOrder> orderQueue = new LinkedList<>();
    
    // 批次大小
    private static final int BATCH_SIZE = 100;
    
    // 当前页码
    private int currentPage = 1;
    
    // 是否已读取完所有数据
    private boolean exhausted = false;

    @Override
    public PayOrder read() {
        // 如果队列为空，并且还有数据可读，则加载下一批数据
        if (orderQueue.isEmpty() && !exhausted) {
            fetchOrders();
        }
        
        // 从队列中取出一个订单
        return orderQueue.poll();
    }
    
    /**
     * 从数据库中获取一批支付订单
     */
    private void fetchOrders() {
        try {
            // 计算24小时前的时间点，只对账最近24小时的订单
            LocalDateTime oneDayAgo = LocalDateTime.now().minusHours(24);
            Date oneDayAgoDate = Date.from(oneDayAgo.atZone(ZoneId.systemDefault()).toInstant());
            
            // 查询条件：已支付状态 + 最近24小时 + 分页
            LambdaQueryWrapper<PayOrder> queryWrapper = new LambdaQueryWrapper<>();
            queryWrapper.eq(PayOrder::getState, PayOrder.STATE_SUCCESS); // 已支付状态
            queryWrapper.ge(PayOrder::getSuccessTime, oneDayAgoDate); // 最近24小时支付成功的订单
            
            // 分页查询
            List<PayOrder> orders = payOrderService.list(queryWrapper);
            
            if (orders.isEmpty()) {
                // 没有更多数据了
                exhausted = true;
            } else {
                // 将查询结果添加到队列中
                orderQueue.addAll(orders);
                // 增加页码
                currentPage++;
            }
            
            log.debug("读取了 {} 条支付订单用于对账", orders.size());
            
        } catch (Exception e) {
            log.error("读取支付订单失败", e);
            exhausted = true; // 出错时标记为已读取完毕，避免无限循环
        }
    }
    
    /**
     * 重置读取器状态，用于重新开始读取
     */
    public void reset() {
        orderQueue.clear();
        currentPage = 1;
        exhausted = false;
    }
} 