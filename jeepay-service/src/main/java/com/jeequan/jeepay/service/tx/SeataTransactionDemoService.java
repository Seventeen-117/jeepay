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
package com.jeequan.jeepay.service.tx;

import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.jeequan.jeepay.core.entity.PayOrder;
import com.jeequan.jeepay.service.impl.PayOrderService;
import io.seata.core.context.RootContext;
import io.seata.saga.engine.StateMachineEngine;
import io.seata.spring.annotation.GlobalTransactional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;

/**
 * Seata 分布式事务demo服务
 * 
 * 展示AT模式（两阶段提交）和Saga模式（长事务补偿）的使用方法
 *
 * @author jiangyangpay
 * @since 2023/05/28
 */
@Slf4j
@Service
public class SeataTransactionDemoService {

    @Autowired
    private PayOrderService payOrderService;
    
    @Autowired
    private StateMachineEngine stateMachineEngine;

    /**
     * AT模式（两阶段提交）示例
     * 使用@GlobalTransactional注解开启全局事务
     */
    @GlobalTransactional(name = "jeepay-at-tx-example", rollbackFor = Exception.class)
    public void demoTwoPhaseCommit(String payOrderId, String newState) {
        // 获取全局事务ID
        log.info("全局事务ID: {}", RootContext.getXID());
        
        // 查询订单
        PayOrder payOrder = payOrderService.getById(payOrderId);
        if (payOrder == null) {
            throw new RuntimeException("订单不存在");
        }
        
        // 修改订单状态
        payOrder.setState(Byte.parseByte(newState));
        boolean updated = payOrderService.updateById(payOrder);
        
        if (!updated) {
            throw new RuntimeException("更新订单失败");
        }
        
        log.info("订单状态已更新: {}", payOrderId);
        
        // 可以在这里调用其他服务的方法，如果发生异常，Seata会自动回滚所有已提交的事务
    }
    
    /**
     * Saga模式示例 - 通过状态机引擎执行
     * 适用于长事务场景，每个步骤都有对应的补偿操作
     */
    public void demoSagaStateMachine(String payOrderId, String newState) {
        Map<String, Object> startParams = new HashMap<>(8);
        startParams.put("payOrderId", payOrderId);
        startParams.put("newState", newState);
        startParams.put("mockException", false); // 是否模拟异常

        // 调用状态机引擎执行Saga流程
        // 注意：这里假设已经有一个名为 "payOrderUpdateStateMachine" 的状态机定义
        String stateMachineName = "payOrderUpdateStateMachine";
        String businessKey = "payOrder:" + payOrderId;
        
        try {
            // 执行状态机
            stateMachineEngine.startWithBusinessKey(stateMachineName, null, businessKey, startParams);
            log.info("Saga事务执行成功");
        } catch (Exception e) {
            log.error("Saga事务执行失败", e);
            throw new RuntimeException("Saga事务执行失败", e);
        }
    }
    
    /**
     * 本地事务示例 - 用于Saga模式下的子事务
     * 通常由状态机调用
     */
    @Transactional(rollbackFor = Exception.class)
    public boolean updatePayOrderState(String payOrderId, String newState, boolean mockException) {
        // 查询订单
        PayOrder payOrder = payOrderService.getById(payOrderId);
        if (payOrder == null) {
            throw new RuntimeException("订单不存在");
        }
        
        // 修改订单状态
        payOrder.setState(Byte.parseByte(newState));
        boolean updated = payOrderService.updateById(payOrder);
        
        // 模拟异常
        if (mockException) {
            throw new RuntimeException("模拟的异常");
        }
        
        return updated;
    }
    
    /**
     * Saga补偿操作 - 回滚订单状态
     * 在状态机中定义了Forward操作和Compensate操作
     */
    @Transactional(rollbackFor = Exception.class)
    public boolean compensatePayOrderState(String payOrderId, String originalState) {
        // 查询订单
        PayOrder payOrder = payOrderService.getById(payOrderId);
        if (payOrder == null) {
            throw new RuntimeException("订单不存在");
        }
        
        // 回滚订单状态
        payOrder.setState(Byte.parseByte(originalState));
        boolean updated = payOrderService.updateById(payOrder);
        
        log.info("订单状态已回滚: {}, 状态: {}", payOrderId, originalState);
        
        return updated;
    }
} 