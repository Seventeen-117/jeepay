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

import com.jeequan.jeepay.core.constants.CS;
import com.jeequan.jeepay.core.entity.MchInfo;
import com.jeequan.jeepay.core.entity.PayOrder;
import com.jeequan.jeepay.core.exception.BizException;
import com.jeequan.jeepay.service.impl.MchInfoService;
import com.jeequan.jeepay.service.impl.PayOrderService;
import io.seata.core.context.RootContext;
import io.seata.saga.engine.StateMachineEngine;
import io.seata.spring.annotation.GlobalTransactional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 支付交易服务 - 使用Seata分布式事务
 * 
 * 实现：
 * 1. AT模式 - 用于强一致性场景，如支付订单创建与商户账户余额更新
 * 2. Saga模式 - 用于长事务场景，如支付流程中的多步骤操作
 *
 * @author jiangyangpay
 * @since 2023/05/28
 */
@Slf4j
@Service
public class PaymentTransactionService {

    @Autowired
    private PayOrderService payOrderService;
    
    @Autowired
    private MchInfoService mchInfoService;
    
    @Autowired
    private StateMachineEngine stateMachineEngine;

    /**
     * AT模式（两阶段提交）- 支付订单完成处理
     * 同时更新订单状态和商户余额
     */
    @GlobalTransactional(name = "pay-order-success-tx", rollbackFor = Exception.class)
    public void completePayOrderWithTwoPhaseCommit(String payOrderId) {
        log.info("开始处理支付订单完成业务，全局事务ID: {}", RootContext.getXID());
        
        // 1. 查询订单信息
        PayOrder payOrder = payOrderService.getById(payOrderId);
        if (payOrder == null) {
            throw new BizException("支付订单不存在");
        }
        
        // 2. 检查订单状态
        if (payOrder.getState() == CS.PAY_STATE_SUCCESS) {
            log.info("订单已是支付成功状态，无需处理");
            return;
        }
        
        if (payOrder.getState() != CS.PAY_STATE_ING) {
            throw new BizException("订单状态不正确，无法完成支付");
        }
        
        // 3. 更新订单状态为成功
        payOrder.setState(CS.PAY_STATE_SUCCESS);
        payOrder.setSuccessTime(new Date());
        boolean updated = payOrderService.updateById(payOrder);
        
        if (!updated) {
            throw new BizException("更新订单状态失败");
        }
        
        // 4. 更新商户余额
        updateMerchantBalance(payOrder.getMchNo(), payOrder.getAmount());
        
        log.info("支付订单处理完成: {}", payOrderId);
    }
    
    /**
     * 更新商户余额
     */
    @Transactional(rollbackFor = Exception.class)
    public void updateMerchantBalance(String mchNo, Long amount) {
        // 查询商户信息
        MchInfo mchInfo = mchInfoService.getById(mchNo);
        if (mchInfo == null) {
            throw new BizException("商户不存在");
        }
        
        // 更新商户余额
        mchInfo.setBalance(mchInfo.getBalance() + amount);
        boolean updated = mchInfoService.updateById(mchInfo);
        
        if (!updated) {
            throw new BizException("更新商户余额失败");
        }
        
        log.info("商户余额已更新，商户号: {}, 增加金额: {}", mchNo, amount);
    }
    
    /**
     * Saga模式 - 支付订单退款处理
     * 使用状态机定义多步骤操作，每步都有对应的补偿操作
     */
    public void refundPayOrderWithSaga(String payOrderId, Long refundAmount, String refundReason) {
        // 1. 查询订单信息
        PayOrder payOrder = payOrderService.getById(payOrderId);
        if (payOrder == null) {
            throw new BizException("支付订单不存在");
        }
        
        // 2. 检查订单状态和退款金额
        if (payOrder.getState() != CS.PAY_STATE_SUCCESS) {
            throw new BizException("订单状态不正确，无法退款");
        }
        
        if (refundAmount > payOrder.getAmount()) {
            throw new BizException("退款金额不能大于支付金额");
        }
        
        // 3. 准备Saga状态机参数
        Map<String, Object> startParams = new HashMap<>(8);
        startParams.put("payOrderId", payOrderId);
        startParams.put("refundAmount", refundAmount);
        startParams.put("refundReason", refundReason);
        startParams.put("mchNo", payOrder.getMchNo());
        startParams.put("originalAmount", payOrder.getAmount());
        
        // 4. 执行Saga流程
        String stateMachineName = "payOrderRefundStateMachine";
        String businessKey = "refund:" + payOrderId;
        
        try {
            // 执行状态机
            stateMachineEngine.startWithBusinessKey(stateMachineName, null, businessKey, startParams);
            log.info("订单退款Saga流程执行成功");
        } catch (Exception e) {
            log.error("订单退款Saga流程执行失败", e);
            throw new BizException("退款处理失败: " + e.getMessage());
        }
    }
    
    /**
     * Saga步骤 - 更新订单退款状态
     */
    @Transactional(rollbackFor = Exception.class)
    public boolean updateOrderRefundStatus(String payOrderId, Long refundAmount) {
        // 查询订单
        PayOrder payOrder = payOrderService.getById(payOrderId);
        if (payOrder == null) {
            throw new BizException("订单不存在");
        }
        
        // 更新订单退款状态
        payOrder.setRefundState(CS.REFUND_STATE_SUCCESS);
        payOrder.setRefundTimes(payOrder.getRefundTimes() + 1);
        payOrder.setRefundAmount(payOrder.getRefundAmount() + refundAmount);
        
        return payOrderService.updateById(payOrder);
    }
    
    /**
     * Saga步骤 - 从商户余额中扣减退款金额
     */
    @Transactional(rollbackFor = Exception.class)
    public boolean deductMerchantBalance(String mchNo, Long refundAmount) {
        // 查询商户信息
        MchInfo mchInfo = mchInfoService.getById(mchNo);
        if (mchInfo == null) {
            throw new BizException("商户不存在");
        }
        
        // 检查商户余额
        if (mchInfo.getBalance() < refundAmount) {
            throw new BizException("商户余额不足");
        }
        
        // 扣减商户余额
        mchInfo.setBalance(mchInfo.getBalance() - refundAmount);
        return mchInfoService.updateById(mchInfo);
    }
    
    /**
     * Saga补偿操作 - 恢复订单退款状态
     */
    @Transactional(rollbackFor = Exception.class)
    public boolean compensateOrderRefundStatus(String payOrderId, Long refundAmount) {
        // 查询订单
        PayOrder payOrder = payOrderService.getById(payOrderId);
        if (payOrder == null) {
            throw new BizException("订单不存在");
        }
        
        // 补偿订单退款状态
        if (payOrder.getRefundTimes() > 0) {
            payOrder.setRefundTimes(payOrder.getRefundTimes() - 1);
        }
        
        if (payOrder.getRefundAmount() >= refundAmount) {
            payOrder.setRefundAmount(payOrder.getRefundAmount() - refundAmount);
        }
        
        // 如果退款金额为0，则更新退款状态为未退款
        if (payOrder.getRefundAmount() == 0) {
            payOrder.setRefundState(CS.REFUND_STATE_NONE);
        }
        
        return payOrderService.updateById(payOrder);
    }
    
    /**
     * Saga补偿操作 - 恢复商户余额
     */
    @Transactional(rollbackFor = Exception.class)
    public boolean compensateMerchantBalance(String mchNo, Long refundAmount) {
        // 查询商户信息
        MchInfo mchInfo = mchInfoService.getById(mchNo);
        if (mchInfo == null) {
            throw new BizException("商户不存在");
        }
        
        // 恢复商户余额
        mchInfo.setBalance(mchInfo.getBalance() + refundAmount);
        return mchInfoService.updateById(mchInfo);
    }
} 