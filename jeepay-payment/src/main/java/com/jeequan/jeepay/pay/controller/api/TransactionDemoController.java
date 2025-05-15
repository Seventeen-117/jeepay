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
package com.jeequan.jeepay.pay.controller.api;

import com.jeequan.jeepay.core.aop.MethodLog;
import com.jeequan.jeepay.core.constants.ApiCodeEnum;
import com.jeequan.jeepay.core.entity.PayOrder;
import com.jeequan.jeepay.core.exception.BizException;
import com.jeequan.jeepay.core.model.ApiRes;
import com.jeequan.jeepay.pay.ctrl.ApiController;
import com.jeequan.jeepay.service.impl.PayOrderService;
import com.jeequan.jeepay.service.tx.PaymentTransactionService;
import com.jeequan.jeepay.service.tx.SeataTransactionDemoService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * 分布式事务演示接口
 *
 * @author jiangyangpay
 * @since 2023/05/28
 */
@Slf4j
@Tag(name = "分布式事务演示接口")
@RestController
@RequestMapping("/api/transaction")
public class TransactionDemoController extends ApiController {

    @Autowired
    private SeataTransactionDemoService demoService;
    
    @Autowired
    private PaymentTransactionService paymentTxService;
    
    @Autowired
    private PayOrderService payOrderService;

    /**
     * AT模式示例 - 两阶段提交
     */
    @Operation(summary = "AT模式示例 - 两阶段提交")
    @MethodLog(remark = "AT模式示例")
    @PostMapping("/at/demo")
    public ApiRes<?> atDemo(@RequestParam String payOrderId, @RequestParam String newState) {
        try {
            // 确保订单存在
            PayOrder payOrder = payOrderService.getById(payOrderId);
            if (payOrder == null) {
                return ApiRes.fail(ApiCodeEnum.SYS_OPERATION_FAIL_SEATA, "订单不存在");
            }
            
            // 调用AT模式示例
            demoService.demoTwoPhaseCommit(payOrderId, newState);
            
            return ApiRes.ok();
        } catch (BizException e) {
            return ApiRes.customFail(e.getMessage());
        } catch (Exception e) {
            log.error("AT模式示例异常", e);
            return ApiRes.fail(ApiCodeEnum.SYSTEM_ERROR, "系统异常：" + e.getMessage());
        }
    }
    
    /**
     * Saga模式示例 - 状态机
     */
    @Operation(summary = "Saga模式示例 - 状态机")
    @MethodLog(remark = "Saga模式示例")
    @PostMapping("/saga/demo")
    public ApiRes<?> sagaDemo(@RequestParam String payOrderId, @RequestParam String newState, @RequestParam(required = false, defaultValue = "false") boolean mockException) {
        try {
            // 确保订单存在
            PayOrder payOrder = payOrderService.getById(payOrderId);
            if (payOrder == null) {
                return ApiRes.fail(ApiCodeEnum.SYS_OPERATION_FAIL_SEATA, "订单不存在");
            }
            
            // 调用Saga模式示例
            demoService.demoSagaStateMachine(payOrderId, newState);
            
            return ApiRes.ok();
        } catch (BizException e) {
            return ApiRes.customFail(e.getMessage());
        } catch (Exception e) {
            log.error("Saga模式示例异常", e);
            return ApiRes.fail(ApiCodeEnum.SYSTEM_ERROR, "系统异常：" + e.getMessage());
        }
    }
    
    /**
     * 完成支付订单 - AT模式
     */
    @Operation(summary = "完成支付订单 - AT模式")
    @MethodLog(remark = "完成支付订单")
    @PostMapping("/pay/complete")
    public ApiRes<?> completePayOrder(@RequestParam String payOrderId) {
        try {
            // 调用支付完成服务
            paymentTxService.completePayOrderWithTwoPhaseCommit(payOrderId);
            
            return ApiRes.ok();
        } catch (BizException e) {
            return ApiRes.customFail(e.getMessage());
        } catch (Exception e) {
            log.error("完成支付订单异常", e);
            return ApiRes.fail(ApiCodeEnum.SYSTEM_ERROR, "系统异常：" + e.getMessage());
        }
    }
    
    /**
     * 订单退款 - Saga模式
     */
    @Operation(summary = "订单退款 - Saga模式")
    @MethodLog(remark = "订单退款")
    @PostMapping("/pay/refund")
    public ApiRes<?> refundPayOrder(@RequestParam String payOrderId, 
                                  @RequestParam Long refundAmount, 
                                  @RequestParam(required = false, defaultValue = "") String refundReason) {
        try {
            // 调用退款服务
            paymentTxService.refundPayOrderWithSaga(payOrderId, refundAmount, refundReason);
            
            return ApiRes.ok();
        } catch (BizException e) {
            return ApiRes.customFail(e.getMessage());
        } catch (Exception e) {
            log.error("订单退款异常", e);
            return ApiRes.fail(ApiCodeEnum.SYSTEM_ERROR, "系统异常：" + e.getMessage());
        }
    }
} 