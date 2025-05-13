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

import com.jeequan.jeepay.core.entity.PayOrder;
import com.jeequan.jeepay.pay.model.Discrepancy;
import com.jeequan.jeepay.service.impl.PaymentRecordService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * 支付差异处理器
 * 用于批处理中检测订单和支付记录之间的差异
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/8/12
 */
@Component
@Slf4j
public class DiscrepancyItemProcessor implements ItemProcessor<PayOrder, Discrepancy> {

    @Autowired private PaymentRecordService paymentRecordService;

    @Override
    public Discrepancy process(PayOrder payOrder) {
        try {
            // 创建差异对象
            Discrepancy discrepancy = new Discrepancy();
            discrepancy.setOrderNo(payOrder.getPayOrderId());
            discrepancy.setExpected(new BigDecimal(payOrder.getAmount()));
            discrepancy.setChannel(payOrder.getIfCode());
            discrepancy.setBackupIfCode(payOrder.getBackupIfCode());
            
            // 查询支付记录表中的实际支付金额
            BigDecimal actualAmount = paymentRecordService.queryActualPaymentAmount(payOrder.getPayOrderId());
            discrepancy.setActual(actualAmount);
            
            // 检测差异类型
            if (actualAmount == null) {
                // 缺失支付记录
                discrepancy.setType("MISSING_PAYMENT");
                log.warn("检测到缺失支付记录，订单号: {}, 订单金额: {}", payOrder.getPayOrderId(), payOrder.getAmount());
                
            } else if (actualAmount.compareTo(new BigDecimal(payOrder.getAmount())) != 0) {
                // 金额不匹配
                discrepancy.setType("AMOUNT_MISMATCH");
                discrepancy.setDiscrepancyAmount(new BigDecimal(payOrder.getAmount()).subtract(actualAmount));
                log.warn("检测到金额不匹配，订单号: {}, 订单金额: {}, 实际支付金额: {}, 差额: {}",
                        payOrder.getPayOrderId(), payOrder.getAmount(), actualAmount,
                        discrepancy.getDiscrepancyAmount());
                
            } else {
                // 无差异
                discrepancy.setType("NONE");
            }
            
            // 只返回有差异的记录，无差异的记录返回null将被过滤掉
            return discrepancy.hasIssue() ? discrepancy : null;
            
        } catch (Exception e) {
            log.error("处理支付订单差异时发生异常，订单号: {}", payOrder.getPayOrderId(), e);
            return null;
        }
    }
} 