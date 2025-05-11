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

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 支付对账实体类 - 物化视图
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/8/12
 */
@Entity
@Table(name = "payment_reconciliation")
@Data
@NoArgsConstructor
public class PaymentReconciliation implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 订单号（主键）
     */
    @Id
    @Column(name = "order_no")
    private String orderNo;

    /**
     * 预期金额（订单金额）
     */
    @Column(name = "expected")
    private BigDecimal expectedAmount;

    /**
     * 实际金额（支付金额）
     */
    @Column(name = "actual")
    private BigDecimal actualAmount;

    /**
     * 差异类型
     * AMOUNT_MISMATCH - 金额不匹配
     * MISSING_PAYMENT - 缺失支付记录
     * NONE - 无差异
     */
    @Column(name = "discrepancy_type")
    private String discrepancyType;

    /**
     * 差异金额
     */
    @Column(name = "discrepancy_amount")
    private BigDecimal discrepancyAmount;

    /**
     * 是否已处理差异
     */
    @Column(name = "is_fixed")
    private Boolean isFixed;

    /**
     * 支付渠道
     */
    @Column(name = "channel")
    private String channel;

    /**
     * 备用支付渠道（如果使用了备用渠道）
     */
    @Column(name = "backup_channel")
    private String backupChannel;
} 