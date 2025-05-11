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
package com.jeequan.jeepay.core.entity;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

/**
 * 支付对账实体类 - 物化视图
 *
 * @author jeepay
 * @site https://www.jeequan.com
 * @date 2023/8/12
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@NoArgsConstructor
@TableName("payment_reconciliation")
public class PaymentReconciliation implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final LambdaQueryWrapper<PaymentReconciliation> gw() {
        return new LambdaQueryWrapper<>();
    }

    /**
     * 订单号（主键）
     */
    @TableId(value = "order_no")
    private String orderNo;

    /**
     * 预期金额（订单金额）
     */
    private BigDecimal expected;

    /**
     * 实际金额（支付金额）
     */
    private BigDecimal actual;

    /**
     * 差异类型
     * AMOUNT_MISMATCH - 金额不匹配
     * MISSING_PAYMENT - 缺失支付记录
     * NONE - 无差异
     */
    private String discrepancyType;

    /**
     * 差异金额
     */
    private BigDecimal discrepancyAmount;

    /**
     * 是否已处理差异
     */
    private Integer isFixed;

    /**
     * 支付渠道
     */
    private String channel;

    /**
     * 备用支付渠道（如果使用了备用渠道）
     */
    private String backupChannel;
    
    /**
     * 创建时间
     */
    private Date createTime;

    /**
     * 更新时间
     */
    private Date updateTime;
} 