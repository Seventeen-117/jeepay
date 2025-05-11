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

import java.math.BigDecimal;

/**
 * 支付差异数据模型
 * 用于对账过程中记录订单和支付记录之间的差异
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/8/12
 */
@Data
@NoArgsConstructor
public class Discrepancy {

    /**
     * 订单号
     */
    private String orderNo;

    /**
     * 差异类型
     * AMOUNT_MISMATCH - 金额不匹配
     * MISSING_PAYMENT - 缺失支付记录
     * NONE - 无差异
     */
    private String type;

    /**
     * 预期金额（订单金额）
     */
    private BigDecimal expected;

    /**
     * 实际金额（支付金额）
     */
    private BigDecimal actual;

    /**
     * 差异金额
     */
    private BigDecimal discrepancyAmount;

    /**
     * 支付渠道
     */
    private String channel;

    /**
     * 备用支付渠道（如果使用了备用渠道）
     */
    private String backupChannel;

    /**
     * 检查是否存在差异
     * @return 如果存在差异返回true，否则返回false
     */
    public boolean hasIssue() {
        return type != null && !type.equals("NONE");
    }

    /**
     * 计算差异金额
     * @return 差异金额
     */
    public BigDecimal calculateDiscrepancy() {
        if (expected == null || actual == null) {
            return null;
        }
        return expected.subtract(actual);
    }
} 