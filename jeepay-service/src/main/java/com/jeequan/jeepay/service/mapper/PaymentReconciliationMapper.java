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
package com.jeequan.jeepay.service.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.jeequan.jeepay.core.entity.PaymentReconciliation;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Update;

/**
 * <p>
 * 支付对账表 Mapper 接口
 * </p>
 *
 * @author jeepay
 * @since 2023-08-12
 */
@Mapper
public interface PaymentReconciliationMapper extends BaseMapper<PaymentReconciliation> {

    /**
     * 检查视图是否存在
     */
    @Update("DROP TABLE IF EXISTS payment_reconciliation")
    void dropTableIfExists();
    
    /**
     * 删除视图（如果存在）
     */
    @Update("DROP VIEW IF EXISTS payment_reconciliation")
    void dropViewIfExists();
    
    /**
     * 创建视图（MySQL不支持物化视图，使用普通视图代替）
     */
    @Update("CREATE VIEW payment_reconciliation AS " +
            "SELECT " +
            "po.pay_order_id as order_no, " +
            "po.amount as expected, " +
            "pr.amount as actual, " +
            "CASE " +
            "WHEN pr.amount IS NULL THEN 'MISSING_PAYMENT' " +
            "WHEN po.amount != pr.amount THEN 'AMOUNT_MISMATCH' " +
            "ELSE 'NONE' " +
            "END as discrepancy_type, " +
            "(po.amount - COALESCE(pr.amount, 0)) as discrepancy_amount, " +
            "0 as is_fixed, " +
            "po.if_code as channel, " +
            "po.backup_if_code as backup_channel, " +
            "NOW() as create_time, " +
            "NOW() as update_time " +
            "FROM t_pay_order po " +
            "LEFT JOIN payment_records pr ON po.pay_order_id = pr.order_no " +
            "WHERE po.state = 2")
    void createMaterializedView();

    /**
     * 刷新视图（MySQL视图不需要刷新，此方法为空操作）
     */
    @Update("SELECT 1")
    void refreshMaterializedView();

    /**
     * 更新差异记录为已处理状态
     * @param orderNo 订单号
     * @return 更新的记录数
     */
    @Update("UPDATE payment_reconciliation SET is_fixed = 1 WHERE order_no = #{orderNo}")
    int markAsFixed(String orderNo);
} 