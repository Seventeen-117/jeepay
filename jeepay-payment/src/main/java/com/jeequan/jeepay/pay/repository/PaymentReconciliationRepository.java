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
package com.jeequan.jeepay.pay.repository;

import com.jeequan.jeepay.pay.model.PaymentReconciliation;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * 支付对账数据仓库接口
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/8/12
 */
@Repository
public interface PaymentReconciliationRepository extends JpaRepository<PaymentReconciliation, String> {

    /**
     * 创建物化视图
     */
    @Query(nativeQuery = true, value = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS payment_reconciliation AS
        SELECT 
            po.pay_order_id as order_no, 
            po.amount as expected, 
            pr.amount as actual,
            CASE 
                WHEN pr.amount IS NULL THEN 'MISSING_PAYMENT'
                WHEN po.amount != pr.amount THEN 'AMOUNT_MISMATCH'
                ELSE 'NONE'
            END as discrepancy_type,
            (po.amount - COALESCE(pr.amount, 0)) as discrepancy_amount,
            false as is_fixed,
            po.if_code as channel,
            po.backup_if_code as backup_if_code,
            NOW() as create_time,
            NOW() as update_time
        FROM t_pay_order po 
        LEFT JOIN payment_records pr ON po.pay_order_id = pr.order_no
        WHERE po.state = 2 -- 已支付状态
        WITH DATA
    """)
    void createMaterializedView();
    
    /**
     * 删除物化视图
     */
    @Query(nativeQuery = true, value = "DROP MATERIALIZED VIEW IF EXISTS payment_reconciliation")
    void dropMaterializedView();
    
    /**
     * 创建物化视图索引
     */
    @Query(nativeQuery = true, value = "CREATE INDEX IF NOT EXISTS idx_payment_reconciliation_order_no ON payment_reconciliation(order_no)")
    void createMaterializedViewIndex();

    /**
     * 刷新物化视图（并发方式）
     * CONCURRENTLY关键字允许在刷新的同时仍然可以查询物化视图，
     * 但需要视图上有唯一索引才能使用此功能
     */
    @Query(nativeQuery = true, value = "REFRESH MATERIALIZED VIEW CONCURRENTLY payment_reconciliation")
    void refreshMaterializedView();
    
    /**
     * 刷新物化视图（非并发方式）
     * 如果没有创建唯一索引可以使用此方法，但会锁定视图
     */
    @Query(nativeQuery = true, value = "REFRESH MATERIALIZED VIEW payment_reconciliation")
    void refreshMaterializedViewNonConcurrent();

    /**
     * 查找未处理的差异记录
     * @return 未处理的差异记录列表
     */
    @Query("SELECT pr FROM PaymentReconciliation pr WHERE pr.discrepancyType != 'NONE' AND pr.isFixed = false")
    List<PaymentReconciliation> findUnfixedDiscrepancies();

    /**
     * 查找特定类型的未处理差异记录
     * @param discrepancyType 差异类型
     * @return 特定类型的未处理差异记录列表
     */
    @Query("SELECT pr FROM PaymentReconciliation pr WHERE pr.discrepancyType = ?1 AND pr.isFixed = false")
    List<PaymentReconciliation> findUnfixedDiscrepanciesByType(String discrepancyType);

    /**
     * 更新差异记录为已处理状态
     * @param orderNo 订单号
     * @return 更新的记录数
     */
    @Modifying
    @Query(nativeQuery = true, value = "UPDATE payment_reconciliation SET is_fixed = true WHERE order_no = ?1")
    int markAsFixed(String orderNo);
} 