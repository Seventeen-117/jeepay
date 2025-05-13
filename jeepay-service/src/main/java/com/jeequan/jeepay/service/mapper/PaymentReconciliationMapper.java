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
 * @author jiangyangpay
 * @since 2023-08-12
 */
@Mapper
public interface PaymentReconciliationMapper extends BaseMapper<PaymentReconciliation> {

    /**
     * 删除视图或物化视图（不管是什么类型都尝试删除）
     * 提供一个通用的方法尝试所有可能的对象类型
     */
    @Update("DO $$ " +
           "BEGIN " +
           "    BEGIN " +
           "        DROP MATERIALIZED VIEW IF EXISTS payment_reconciliation; " +
           "        RAISE NOTICE 'Dropped materialized view payment_reconciliation'; " +
           "    EXCEPTION WHEN OTHERS THEN " +
           "        RAISE NOTICE 'No materialized view found or other error'; " +
           "    END; " +
           "    BEGIN " +
           "        DROP VIEW IF EXISTS payment_reconciliation; " +
           "        RAISE NOTICE 'Dropped view payment_reconciliation'; " +
           "    EXCEPTION WHEN OTHERS THEN " +
           "        RAISE NOTICE 'No view found or other error'; " +
           "    END; " +
           "    BEGIN " +
           "        DROP TABLE IF EXISTS payment_reconciliation; " +
           "        RAISE NOTICE 'Dropped table payment_reconciliation'; " +
           "    EXCEPTION WHEN OTHERS THEN " +
           "        RAISE NOTICE 'No table found or other error'; " +
           "    END; " +
           "END $$")
    void dropAllReconciliationObjects();
    
    /**
     * 创建MySQL视图（当数据库类型为MySQL时调用）
     */
    @Update("CREATE OR REPLACE VIEW payment_reconciliation AS " +
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
            "po.backup_if_code as backup_if_code, " +
            "NOW() as create_time, " +
            "NOW() as update_time " +
            "FROM t_pay_order po " +
            "LEFT JOIN payment_records pr ON po.pay_order_id = pr.order_no " +
            "WHERE po.state = 2")
    void createMySQLView();
    
    /**
     * 创建PostgreSQL物化视图（当数据库类型为PostgreSQL时调用）
     */
    @Update("CREATE MATERIALIZED VIEW IF NOT EXISTS payment_reconciliation AS " +
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
            "po.backup_if_code as backup_if_code, " +
            "NOW() as create_time, " +
            "NOW() as update_time " +
            "FROM t_pay_order po " +
            "LEFT JOIN payment_records pr ON po.pay_order_id = pr.order_no " +
            "WHERE po.state = 2 " +
            "WITH DATA")
    void createPostgreSQLMaterializedView();
    
    /**
     * 创建对物化视图的索引（提高查询性能）
     */
    @Update("CREATE INDEX IF NOT EXISTS idx_payment_reconciliation_order_no ON payment_reconciliation(order_no)")
    void createMaterializedViewIndex();

    /**
     * 刷新MySQL视图（MySQL视图不需要刷新，此方法为空操作）
     */
    @Update("SELECT 1")
    void refreshMySQLView();
    
    /**
     * 刷新PostgreSQL物化视图
     */
    @Update("DO $$ " +
           "BEGIN " +
           "    BEGIN " +
           "        IF EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'payment_reconciliation') THEN " +
           "            BEGIN " +
           "                -- 先尝试并发刷新 " +
           "                BEGIN " +
           "                    REFRESH MATERIALIZED VIEW CONCURRENTLY payment_reconciliation; " +
           "                    RAISE NOTICE 'Refreshed materialized view CONCURRENTLY'; " +
           "                EXCEPTION WHEN OTHERS THEN " +
           "                    -- 如果并发刷新失败，回退到普通刷新 " +
           "                    REFRESH MATERIALIZED VIEW payment_reconciliation; " +
           "                    RAISE NOTICE 'Concurrent refresh failed, used normal refresh'; " +
           "                END; " +
           "            END; " +
           "        ELSE " +
           "            RAISE NOTICE 'Materialized view payment_reconciliation does not exist'; " +
           "        END IF; " +
           "    EXCEPTION WHEN OTHERS THEN " +
           "        RAISE NOTICE 'Error refreshing materialized view: %', SQLERRM; " +
           "    END; " +
           "END $$")
    void refreshPostgreSQLMaterializedView();

    /**
     * 更新差异记录为已处理状态
     * @param orderNo 订单号
     * @return 更新的记录数
     */
    @Update("UPDATE payment_reconciliation SET is_fixed = 1 WHERE order_no = #{orderNo}")
    int markAsFixed(String orderNo);
} 