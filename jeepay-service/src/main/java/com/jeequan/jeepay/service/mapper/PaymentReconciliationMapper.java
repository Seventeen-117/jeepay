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
     * 物化视图删除方法已移除
     * payment_reconciliation 物化视图由 postgresql_reconciliation_schema.sql 脚本管理
     * 不再提供代码级别的物化视图删除功能
     */

    /**
     * 创建MySQL视图的方法已移除
     * 所有视图/物化视图的创建均通过postgresql_reconciliation_schema.sql脚本直接在数据库中创建
     */

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