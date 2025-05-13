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
package com.jeequan.jeepay.pay.service;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.jeequan.jeepay.core.entity.PaymentReconciliation;
import com.jeequan.jeepay.service.mapper.PaymentReconciliationMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PostgreSQL支付对账服务 (MyBatis-Plus版)
 * 该服务使用专用的PostgreSQL数据源，无论主数据源是什么都会使用PostgreSQL的物化视图进行对账
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/9/16
 */
@Service
@Slf4j
public class PostgreSQLReconciliationServiceMybatis extends ServiceImpl<PaymentReconciliationMapper, PaymentReconciliation> {

    @Autowired
    @Qualifier("reconciliationJdbcTemplate")
    private JdbcTemplate jdbcTemplate;
    
    @Value("${jeepay.reconciliation.concurrent-refresh:false}")
    private boolean concurrentRefresh;
    
    /**
     * 刷新支付对账物化视图
     */
    public void refreshReconciliationView() {
        try {
            log.info("刷新PostgreSQL支付对账视图...");
            
            // 首先检查是否能够连接到数据库
            if (!checkDatabaseConnection()) {
                log.error("无法连接到PostgreSQL数据库，跳过刷新操作");
                return;
            }
            
            // 检查数据库是否支持物化视图
            boolean materializedViewSupported = checkMaterializedViewSupport();
            
            // 确保源表存在并且结构正确
            if (!checkSourceTablesExist()) {
                log.error("源表不存在或结构不正确，跳过刷新操作");
                
                // 尝试修复，例如创建缺失的表或列
                try {
                    repairSourceTables();
                } catch (Exception e) {
                    log.error("修复源表失败: {}", e.getMessage(), e);
                }
                return;
            }
            
            if (materializedViewSupported) {
                // 使用物化视图方式
                refreshWithMaterializedView();
            } else {
                // 使用普通表方式
                refreshWithRegularTable();
            }
        } catch (Exception e) {
            log.error("刷新PostgreSQL支付对账视图过程中发生错误: {}", e.getMessage(), e);
            // 不抛出异常，让系统继续运行
        }
    }
    
    /**
     * 检查数据库连接是否正常
     */
    private boolean checkDatabaseConnection() {
        try {
            jdbcTemplate.queryForObject("SELECT 1", Integer.class);
            return true;
        } catch (Exception e) {
            log.error("PostgreSQL数据库连接异常: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 检查是否支持物化视图
     */
    private boolean checkMaterializedViewSupport() {
        try {
            // 更可靠的方法检查物化视图功能
            String checkSql = 
                "SELECT EXISTS (" +
                "   SELECT 1 FROM pg_namespace n JOIN pg_class c ON n.oid = c.relnamespace " +
                "   WHERE n.nspname = 'pg_catalog' AND c.relname = 'pg_matviews'" +
                ")";
            
            Boolean result = jdbcTemplate.queryForObject(checkSql, Boolean.class);
            boolean supported = result != null && result;
            
            if (supported) {
                log.info("当前PostgreSQL数据库支持物化视图功能");
            } else {
                log.warn("当前PostgreSQL数据库不支持物化视图功能，将使用普通表代替");
            }
            
            return supported;
        } catch (Exception e) {
            log.warn("检查物化视图支持时出错，将使用普通表代替: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * 检查源表是否存在
     */
    private boolean checkSourceTablesExist() {
        try {
            // 检查t_pay_order表
            String checkPayOrderTableSql = 
                "SELECT EXISTS (" +
                "   SELECT FROM pg_catalog.pg_tables " +
                "   WHERE tablename = 't_pay_order'" +
                ");";
            
            Boolean payOrderTableExists = jdbcTemplate.queryForObject(checkPayOrderTableSql, Boolean.class);
            if (payOrderTableExists == null || !payOrderTableExists) {
                log.error("t_pay_order表不存在");
                return false;
            }
            
            // 检查payment_records表
            String checkPaymentRecordsTableSql = 
                "SELECT EXISTS (" +
                "   SELECT FROM pg_catalog.pg_tables " +
                "   WHERE tablename = 'payment_records'" +
                ");";
            
            Boolean paymentRecordsTableExists = jdbcTemplate.queryForObject(checkPaymentRecordsTableSql, Boolean.class);
            if (paymentRecordsTableExists == null || !paymentRecordsTableExists) {
                log.error("payment_records表不存在");
                return false;
            }
            
            // 验证t_pay_order表的必要列
            String checkPayOrderColumnsSql = 
                "SELECT " +
                "  COUNT(*) = 5 AS columns_exist " +
                "FROM " +
                "  information_schema.columns " +
                "WHERE " +
                "  table_name = 't_pay_order' AND " +
                "  column_name IN ('pay_order_id', 'amount', 'state', 'if_code', 'backup_if_code');";
            
            Boolean payOrderColumnsExist = jdbcTemplate.queryForObject(checkPayOrderColumnsSql, Boolean.class);
            if (payOrderColumnsExist == null || !payOrderColumnsExist) {
                log.error("t_pay_order表缺少必要列");
                return false;
            }
            
            // 验证payment_records表的必要列
            String checkPaymentRecordsColumnsSql = 
                "SELECT " +
                "  COUNT(*) = 2 AS columns_exist " +
                "FROM " +
                "  information_schema.columns " +
                "WHERE " +
                "  table_name = 'payment_records' AND " +
                "  column_name IN ('order_no', 'amount');";
            
            Boolean paymentRecordsColumnsExist = jdbcTemplate.queryForObject(checkPaymentRecordsColumnsSql, Boolean.class);
            if (paymentRecordsColumnsExist == null || !paymentRecordsColumnsExist) {
                log.error("payment_records表缺少必要列");
                return false;
            }
            
            return true;
        } catch (Exception e) {
            log.error("检查源表是否存在时出错: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 尝试修复源表结构
     */
    private void repairSourceTables() {
        try {
            log.info("尝试修复源表结构...");
            
            // 检查t_pay_order表是否存在
            String checkPayOrderTableSql = 
                "SELECT EXISTS (" +
                "   SELECT FROM pg_catalog.pg_tables " +
                "   WHERE tablename = 't_pay_order'" +
                ");";
            
            Boolean payOrderTableExists = jdbcTemplate.queryForObject(checkPayOrderTableSql, Boolean.class);
            
            if (payOrderTableExists == null || !payOrderTableExists) {
                log.warn("t_pay_order表不存在，尝试创建基本结构...");
                
                // 创建一个基本的t_pay_order表结构
                String createPayOrderTableSql = 
                    "CREATE TABLE IF NOT EXISTS t_pay_order (" +
                    "    pay_order_id VARCHAR(64) PRIMARY KEY," +
                    "    amount DECIMAL(20,6) NOT NULL," +
                    "    state INT NOT NULL," +
                    "    if_code VARCHAR(30)," +
                    "    backup_if_code VARCHAR(30)," +
                    "    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP" +
                    ");";
                
                jdbcTemplate.execute(createPayOrderTableSql);
                log.info("t_pay_order表基本结构创建完成");
            }
            
            // 检查payment_records表是否存在
            String checkPaymentRecordsTableSql = 
                "SELECT EXISTS (" +
                "   SELECT FROM pg_catalog.pg_tables " +
                "   WHERE tablename = 'payment_records'" +
                ");";
            
            Boolean paymentRecordsTableExists = jdbcTemplate.queryForObject(checkPaymentRecordsTableSql, Boolean.class);
            
            if (paymentRecordsTableExists == null || !paymentRecordsTableExists) {
                log.warn("payment_records表不存在，尝试创建基本结构...");
                
                // 创建一个基本的payment_records表结构
                String createPaymentRecordsTableSql = 
                    "CREATE TABLE IF NOT EXISTS payment_records (" +
                    "    id SERIAL PRIMARY KEY," +
                    "    order_no VARCHAR(64) NOT NULL," +
                    "    amount DECIMAL(20,6) NOT NULL," +
                    "    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                    "    CONSTRAINT uk_payment_records_order_no UNIQUE (order_no)" +
                    ");";
                
                jdbcTemplate.execute(createPaymentRecordsTableSql);
                log.info("payment_records表基本结构创建完成");
            }
            
            // 检查t_pay_order表的列是否完整
            checkAndRepairTableColumns("t_pay_order", new String[][]{
                {"pay_order_id", "VARCHAR(64)"},
                {"amount", "DECIMAL(20,6)"},
                {"state", "INT"},
                {"if_code", "VARCHAR(30)"},
                {"backup_if_code", "VARCHAR(30)"}
            });
            
            // 检查payment_records表的列是否完整
            checkAndRepairTableColumns("payment_records", new String[][]{
                {"order_no", "VARCHAR(64)"},
                {"amount", "DECIMAL(20,6)"}
            });
            
            log.info("源表结构修复完成");
        } catch (Exception e) {
            log.error("修复源表结构过程中出错: {}", e.getMessage(), e);
            throw e;
        }
    }
    
    /**
     * 检查并修复表的列
     * @param tableName 表名
     * @param columns 列定义数组，每个元素是包含列名和类型的数组
     */
    private void checkAndRepairTableColumns(String tableName, String[][] columns) {
        for (String[] column : columns) {
            String columnName = column[0];
            String columnType = column[1];
            
            // 检查列是否存在
            String checkColumnSql = 
                "SELECT EXISTS (" +
                "   SELECT FROM information_schema.columns " +
                "   WHERE table_name = ? AND column_name = ?" +
                ");";
            
            Boolean columnExists = jdbcTemplate.queryForObject(checkColumnSql, Boolean.class, tableName, columnName);
            
            if (columnExists == null || !columnExists) {
                log.warn("表[{}]缺少列[{}]，尝试添加...", tableName, columnName);
                
                // 添加缺失的列
                String addColumnSql = "ALTER TABLE " + tableName + " ADD COLUMN " + columnName + " " + columnType;
                
                try {
                    jdbcTemplate.execute(addColumnSql);
                    log.info("表[{}]添加列[{}]成功", tableName, columnName);
                } catch (Exception e) {
                    log.error("表[{}]添加列[{}]失败: {}", tableName, columnName, e.getMessage(), e);
                }
            }
        }
    }

    /**
     * 使用物化视图方式刷新对账数据
     */
    private void refreshWithMaterializedView() {
        try {
            // 再做一次具体的物化视图功能测试，以确保真的支持
            if (!checkSpecificMaterializedViewSupport()) {
                log.warn("物化视图功能测试失败，切换到普通表方式");
                refreshWithRegularTable();
                return;
            }
            
            // 检查物化视图是否存在
            boolean viewExists = checkMaterializedViewExists();
            
            if (!viewExists) {
                // 物化视图不存在，尝试创建
                log.info("物化视图不存在，尝试创建...");
                ensureReconciliationViewExists();
                // 再次检查是否创建成功
                viewExists = checkMaterializedViewExists();
                if (!viewExists) {
                    log.error("物化视图创建失败，切换到普通表方式");
                    refreshWithRegularTable();
                    return;
                }
            }
            
            // 检查是否有唯一索引，决定刷新方式
            boolean hasUniqueIndex = checkUniqueIndexExists();
            boolean useConurrentRefresh = concurrentRefresh && hasUniqueIndex;
            
            try {
                if (useConurrentRefresh) {
                    // 尝试并发刷新
                    log.info("使用并发方式刷新物化视图...");
                    jdbcTemplate.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY payment_reconciliation");
                    log.info("物化视图并发刷新完成");
                } else {
                    // 使用普通刷新
                    log.info("使用普通方式刷新物化视图...");
                    jdbcTemplate.execute("REFRESH MATERIALIZED VIEW payment_reconciliation");
                    log.info("物化视图普通刷新完成");
                }
            } catch (Exception e) {
                log.error("刷新物化视图失败: {}", e.getMessage(), e);
                
                // 记录导致失败的具体异常信息
                Throwable rootCause = getRootCause(e);
                log.error("刷新失败的根本原因: {}", rootCause.getMessage());
                
                // 尝试重建物化视图
                log.info("尝试重建物化视图...");
                try {
                    createMaterializedViewSafely();
                    log.info("物化视图重建成功");
                } catch (Exception rebuildEx) {
                    log.error("物化视图重建失败: {}", rebuildEx.getMessage(), rebuildEx);
                    
                    // 尝试修复数据一致性问题，例如删除异常数据
                    tryRepairDataInconsistency();
                    
                    // 如果物化视图仍然失败，尝试使用普通表方式
                    log.info("尝试切换到普通表方式...");
                    refreshWithRegularTable();
                }
            }
        } catch (Exception e) {
            log.error("使用物化视图刷新对账数据失败: {}", e.getMessage(), e);
            
            // 尝试切换到普通表方式
            log.info("尝试切换到普通表方式...");
            refreshWithRegularTable();
        }
    }
    
    /**
     * 使用普通表方式刷新对账数据
     */
    private void refreshWithRegularTable() {
        try {
            log.info("使用普通表方式刷新对账数据...");
            
            // 检查对账表是否存在
            ensureReconciliationTableExists();
            
            // 清空对账表并重新填充数据
            refreshReconciliationTable();
            
            log.info("普通表方式刷新完成");
        } catch (Exception e) {
            log.error("使用普通表方式刷新对账数据失败: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 获取异常的根本原因
     */
    private Throwable getRootCause(Throwable throwable) {
        Throwable cause = throwable.getCause();
        if (cause == null) {
            return throwable;
        }
        return getRootCause(cause);
    }
    
    /**
     * 首先确保数据库环境支持物化视图
     */
    private boolean checkSpecificMaterializedViewSupport() {
        try {
            // 简化测试方式，单独执行每条语句
            // 创建临时表
            jdbcTemplate.execute("CREATE TEMP TABLE IF NOT EXISTS mv_test_source(id int)");
            
            // 清空临时表
            jdbcTemplate.execute("TRUNCATE mv_test_source");
            
            // 插入测试数据
            jdbcTemplate.execute("INSERT INTO mv_test_source VALUES(1)");
            
            // 尝试创建物化视图（先删除已存在的）
            try {
                jdbcTemplate.execute("DROP MATERIALIZED VIEW IF EXISTS mv_test");
            } catch (Exception e) {
                log.debug("删除测试物化视图失败，可能不存在: {}", e.getMessage());
                // 继续测试
            }
            
            // 尝试创建物化视图
            jdbcTemplate.execute("CREATE MATERIALIZED VIEW mv_test AS SELECT * FROM mv_test_source");
            
            // 查询物化视图
            jdbcTemplate.queryForList("SELECT * FROM mv_test");
            
            // 刷新物化视图
            jdbcTemplate.execute("REFRESH MATERIALIZED VIEW mv_test");
            
            // 成功创建后删除
            jdbcTemplate.execute("DROP MATERIALIZED VIEW mv_test");
            
            log.info("物化视图功能测试通过，数据库支持物化视图");
            return true;
        } catch (Exception e) {
            log.warn("物化视图功能测试失败，数据库不支持物化视图: {}", e.getMessage());
            return false;
        } finally {
            // 清理临时表
            try {
                jdbcTemplate.execute("DROP TABLE IF EXISTS mv_test_source");
            } catch (Exception e) {
                log.debug("清理临时表失败: {}", e.getMessage());
            }
        }
    }
    
    /**
     * 检查物化视图是否存在
     */
    private boolean checkMaterializedViewExists() {
        try {
            String checkViewSql = 
                "SELECT EXISTS (" +
                "   SELECT FROM pg_catalog.pg_matviews " +
                "   WHERE matviewname = 'payment_reconciliation'" +
                ");";
            
            Boolean viewExists = jdbcTemplate.queryForObject(checkViewSql, Boolean.class);
            return viewExists != null && viewExists;
        } catch (Exception e) {
            log.error("检查物化视图是否存在时出错: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 检查物化视图是否存在唯一索引
     * 只有存在唯一索引的物化视图才能使用CONCURRENTLY刷新
     */
    private boolean checkUniqueIndexExists() {
        try {
            String checkIndexSql = 
                "SELECT EXISTS (" +
                "   SELECT FROM pg_indexes " +
                "   WHERE tablename = 'payment_reconciliation' " +
                "   AND indexdef LIKE '%UNIQUE%'" +
                ");";
            
            Boolean hasIndex = jdbcTemplate.queryForObject(checkIndexSql, Boolean.class);
            return hasIndex != null && hasIndex;
        } catch (Exception e) {
            log.warn("检查唯一索引时出错: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * 检查表是否存在
     */
    private boolean checkTableExists(String tableName) {
        try {
            String checkTableSql = 
                "SELECT EXISTS (" +
                "   SELECT 1 FROM information_schema.tables " +
                "   WHERE table_name = ?" +
                ");";
            
            Boolean tableExists = jdbcTemplate.queryForObject(checkTableSql, Boolean.class, tableName);
            return tableExists != null && tableExists;
        } catch (Exception e) {
            log.error("检查表[{}]是否存在时出错: {}", tableName, e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 尝试修复数据一致性问题
     */
    private void tryRepairDataInconsistency() {
        try {
            log.info("尝试修复数据一致性问题...");
            
            // 检查t_pay_order表中是否有重复的pay_order_id
            String checkDuplicatesSql = 
                "SELECT pay_order_id, COUNT(*) " +
                "FROM t_pay_order " +
                "GROUP BY pay_order_id " +
                "HAVING COUNT(*) > 1 " +
                "LIMIT 10";
            
            List<Map<String, Object>> duplicates = jdbcTemplate.queryForList(checkDuplicatesSql);
            
            if (!duplicates.isEmpty()) {
                log.warn("发现t_pay_order表中有重复的pay_order_id: {}", duplicates);
                
                // 记录这些重复项，但不自动处理，需要人工介入
                for (Map<String, Object> duplicate : duplicates) {
                    String orderId = (String) duplicate.get("pay_order_id");
                    log.error("订单号[{}]在t_pay_order表中存在多条记录，需要人工处理", orderId);
                }
            }
            
            // 检查payment_records表中是否有重复的order_no
            String checkPaymentRecordsDuplicatesSql = 
                "SELECT order_no, COUNT(*) " +
                "FROM payment_records " +
                "GROUP BY order_no " +
                "HAVING COUNT(*) > 1 " +
                "LIMIT 10";
            
            List<Map<String, Object>> paymentRecordsDuplicates = jdbcTemplate.queryForList(checkPaymentRecordsDuplicatesSql);
            
            if (!paymentRecordsDuplicates.isEmpty()) {
                log.warn("发现payment_records表中有重复的order_no: {}", paymentRecordsDuplicates);
                
                // 记录这些重复项，但不自动处理，需要人工介入
                for (Map<String, Object> duplicate : paymentRecordsDuplicates) {
                    String orderId = (String) duplicate.get("order_no");
                    log.error("订单号[{}]在payment_records表中存在多条记录，需要人工处理", orderId);
                }
            }
            
            log.info("数据一致性检查完成");
        } catch (Exception e) {
            log.error("修复数据一致性过程中出错: {}", e.getMessage(), e);
        }
    }

    /**
     * 确保支付对账物化视图存在
     */
    private void ensureReconciliationViewExists() {
        log.debug("检查支付对账物化视图是否存在...");
        try {
            // 再次确认物化视图功能是否可用
            if (!checkSpecificMaterializedViewSupport()) {
                log.error("当前PostgreSQL数据库不支持物化视图功能，将使用普通表代替");
                refreshWithRegularTable();
                return;
            }
            
            // 检查物化视图是否存在
            String checkViewSql = 
                "SELECT EXISTS (" +
                "   SELECT FROM pg_catalog.pg_matviews " +
                "   WHERE matviewname = 'payment_reconciliation'" +
                ");";
            
            Boolean viewExists = jdbcTemplate.queryForObject(checkViewSql, Boolean.class);
            
            if (viewExists == null || !viewExists) {
                log.info("支付对账物化视图不存在，正在创建...");
                
                // 确保t_pay_order_compensation表存在（用于补偿记录）
                ensureCompensationTableExists();
                
                // 先检查源表是否存在
                if (checkSourceTablesExist()) {
                    // 使用安全的方法创建物化视图
                    createMaterializedViewSafely();
                } else {
                    log.error("源表不存在或结构不正确，无法创建物化视图");
                    return;
                }
                
                // 创建刷新函数
                ensureRefreshFunctionExists();
                
                log.info("支付对账物化视图初始化完成");
            } else {
                log.debug("支付对账物化视图已存在");
                
                // 检查视图结构是否正确
                if (!checkViewStructure()) {
                    log.warn("物化视图结构不正确，尝试重建...");
                    createMaterializedViewSafely();
                }
            }
        } catch (Exception e) {
            log.error("检查或创建支付对账物化视图时出错: {}", e.getMessage(), e);
            // 不抛出异常，让系统继续运行
        }
    }
    
    /**
     * 确保补偿表存在
     */
    private void ensureCompensationTableExists() {
        try {
            String checkTableSql = 
                "SELECT EXISTS (" +
                "   SELECT FROM pg_catalog.pg_tables " +
                "   WHERE tablename = 't_pay_order_compensation'" +
                ");";
            
            Boolean tableExists = jdbcTemplate.queryForObject(checkTableSql, Boolean.class);
            
            if (tableExists == null || !tableExists) {
                log.info("支付订单补偿表不存在，正在创建...");
                
                String createTableSql = 
                    "CREATE TABLE IF NOT EXISTS t_pay_order_compensation (" +
                    "    id SERIAL PRIMARY KEY," +
                    "    order_no VARCHAR(64) NOT NULL," +
                    "    discrepancy_amount NUMERIC(20,6) NOT NULL," +
                    "    compensation_amount NUMERIC(20,6) NOT NULL," +
                    "    discrepancy_type VARCHAR(20) NOT NULL," +
                    "    status VARCHAR(20) NOT NULL DEFAULT 'PENDING'," +
                    "    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                    "    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                    "    channel VARCHAR(30)," +
                    "    remarks TEXT," +
                    "    CONSTRAINT uk_order_no UNIQUE (order_no)" +
                    ");";
                
                jdbcTemplate.execute(createTableSql);
                
                // 创建索引
                String createIndexSql = 
                    "CREATE INDEX IF NOT EXISTS idx_t_pay_order_compensation_status ON t_pay_order_compensation (status);" +
                    "CREATE INDEX IF NOT EXISTS idx_t_pay_order_compensation_create_time ON t_pay_order_compensation (create_time);";
                
                jdbcTemplate.execute(createIndexSql);
                
                log.info("支付订单补偿表创建完成");
            }
        } catch (Exception e) {
            log.error("创建补偿表时出错: {}", e.getMessage(), e);
            // 不抛出异常
        }
    }
    
    /**
     * 检查物化视图结构是否正确
     */
    private boolean checkViewStructure() {
        try {
            String checkColumnsSql = 
                "SELECT " +
                "  COUNT(*) = 10 AS columns_exist " +
                "FROM " +
                "  information_schema.columns " +
                "WHERE " +
                "  table_name = 'payment_reconciliation' AND " +
                "  column_name IN ('order_no', 'expected', 'actual', 'discrepancy_type', " +
                "                  'discrepancy_amount', 'is_fixed', 'channel', " +
                "                  'backup_if_code', 'create_time', 'update_time');";
            
            Boolean columnsExist = jdbcTemplate.queryForObject(checkColumnsSql, Boolean.class);
            return columnsExist != null && columnsExist;
        } catch (Exception e) {
            log.warn("检查物化视图结构时出错: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * 安全地创建物化视图
     */
    private void createMaterializedViewSafely() {
        try {
            // 1. 首先尝试删除旧的物化视图（如果存在）
            String dropViewSql = 
                "DO $$ " +
                "BEGIN " +
                "    IF EXISTS (SELECT FROM pg_catalog.pg_matviews WHERE matviewname = 'payment_reconciliation') THEN " +
                "        DROP MATERIALIZED VIEW payment_reconciliation; " +
                "    END IF; " +
                "END $$;";
            
            jdbcTemplate.execute(dropViewSql);
            log.info("旧物化视图已清除（如果存在）");
            
            // 2. 检查源表中是否有数据，避免在无数据的情况下创建视图
            String checkDataSql = "SELECT EXISTS (SELECT 1 FROM t_pay_order LIMIT 1)";
            Boolean hasData = jdbcTemplate.queryForObject(checkDataSql, Boolean.class);
            
            if (hasData == null || !hasData) {
                log.warn("t_pay_order表中没有数据，创建空视图");
            }
            
            // 3. 创建新的物化视图
            // 使用PL/pgSQL块来处理可能的错误
            String createViewSql = 
                "DO $$ " +
                "BEGIN " +
                "    BEGIN " +
                "        CREATE MATERIALIZED VIEW payment_reconciliation AS " +
                "        SELECT " +
                "            po.pay_order_id AS order_no, " +
                "            CAST(po.amount AS NUMERIC(20,6)) AS expected, " +
                "            pr.amount AS actual, " +
                "            CASE " +
                "                WHEN pr.amount IS NULL THEN 'MISSING_PAYMENT' " +
                "                WHEN CAST(po.amount AS NUMERIC(20,6)) != pr.amount THEN 'AMOUNT_MISMATCH' " +
                "                ELSE 'NONE' " +
                "            END AS discrepancy_type, " +
                "            CASE " +
                "                WHEN pr.amount IS NULL THEN CAST(po.amount AS NUMERIC(20,6)) " +
                "                ELSE CAST(po.amount AS NUMERIC(20,6)) - pr.amount " +
                "            END AS discrepancy_amount, " +
                "            0 AS is_fixed, " +  // 注意这里是0而不是FALSE，与实体类字段类型一致
                "            po.if_code AS channel, " +
                "            po.backup_if_code, " +
                "            CURRENT_TIMESTAMP AS create_time, " +
                "            CURRENT_TIMESTAMP AS update_time " +
                "        FROM " +
                "            t_pay_order po " +
                "        LEFT JOIN " +
                "            payment_records pr ON po.pay_order_id = pr.order_no " +
                "        WHERE " +
                "            po.state = 2; " +
                "        EXCEPTION WHEN OTHERS THEN " +
                "            RAISE NOTICE 'Error creating materialized view: %', SQLERRM; " +
                "            RAISE EXCEPTION '%', SQLERRM; " +
                "    END; " +
                "END $$;";
            
            jdbcTemplate.execute(createViewSql);
            log.info("物化视图已创建");
            
            // 4. 刷新物化视图，为创建索引做准备
            jdbcTemplate.execute("REFRESH MATERIALIZED VIEW payment_reconciliation");
            log.info("物化视图已初始刷新");
            
            // 5. 创建索引
            try {
                // 使用事务保护索引创建
                String createIndexSql = 
                    "DO $$ " +
                    "BEGIN " +
                    "    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'payment_reconciliation_pkey') THEN " +
                    "        CREATE UNIQUE INDEX payment_reconciliation_pkey ON payment_reconciliation (order_no); " +
                    "    END IF; " +
                    "END $$;";
                
                jdbcTemplate.execute(createIndexSql);
                log.info("物化视图索引已创建");
            } catch (Exception e) {
                // 索引创建失败不应该阻止视图的使用
                log.warn("创建索引失败，但物化视图仍可使用: {}", e.getMessage());
            }
            
        } catch (Exception e) {
            log.error("创建物化视图失败: {}", e.getMessage(), e);
            
            // 尝试创建一个基本版本的视图（如果上面的失败了）
            try {
                log.info("尝试创建简化版的物化视图...");
                String fallbackViewSql = 
                    "DO $$ " +
                    "BEGIN " +
                    "    BEGIN " +
                    "        CREATE MATERIALIZED VIEW IF NOT EXISTS payment_reconciliation AS " +
                    "        SELECT " +
                    "            po.pay_order_id AS order_no, " +
                    "            CAST(po.amount AS NUMERIC(20,6)) AS expected, " +
                    "            NULL::NUMERIC AS actual, " +
                    "            'MISSING_PAYMENT'::TEXT AS discrepancy_type, " +
                    "            CAST(po.amount AS NUMERIC(20,6)) AS discrepancy_amount, " +
                    "            0 AS is_fixed, " +  // 注意这里是0，与实体类字段类型一致
                    "            po.if_code AS channel, " +
                    "            po.backup_if_code, " +
                    "            CURRENT_TIMESTAMP AS create_time, " +
                    "            CURRENT_TIMESTAMP AS update_time " +
                    "        FROM " +
                    "            t_pay_order po " +
                    "        WHERE " +
                    "            po.state = 2; " +
                    "        EXCEPTION WHEN OTHERS THEN " +
                    "            RAISE NOTICE 'Error creating fallback materialized view: %', SQLERRM; " +
                    "    END; " +
                    "END $$;";
                
                jdbcTemplate.execute(fallbackViewSql);
                log.info("简化版物化视图创建成功");
                
                // 刷新简化版视图
                jdbcTemplate.execute("REFRESH MATERIALIZED VIEW payment_reconciliation");
                
            } catch (Exception fallbackEx) {
                log.error("创建简化版物化视图也失败: {}", fallbackEx.getMessage(), fallbackEx);
            }
        }
    }
    
    /**
     * 确保刷新函数存在
     */
    private void ensureRefreshFunctionExists() {
        try {
            // 首先检查是否存在唯一索引
            boolean hasUniqueIndex = checkUniqueIndexExists();
            final String refreshType = hasUniqueIndex ? "CONCURRENTLY" : "";
            
            // 检查函数是否存在
            String checkFunctionSql = 
                "SELECT EXISTS (" +
                "   SELECT FROM pg_proc " +
                "   WHERE proname = 'refresh_payment_reconciliation'" +
                ");";
            
            Boolean functionExists = jdbcTemplate.queryForObject(checkFunctionSql, Boolean.class);
            
            if (functionExists == null || !functionExists) {
                log.info("刷新函数不存在，正在创建...");
                
                // 创建函数 - 根据索引情况决定是否使用CONCURRENTLY
                String createFunctionSql = 
                    "CREATE OR REPLACE FUNCTION refresh_payment_reconciliation() " +
                    "RETURNS VOID AS $$ " +
                    "BEGIN " +
                    "    REFRESH MATERIALIZED VIEW " + refreshType + " payment_reconciliation; " +
                    "END; " +
                    "$$ LANGUAGE plpgsql;";
                
                jdbcTemplate.execute(createFunctionSql);
                
                // 添加函数注释
                String commentSql = 
                    "COMMENT ON FUNCTION refresh_payment_reconciliation() " +
                    "IS '刷新支付对账物化视图的函数';";
                
                jdbcTemplate.execute(commentSql);
                
                log.info("刷新函数创建完成");
            } else {
                // 更新函数以反映索引的状态
                String updateFunctionSql = 
                    "CREATE OR REPLACE FUNCTION refresh_payment_reconciliation() " +
                    "RETURNS VOID AS $$ " +
                    "BEGIN " +
                    "    REFRESH MATERIALIZED VIEW " + refreshType + " payment_reconciliation; " +
                    "END; " +
                    "$$ LANGUAGE plpgsql;";
                
                jdbcTemplate.execute(updateFunctionSql);
                log.debug("刷新函数已更新");
            }
        } catch (Exception e) {
            log.error("检查或创建刷新函数时出错", e);
            throw e;  // 重新抛出异常，让调用方知道出错了
        }
    }

    /**
     * 确保对账表存在
     */
    private void ensureReconciliationTableExists() {
        try {
            // 检查表是否存在
            String checkTableSql = 
                "SELECT EXISTS (" +
                "   SELECT FROM pg_catalog.pg_tables " +
                "   WHERE tablename = 'payment_reconciliation'" +
                ");";
            
            Boolean tableExists = jdbcTemplate.queryForObject(checkTableSql, Boolean.class);
            
            if (tableExists == null || !tableExists) {
                log.info("支付对账表不存在，正在创建...");
                
                String createTableSql = 
                    "CREATE TABLE IF NOT EXISTS payment_reconciliation (" +
                    "    order_no VARCHAR(64) PRIMARY KEY," +
                    "    expected DECIMAL(20,6) NOT NULL," +
                    "    actual DECIMAL(20,6)," +
                    "    discrepancy_type VARCHAR(20) NOT NULL," +
                    "    discrepancy_amount DECIMAL(20,6) NOT NULL," +
                    "    is_fixed INTEGER NOT NULL DEFAULT 0," +  // 使用Integer类型与实体类匹配
                    "    channel VARCHAR(30)," +
                    "    backup_if_code VARCHAR(30)," +
                    "    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                    "    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP" +
                    ");";
                
                jdbcTemplate.execute(createTableSql);
                
                // 分别创建索引，避免语法错误
                try {
                    jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_payment_reconciliation_discrepancy_type ON payment_reconciliation (discrepancy_type)");
                } catch (Exception e) {
                    log.warn("创建索引idx_payment_reconciliation_discrepancy_type失败: {}", e.getMessage());
                }
                
                try {
                    jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_payment_reconciliation_is_fixed ON payment_reconciliation (is_fixed)");
                } catch (Exception e) {
                    log.warn("创建索引idx_payment_reconciliation_is_fixed失败: {}", e.getMessage());
                }
                
                try {
                    jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_payment_reconciliation_channel ON payment_reconciliation (channel)");
                } catch (Exception e) {
                    log.warn("创建索引idx_payment_reconciliation_channel失败: {}", e.getMessage());
                }
                
                log.info("支付对账表创建完成");
            } else {
                // 检查表结构是否完整
                ensureTableColumns("payment_reconciliation", new String[][]{
                    {"order_no", "VARCHAR(64)"},
                    {"expected", "DECIMAL(20,6)"},
                    {"actual", "DECIMAL(20,6)"},
                    {"discrepancy_type", "VARCHAR(20)"},
                    {"discrepancy_amount", "DECIMAL(20,6)"},
                    {"is_fixed", "INTEGER"},
                    {"channel", "VARCHAR(30)"},
                    {"backup_if_code", "VARCHAR(30)"},
                    {"create_time", "TIMESTAMP"},
                    {"update_time", "TIMESTAMP"}
                });
            }
        } catch (Exception e) {
            log.error("确保对账表存在时出错: {}", e.getMessage(), e);
            // 不直接抛出异常，让系统继续运行
        }
    }
    
    /**
     * 确保表包含所有必要的列
     */
    private void ensureTableColumns(String tableName, String[][] columns) {
        for (String[] column : columns) {
            String columnName = column[0];
            String columnType = column[1];
            
            // 检查列是否存在
            String checkColumnSql = 
                "SELECT EXISTS (" +
                "   SELECT FROM information_schema.columns " +
                "   WHERE table_name = ? AND column_name = ?" +
                ");";
            
            Boolean columnExists = jdbcTemplate.queryForObject(checkColumnSql, Boolean.class, tableName, columnName);
            
            if (columnExists == null || !columnExists) {
                log.warn("表[{}]缺少列[{}]，尝试添加...", tableName, columnName);
                
                // 添加缺失的列
                String addColumnSql = "ALTER TABLE " + tableName + " ADD COLUMN " + columnName + " " + columnType;
                
                try {
                    jdbcTemplate.execute(addColumnSql);
                    log.info("表[{}]添加列[{}]成功", tableName, columnName);
                } catch (Exception e) {
                    log.error("表[{}]添加列[{}]失败: {}", tableName, columnName, e.getMessage(), e);
                }
            }
        }
    }
    
    /**
     * 刷新对账表数据
     */
    private void refreshReconciliationTable() {
        try {
            log.info("开始刷新对账表数据...");
            
            // 首先检查表是否存在
            if (!checkTableExists("payment_reconciliation")) {
                log.error("payment_reconciliation表不存在，无法刷新");
                // 尝试重新创建表
                ensureReconciliationTableExists();
                
                // 再次检查表是否存在
                if (!checkTableExists("payment_reconciliation")) {
                    log.error("无法创建payment_reconciliation表，中止刷新");
                    return;
                }
            }

            // 使用DELETE替代TRUNCATE，更兼容
            try {
                log.info("清空对账表中的旧数据...");
                jdbcTemplate.execute("DELETE FROM payment_reconciliation");
            } catch (Exception e) {
                log.warn("使用DELETE清空表失败，尝试其他方式: {}", e.getMessage());
                
                // 尝试不清空表，直接使用INSERT或UPDATE方式
                log.info("跳过清空表步骤，直接更新数据");
                
                // 获取现有记录的订单号列表
                List<String> existingOrderNos = new ArrayList<>();
                try {
                    existingOrderNos = jdbcTemplate.queryForList("SELECT order_no FROM payment_reconciliation", String.class);
                } catch (Exception ex) {
                    log.warn("获取现有订单号失败: {}", ex.getMessage());
                    // 继续处理，假设没有现有记录
                }
                
                // 如果有现有记录，先尝试删除
                if (!existingOrderNos.isEmpty()) {
                    log.info("发现{}条现有记录，尝试逐条删除", existingOrderNos.size());
                    for (String orderNo : existingOrderNos) {
                        try {
                            jdbcTemplate.update("DELETE FROM payment_reconciliation WHERE order_no = ?", orderNo);
                        } catch (Exception ex) {
                            log.warn("删除订单号{}失败: {}", orderNo, ex.getMessage());
                            // 继续处理其他记录
                        }
                    }
                }
            }
            
            // 使用INSERT INTO SELECT语句重新填充数据
            log.info("开始插入最新对账数据...");
            try {
                // 不直接从多个表中选择，而是先获取数据，然后逐条插入
                // 这样可以避免复杂SQL语句的兼容性问题
                
                // 获取支付订单数据
                String orderSql = "SELECT pay_order_id, amount, if_code, backup_if_code FROM t_pay_order WHERE state = 2";
                List<Map<String, Object>> orders = jdbcTemplate.queryForList(orderSql);
                
                log.info("找到{}个需要处理的支付订单", orders.size());
                int successCount = 0;
                
                for (Map<String, Object> order : orders) {
                    try {
                        String payOrderId = (String) order.get("pay_order_id");
                        BigDecimal amount = (BigDecimal) order.get("amount");
                        String ifCode = (String) order.get("if_code");
                        String backupIfCode = (String) order.get("backup_if_code");
                        
                        // 查询对应的支付记录
                        BigDecimal actualAmount = null;
                        try {
                            actualAmount = jdbcTemplate.queryForObject(
                                    "SELECT amount FROM payment_records WHERE order_no = ?", 
                                    BigDecimal.class, payOrderId);
                        } catch (Exception ex) {
                            // 可能没有对应的支付记录，actualAmount保持为null
                        }
                        
                        // 确定差异类型和差异金额
                        String discrepancyType;
                        BigDecimal discrepancyAmount;
                        
                        if (actualAmount == null) {
                            discrepancyType = "MISSING_PAYMENT";
                            discrepancyAmount = amount;
                        } else if (actualAmount.compareTo(amount) != 0) {
                            discrepancyType = "AMOUNT_MISMATCH";
                            discrepancyAmount = amount.subtract(actualAmount);
                        } else {
                            discrepancyType = "NONE";
                            discrepancyAmount = BigDecimal.ZERO;
                        }
                        
                        // 使用参数化SQL插入记录，避免SQL语法问题
                        jdbcTemplate.update(
                                "INSERT INTO payment_reconciliation " +
                                "(order_no, expected, actual, discrepancy_type, discrepancy_amount, " +
                                "is_fixed, channel, backup_if_code, create_time, update_time) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                                payOrderId, amount, actualAmount, discrepancyType, 
                                discrepancyAmount, 0, ifCode, backupIfCode);  // 使用0代表false与实体类匹配
                        
                        successCount++;
                    } catch (Exception e) {
                        log.warn("处理订单时出错: {}", e.getMessage());
                        // 继续处理其他订单
                    }
                }
                
                log.info("对账表数据刷新完成，成功插入{}条记录", successCount);
            } catch (Exception e) {
                log.error("批量插入数据失败: {}", e.getMessage(), e);
                
                // 尝试分批插入
                log.info("尝试分批插入...");
                tryBatchInsert();
            }
        } catch (Exception e) {
            log.error("刷新对账表数据失败: {}", e.getMessage(), e);
            // 不抛出异常，让系统继续运行
        }
    }
    
    /**
     * 尝试分批获取数据并插入
     */
    private void tryBatchInsert() {
        try {
            log.info("尝试分批获取数据并插入...");
            
            // 首先获取需要处理的支付订单
            String orderSql = "SELECT pay_order_id, amount, if_code, backup_if_code FROM t_pay_order WHERE state = 2";
            List<Map<String, Object>> orders = jdbcTemplate.queryForList(orderSql);
            
            if (orders.isEmpty()) {
                log.info("没有找到需要处理的支付订单");
                return;
            }
            
            log.info("找到{}个支付订单需要处理", orders.size());
            int successCount = 0;
            
            // 逐条处理订单
            for (Map<String, Object> order : orders) {
                try {
                    String payOrderId = (String) order.get("pay_order_id");
                    BigDecimal amount = (BigDecimal) order.get("amount");
                    String ifCode = (String) order.get("if_code");
                    String backupIfCode = (String) order.get("backup_if_code");
                    
                    // 查询对应的支付记录
                    List<Map<String, Object>> paymentRecords = jdbcTemplate.queryForList(
                            "SELECT amount FROM payment_records WHERE order_no = ?", 
                            payOrderId);
                    
                    BigDecimal actualAmount = null;
                    if (!paymentRecords.isEmpty() && paymentRecords.get(0).get("amount") != null) {
                        actualAmount = (BigDecimal) paymentRecords.get(0).get("amount");
                    }
                    
                    // 确定差异类型和差异金额
                    String discrepancyType;
                    BigDecimal discrepancyAmount;
                    
                    if (actualAmount == null) {
                        discrepancyType = "MISSING_PAYMENT";
                        discrepancyAmount = amount;
                    } else if (!actualAmount.equals(amount)) {
                        discrepancyType = "AMOUNT_MISMATCH";
                        discrepancyAmount = amount.subtract(actualAmount);
                    } else {
                        discrepancyType = "NONE";
                        discrepancyAmount = BigDecimal.ZERO;
                    }
                    
                    // 先检查记录是否已存在
                    boolean exists = false;
                    try {
                        exists = Boolean.TRUE.equals(jdbcTemplate.queryForObject(
                                "SELECT EXISTS(SELECT 1 FROM payment_reconciliation WHERE order_no = ?)",
                                Boolean.class, payOrderId));
                    } catch (Exception e) {
                        log.debug("检查记录是否存在时出错: {}", e.getMessage());
                    }
                    
                    // 根据记录是否存在执行更新或插入
                    if (exists) {
                        // 使用更新
                        jdbcTemplate.update(
                                "UPDATE payment_reconciliation SET " +
                                "expected = ?, actual = ?, discrepancy_type = ?, " +
                                "discrepancy_amount = ?, is_fixed = 0, " +  // 使用0代表false与实体类匹配
                                "channel = ?, backup_if_code = ?, update_time = CURRENT_TIMESTAMP " +
                                "WHERE order_no = ?",
                                amount, actualAmount, discrepancyType, 
                                discrepancyAmount, ifCode, backupIfCode, payOrderId);
                    } else {
                        // 使用插入
                        jdbcTemplate.update(
                                "INSERT INTO payment_reconciliation " +
                                "(order_no, expected, actual, discrepancy_type, discrepancy_amount, " +
                                "is_fixed, channel, backup_if_code, create_time, update_time) " +
                                "VALUES (?, ?, ?, ?, ?, 0, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                                payOrderId, amount, actualAmount, discrepancyType, 
                                discrepancyAmount, ifCode, backupIfCode);
                    }
                    
                    successCount++;
                } catch (Exception e) {
                    log.warn("处理订单时出错: {}", e.getMessage());
                    // 继续处理其他订单
                }
            }
            
            log.info("分批处理完成，成功处理{}个订单", successCount);
        } catch (Exception e) {
            log.error("分批插入数据失败: {}", e.getMessage(), e);
            
            // 最后尝试最简化的插入方式
            log.info("尝试更简单的插入方式...");
            trySimplifiedInsert();
        }
    }
    
    /**
     * 尝试使用最简化的方式插入数据
     */
    private void trySimplifiedInsert() {
        try {
            // 获取基本订单信息
            String sql = "SELECT pay_order_id FROM t_pay_order WHERE state = 2";
            List<String> orderIds = jdbcTemplate.queryForList(sql, String.class);
            
            if (orderIds.isEmpty()) {
                log.info("没有找到需要处理的订单");
                return;
            }
            
            log.info("找到{}个需要处理的订单，使用最基本方式插入", orderIds.size());
            int count = 0;
            
            for (String orderId : orderIds) {
                try {
                    // 使用最简单的插入语句，只包含必要字段
                    jdbcTemplate.update(
                            "INSERT INTO payment_reconciliation (order_no, expected, discrepancy_type, discrepancy_amount, is_fixed, create_time, update_time) " +
                            "VALUES (?, 0, 'NONE', 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                            orderId);
                    count++;
                } catch (Exception e) {
                    log.warn("简化插入订单[{}]失败: {}", orderId, e.getMessage());
                }
            }
            
            log.info("简化插入完成，成功插入{}条记录", count);
            
            // 如果有成功插入的记录，尝试更新其他字段
            if (count > 0) {
                log.info("尝试更新已插入记录的详细信息");
                updateReconciliationDetails();
            }
        } catch (Exception e) {
            log.error("简化插入失败: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 更新已插入记录的详细信息
     */
    private void updateReconciliationDetails() {
        try {
            // 获取所有需要更新的记录
            List<Map<String, Object>> records = jdbcTemplate.queryForList(
                    "SELECT order_no FROM payment_reconciliation");
            
            for (Map<String, Object> record : records) {
                String orderNo = (String) record.get("order_no");
                
                try {
                    // 获取订单金额和渠道信息
                    Map<String, Object> orderInfo = jdbcTemplate.queryForMap(
                            "SELECT amount, if_code, backup_if_code FROM t_pay_order WHERE pay_order_id = ?",
                            orderNo);
                    
                    if (orderInfo != null && !orderInfo.isEmpty()) {
                        BigDecimal amount = (BigDecimal) orderInfo.get("amount");
                        String ifCode = (String) orderInfo.get("if_code");
                        String backupIfCode = (String) orderInfo.get("backup_if_code");
                        
                        // 获取实际支付金额
                        BigDecimal actualAmount = null;
                        try {
                            actualAmount = jdbcTemplate.queryForObject(
                                    "SELECT amount FROM payment_records WHERE order_no = ?",
                                    BigDecimal.class, orderNo);
                        } catch (Exception e) {
                            // 可能没有对应的支付记录
                        }
                        
                        // 确定差异类型和差异金额
                        String discrepancyType;
                        BigDecimal discrepancyAmount;
                        
                        if (actualAmount == null) {
                            discrepancyType = "MISSING_PAYMENT";
                            discrepancyAmount = amount;
                        } else if (actualAmount.compareTo(amount) != 0) {
                            discrepancyType = "AMOUNT_MISMATCH";
                            discrepancyAmount = amount.subtract(actualAmount);
                        } else {
                            discrepancyType = "NONE";
                            discrepancyAmount = BigDecimal.ZERO;
                        }
                        
                        // 更新记录
                        jdbcTemplate.update(
                                "UPDATE payment_reconciliation SET " +
                                "expected = ?, actual = ?, discrepancy_type = ?, " +
                                "discrepancy_amount = ?, channel = ?, backup_if_code = ? " +
                                "WHERE order_no = ?",
                                amount, actualAmount, discrepancyType, 
                                discrepancyAmount, ifCode, backupIfCode, orderNo);
                    }
                } catch (Exception e) {
                    log.warn("更新订单[{}]详细信息失败: {}", orderNo, e.getMessage());
                }
            }
            
            log.info("详细信息更新完成");
        } catch (Exception e) {
            log.error("更新详细信息失败: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 查询所有未修复的支付差异记录
     * @return 差异记录列表
     */
    public List<PaymentReconciliation> findAllUnfixedDiscrepancies() {
        try {
            // 先刷新视图以确保数据最新
            refreshReconciliationView();
            
            // 使用MyBatis-Plus查询未修复的支付差异记录
            return lambdaQuery()
                    .ne(PaymentReconciliation::getDiscrepancyType, "NONE")
                    .eq(PaymentReconciliation::getIsFixed, 0)
                    .list();
        } catch (Exception e) {
            log.error("查询未修复的支付差异记录失败: {}", e.getMessage(), e);
            return List.of();
        }
    }
    
    /**
     * 根据订单号查询支付对账记录
     * @param orderNo 订单号
     * @return 对账记录
     */
    public PaymentReconciliation findByOrderNo(String orderNo) {
        try {
            // 使用MyBatis-Plus查询指定订单号的支付对账记录
            return lambdaQuery()
                    .eq(PaymentReconciliation::getOrderNo, orderNo)
                    .one();
        } catch (Exception e) {
            log.error("根据订单号查询支付对账记录失败: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * 标记对账差异已修复
     * @param orderNo 订单号
     * @return 是否成功
     */
    public boolean markAsFixed(String orderNo) {
        try {
            // 使用MyBatis-Plus更新记录
            return lambdaUpdate()
                    .eq(PaymentReconciliation::getOrderNo, orderNo)
                    .set(PaymentReconciliation::getIsFixed, 1) // 使用1代表true与实体类匹配
                    .set(PaymentReconciliation::getUpdateTime, new java.util.Date())
                    .update();
        } catch (Exception e) {
            log.error("标记对账差异已修复失败: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 获取对账统计信息
     * @return 统计信息
     */
    public Map<String, Object> getReconciliationStats() {
        try {
            // 先刷新视图以确保数据最新
            refreshReconciliationView();
            
            // 检查表是否存在
            String checkTableSql = 
                "SELECT EXISTS (" +
                "   SELECT 1 FROM information_schema.tables " +
                "   WHERE table_name = 'payment_reconciliation'" +
                ");";
            
            Boolean tableExists = jdbcTemplate.queryForObject(checkTableSql, Boolean.class);
            
            if (tableExists == null || !tableExists) {
                log.error("payment_reconciliation表不存在，无法获取统计信息");
                return createEmptyStats();
            }
            
            // 使用MyBatis-Plus的聚合查询功能较复杂，这里仍使用原生SQL
            String sql = "SELECT " +
                    "COUNT(*) AS total_records, " +
                    "SUM(CASE WHEN discrepancy_type != 'NONE' THEN 1 ELSE 0 END) AS total_discrepancies, " +
                    "SUM(CASE WHEN discrepancy_type != 'NONE' AND is_fixed = 1 THEN 1 ELSE 0 END) AS fixed_discrepancies, " +
                    "SUM(CASE WHEN discrepancy_type = 'AMOUNT_MISMATCH' THEN 1 ELSE 0 END) AS amount_mismatches, " +
                    "SUM(CASE WHEN discrepancy_type = 'MISSING_PAYMENT' THEN 1 ELSE 0 END) AS missing_payments, " +
                    "COALESCE(SUM(CASE WHEN discrepancy_type != 'NONE' THEN discrepancy_amount ELSE 0 END), 0) AS total_discrepancy_amount " +
                    "FROM payment_reconciliation";
            
            try {
                return jdbcTemplate.queryForMap(sql);
            } catch (Exception e) {
                log.error("获取对账统计信息出错: {}", e.getMessage(), e);
                return createEmptyStats();
            }
        } catch (Exception e) {
            log.error("获取对账统计信息失败: {}", e.getMessage(), e);
            return createEmptyStats();
        }
    }
    
    /**
     * 创建空的统计信息
     */
    private Map<String, Object> createEmptyStats() {
        return Map.of(
            "total_records", 0,
            "total_discrepancies", 0,
            "fixed_discrepancies", 0,
            "amount_mismatches", 0,
            "missing_payments", 0,
            "total_discrepancy_amount", 0
        );
    }
    
    /**
     * 按日期范围查询对账差异
     * @param startDate 开始日期 (yyyy-MM-dd)
     * @param endDate 结束日期 (yyyy-MM-dd)
     * @return 差异记录列表
     */
    public List<PaymentReconciliation> findDiscrepanciesByDateRange(String startDate, String endDate) {
        try {
            // 先刷新视图以确保数据最新
            refreshReconciliationView();
            
            // 日期处理
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            LocalDateTime startDateTime = LocalDateTime.parse(startDate + " 00:00:00", 
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            LocalDateTime endDateTime = LocalDateTime.parse(endDate + " 23:59:59", 
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                
            // 转换为java.util.Date类型
            java.util.Date startDateObj = java.sql.Timestamp.valueOf(startDateTime);
            java.util.Date endDateObj = java.sql.Timestamp.valueOf(endDateTime);
            
            // 使用MyBatis-Plus查询
            return lambdaQuery()
                    .ne(PaymentReconciliation::getDiscrepancyType, "NONE")
                    .between(PaymentReconciliation::getCreateTime, startDateObj, endDateObj)
                    .list();
        } catch (Exception e) {
            log.error("按日期范围查询对账差异失败: {}", e.getMessage(), e);
            return List.of();
        }
    }
    
    /**
     * 按渠道统计对账差异
     * @return 各渠道差异统计
     */
    public List<Map<String, Object>> getDiscrepanciesByChannel() {
        try {
            // 先刷新视图以确保数据最新
            refreshReconciliationView();
            
            // 检查表是否存在
            if (!checkTableExists("payment_reconciliation")) {
                log.error("payment_reconciliation表不存在，无法统计渠道差异");
                return List.of();
            }
            
            // 使用原生SQL进行统计，MyBatis-Plus对GROUP BY支持有限
            String sql = "SELECT channel, " +
                    "COUNT(*) AS total_records, " +
                    "SUM(CASE WHEN discrepancy_type != 'NONE' THEN 1 ELSE 0 END) AS total_discrepancies, " +
                    "COALESCE(SUM(CASE WHEN discrepancy_type != 'NONE' THEN discrepancy_amount ELSE 0 END), 0) AS total_discrepancy_amount " +
                    "FROM payment_reconciliation " +
                    "GROUP BY channel " +
                    "ORDER BY total_discrepancies DESC";
            
            return jdbcTemplate.queryForList(sql);
        } catch (Exception e) {
            log.error("按渠道统计对账差异失败: {}", e.getMessage(), e);
            return List.of();
        }
    }
    
    /**
     * 修复所有差异
     * 该方法仅用于标记差异为已修复，不会执行实际的金额转移等操作
     * @return 修复的记录数
     */
    public int fixAllDiscrepancies() {
        try {
            // 使用MyBatis-Plus批量更新
            boolean success = lambdaUpdate()
                    .ne(PaymentReconciliation::getDiscrepancyType, "NONE")
                    .eq(PaymentReconciliation::getIsFixed, 0)
                    .set(PaymentReconciliation::getIsFixed, 1) // 使用1代表true与实体类匹配
                    .set(PaymentReconciliation::getUpdateTime, new java.util.Date())
                    .update();
            
            // 如果更新成功，获取更新记录数
            if (success) {
                // 查询已修复的记录数
                return lambdaQuery()
                        .ne(PaymentReconciliation::getDiscrepancyType, "NONE")
                        .eq(PaymentReconciliation::getIsFixed, 1)
                        .count().intValue();
            }
            return 0;
        } catch (Exception e) {
            log.error("修复所有差异失败: {}", e.getMessage(), e);
            return 0;
        }
    }
} 