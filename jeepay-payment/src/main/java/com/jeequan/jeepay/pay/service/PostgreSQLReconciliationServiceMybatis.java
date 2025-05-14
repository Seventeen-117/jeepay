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
    
    @Value("${jeepay.reconciliation.skip-sql-execution:false}")
    private boolean skipSqlExecution;
    
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
                // 使用物化视图方式
                refreshWithMaterializedView();
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
     * 检查物化视图是否存在唯一索引
     * 只有存在唯一索引的物化视图才能使用CONCURRENTLY刷新
     */
    private boolean checkUniqueIndexExists() {
        try {
            // 首先检查payment_reconciliation是否是物化视图
            boolean isMaterializedView = checkIsMaterializedView("payment_reconciliation");
            
            if (isMaterializedView) {
                // 对于物化视图，检查其关联的索引
                String checkIndexSql = 
                    "SELECT EXISTS (" +
                    "   SELECT 1 FROM pg_indexes " +
                    "   WHERE tablename = 'payment_reconciliation' " +
                    "   AND indexdef LIKE '%UNIQUE%'" +
                    ")";
                
                Boolean hasIndex = jdbcTemplate.queryForObject(checkIndexSql, Boolean.class);
                return hasIndex != null && hasIndex;
            } else {
                // 如果不是物化视图，则不能有唯一索引（PostgreSQL中普通视图不支持索引）
                log.debug("payment_reconciliation不是物化视图，不支持直接创建唯一索引");
                return false;
            }
        } catch (Exception e) {
            log.warn("检查唯一索引时出错: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 使用物化视图方式刷新对账数据
     */
    private void refreshWithMaterializedView() {
        try {
            // 检查是否有唯一索引，只有有唯一索引的情况下才能使用并发刷新
            boolean hasUniqueIndex = checkUniqueIndexExists();
            boolean useConurrentRefresh = concurrentRefresh && hasUniqueIndex;
            
            if (concurrentRefresh && !hasUniqueIndex) {
                log.warn("物化视图没有唯一索引，无法使用并发刷新，将使用普通刷新");
            }
            
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
                log.error("刷新物化视图失败，错误信息: {}", e.getMessage(), e);
                
                // 记录导致失败的具体异常信息
                Throwable rootCause = getRootCause(e);
                log.error("刷新失败的根本原因: {}", rootCause.getMessage());
                
                log.warn("物化视图刷新失败。如果物化视图不存在或结构有问题，请使用postgresql_reconciliation_schema.sql脚本重新创建");
                
                // 尝试切换到普通表方式
                log.info("尝试切换到普通表方式...");
                refreshWithRegularTable();
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
     * 检查对象是否是物化视图
     */
    private boolean checkIsMaterializedView(String viewName) {
        try {
            String checkSql = 
                "SELECT EXISTS (" +
                "   SELECT FROM pg_catalog.pg_matviews " +
                "   WHERE matviewname = ?" +
                ")";
            
            Boolean isMaterializedView = jdbcTemplate.queryForObject(checkSql, Boolean.class, viewName);
            return isMaterializedView != null && isMaterializedView;
        } catch (Exception e) {
            log.warn("检查{}是否为物化视图时出错: {}", viewName, e.getMessage());
            return false;
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
                log.error("payment_reconciliation表/视图不存在，无法刷新");
                log.info("请使用postgresql_reconciliation_schema.sql脚本创建物化视图");
                return; // 直接返回，不尝试创建表/视图
            }

            // 检查payment_reconciliation是否是物化视图
            boolean isMaterializedView = checkIsMaterializedView("payment_reconciliation");
            
            if (isMaterializedView) {
                // 如果是物化视图，使用REFRESH MATERIALIZED VIEW命令
                log.info("payment_reconciliation是物化视图，使用REFRESH MATERIALIZED VIEW刷新...");
                try {
                    // 检查是否有唯一索引以决定是否可以并发刷新
                    boolean hasUniqueIndex = checkUniqueIndexExists();
                    boolean canUseConcurrentRefresh = concurrentRefresh && hasUniqueIndex;
                    
                    if (concurrentRefresh && !hasUniqueIndex) {
                        log.warn("物化视图没有唯一索引，无法使用并发刷新，将使用普通刷新");
                    }
                    
                    if (canUseConcurrentRefresh) {
                        // 如果启用了并发刷新且存在唯一索引，使用CONCURRENTLY选项
                        log.info("使用并发方式刷新物化视图...");
                        jdbcTemplate.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY payment_reconciliation");
                        log.info("物化视图并发刷新完成");
                    } else {
                        // 使用普通刷新
                        log.info("使用普通方式刷新物化视图...");
                        jdbcTemplate.execute("REFRESH MATERIALIZED VIEW payment_reconciliation");
                        log.info("物化视图普通刷新完成");
                    }
                    
                    return; // 刷新完成后直接返回，不需要后续操作
                } catch (Exception e) {
                    log.error("刷新物化视图失败，错误信息: {}", e.getMessage(), e);
                    log.warn("物化视图刷新失败。如果物化视图不存在或结构有问题，请使用postgresql_reconciliation_schema.sql脚本重新创建");
                    return; // 不尝试重建物化视图，直接返回
                }
            } else {
                log.info("payment_reconciliation不是物化视图，继续使用常规方式刷新...");
            }

            // 使用DELETE替代TRUNCATE，更兼容
            try {
                log.info("尝试清空对账表中的旧数据...");
                jdbcTemplate.execute("DELETE FROM payment_reconciliation");
            } catch (Exception e) {
                log.warn("使用DELETE清空表失败，尝试其他方式: {}", e.getMessage());
                
                // 检查是否是视图无法删除的错误
                if (e.getMessage() != null && e.getMessage().contains("cannot delete from view")) {
                    log.info("payment_reconciliation是一个视图，无法直接DELETE。尝试使用TRUNCATE命令...");
                    try {
                        // 对于某些视图，可能可以使用TRUNCATE
                        jdbcTemplate.execute("TRUNCATE TABLE payment_reconciliation");
                        log.info("使用TRUNCATE清空视图成功");
                    } catch (Exception truncateEx) {
                        log.warn("使用TRUNCATE清空视图失败: {}", truncateEx.getMessage());
                        
                        // 如果是视图且无法删除，考虑创建INSTEAD OF DELETE触发器
                        log.info("尝试为视图创建INSTEAD OF DELETE触发器...");
                        try {
                            // 检查触发器是否已存在
                            String checkTriggerSql = 
                                "SELECT EXISTS (" +
                                "   SELECT FROM pg_trigger " +
                                "   WHERE tgname = 'payment_reconciliation_delete_trigger'" +
                                ")";
                            
                            Boolean triggerExists = jdbcTemplate.queryForObject(checkTriggerSql, Boolean.class);
                            
                            if (triggerExists == null || !triggerExists) {
                                // 创建触发器功能
                                String createFunctionSql = 
                                    "CREATE OR REPLACE FUNCTION payment_reconciliation_delete_func() " +
                                    "RETURNS TRIGGER AS $$ " +
                                    "BEGIN " +
                                    "    DELETE FROM payment_reconciliation_data WHERE order_no = OLD.order_no; " +
                                    "    RETURN OLD; " +
                                    "END; " +
                                    "$$ LANGUAGE plpgsql;";
                                
                                // 创建触发器
                                String createTriggerSql = 
                                    "CREATE TRIGGER payment_reconciliation_delete_trigger " +
                                    "INSTEAD OF DELETE ON payment_reconciliation " +
                                    "FOR EACH ROW " +
                                    "EXECUTE FUNCTION payment_reconciliation_delete_func();";
                                
                                // 执行创建
                                jdbcTemplate.execute(createFunctionSql);
                                jdbcTemplate.execute(createTriggerSql);
                                
                                log.info("视图触发器创建成功");
                                
                                // 再次尝试删除
                                jdbcTemplate.execute("DELETE FROM payment_reconciliation");
                                log.info("通过触发器清空视图成功");
                            }
                        } catch (Exception triggerEx) {
                            log.warn("创建视图触发器失败: {}", triggerEx.getMessage());
                            // 继续尝试其他方法
                        }
                    }
                }
                
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