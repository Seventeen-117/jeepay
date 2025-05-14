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
package com.jeequan.jeepay.pay.controller.api;

import com.jeequan.jeepay.core.aop.MethodLog;
import com.jeequan.jeepay.core.constants.ApiCodeEnum;
import com.jeequan.jeepay.core.model.ApiRes;
import com.jeequan.jeepay.pay.config.DatabaseSchemaInitializer;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据库Schema管理接口
 * 用于检查和初始化数据库表结构
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/10/21
 */
@Slf4j
@RestController
@RequestMapping("/api/schema")
@Tag(name = "数据库管理", description = "检查和初始化数据库表结构")
public class SchemaController {

    @Autowired
    private DatabaseSchemaInitializer schemaInitializer;
    
    @Autowired
    private DataSource dataSource;
    
    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    @Value("${spring.datasource.dynamic.primary:mysql}")
    private String primaryDataSource;
    
    @Value("${jeepay.reconciliation.skip-sql-execution:false}")
    private boolean skipSqlExecution;

    /**
     * 检查数据库类型
     */
    @GetMapping("/check-db-type")
    @Operation(summary = "检查数据库类型")
    @MethodLog(remark = "检查数据库类型")
    public ApiRes checkDatabaseType() {
        try {
            // 获取数据库类型
            String dbType = jdbcTemplate.queryForObject("SELECT version()", String.class);
            boolean isPostgreSQL = dbType != null && dbType.toLowerCase().contains("postgresql");
            
            Map<String, Object> result = new HashMap<>();
            result.put("dbType", isPostgreSQL ? "PostgreSQL" : "MySQL");
            result.put("version", dbType);
            result.put("primaryDataSource", primaryDataSource);
            result.put("skipSqlExecution", skipSqlExecution);
            
            return ApiRes.ok(result);
        } catch (Exception e) {
            log.error("检查数据库类型失败", e);
            return ApiRes.fail(ApiCodeEnum.SYSTEM_ERROR, "检查数据库类型失败: " + e.getMessage());
        }
    }

    /**
     * 手动触发数据库表初始化
     */
    @GetMapping("/init")
    @Operation(summary = "手动初始化数据库表结构")
    @MethodLog(remark = "手动初始化数据库表")
    public ApiRes initSchema() {
        try {
            log.info("手动触发数据库表初始化");
            // 使用新方法，确保能够正确处理已手动执行过的SQL脚本
            schemaInitializer.initializeSchema();
            log.info("手动初始化数据库表完成");
            return ApiRes.ok("初始化成功");
        } catch (Exception e) {
            log.error("初始化数据库表失败", e);
            return ApiRes.customFail("初始化失败：" + e.getMessage());
        }
    }
    
    /**
     * 检查数据库表结构
     */
    @GetMapping("/check-tables")
    @Operation(summary = "检查必要的数据库表结构")
    @MethodLog(remark = "检查数据库表结构")
    public ApiRes checkTables() {
        try {
            List<Map<String, Object>> tables = new ArrayList<>();
            
            // 检查必要的表
            String[] requiredTables = {
                "payment_records", 
                "payment_compensation_records", 
                "t_pay_order", 
                "t_refund_order", 
                "t_pay_order_division_record",
                "t_pay_order_compensation",
                "t_transfer_order",
                "payment_channel_metrics"
            };
            
            for (String tableName : requiredTables) {
                Map<String, Object> tableInfo = new HashMap<>();
                tableInfo.put("tableName", tableName);
                
                try {
                    // 检查表是否存在
                    boolean exists = checkTableExists(tableName);
                    tableInfo.put("exists", exists);
                    
                    if (exists) {
                        // 如果表存在，获取行数
                        Long rowCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + tableName, Long.class);
                        tableInfo.put("rowCount", rowCount);
                    }
                } catch (Exception e) {
                    tableInfo.put("error", e.getMessage());
                }
                
                tables.add(tableInfo);
            }
            
            // 检查物化视图
            Map<String, Object> matviewInfo = new HashMap<>();
            matviewInfo.put("tableName", "payment_reconciliation (materialized view)");
            
            try {
                boolean matviewExists = checkMaterializedViewExists();
                matviewInfo.put("exists", matviewExists);
                
                if (matviewExists) {
                    Long rowCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM payment_reconciliation", Long.class);
                    matviewInfo.put("rowCount", rowCount);
                } else {
                    // 检查是否为普通表或普通视图
                    try {
                        boolean tableExists = checkTableExists("payment_reconciliation");
                        if (tableExists) {
                            matviewInfo.put("note", "存在作为普通表或普通视图，而非物化视图");
                            Long rowCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM payment_reconciliation", Long.class);
                            matviewInfo.put("rowCount", rowCount);
                        }
                    } catch (Exception e) {
                        matviewInfo.put("note", "检查普通表失败: " + e.getMessage());
                    }
                }
            } catch (Exception e) {
                matviewInfo.put("error", e.getMessage());
            }
            
            tables.add(matviewInfo);
            
            return ApiRes.ok(tables);
        } catch (Exception e) {
            log.error("检查数据库表失败", e);
            return ApiRes.fail(ApiCodeEnum.SYSTEM_ERROR, "检查数据库表失败: " + e.getMessage());
        }
    }
    
    /**
     * 检查关键数据库表是否存在（原有方法保留，与新方法合并）
     */
    @GetMapping("/check")
    @Operation(summary = "检查关键数据库表")
    @MethodLog(remark = "检查关键数据库表")
    public ApiRes checkSchema() {
        Map<String, Object> tableStatus = new HashMap<>();
        String[] tableNames = {"t_pay_order", "t_refund_order", "payment_records", "payment_reconciliation", "t_pay_order_compensation", "t_transfer_order"};
        
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            // 获取数据库类型
            String dbType = conn.getMetaData().getDatabaseProductName();
            tableStatus.put("databaseType", dbType);
            tableStatus.put("skipSqlExecution", skipSqlExecution);
            
            for (String tableName : tableNames) {
                try {
                    String checkSql;
                    if (dbType.contains("PostgreSQL")) {
                        checkSql = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = '" + tableName + "')";
                        ResultSet rs = stmt.executeQuery(checkSql);
                        boolean exists = false;
                        if (rs.next()) {
                            exists = rs.getBoolean(1);
                        }
                        rs.close();
                        
                        tableStatus.put(tableName, exists);
                        
                        // 如果表存在，获取行数
                        if (exists) {
                            try {
                                ResultSet countRs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
                                if (countRs.next()) {
                                    tableStatus.put(tableName + "_rows", countRs.getLong(1));
                                }
                                countRs.close();
                            } catch (Exception e) {
                                tableStatus.put(tableName + "_rows", "错误: " + e.getMessage());
                            }
                        }
                    } else {
                        // MySQL或其他数据库
                        checkSql = "SHOW TABLES LIKE '" + tableName + "'";
                        ResultSet rs = stmt.executeQuery(checkSql);
                        boolean exists = rs.next();
                        tableStatus.put(tableName, exists);
                        rs.close();
                        
                        // 如果表存在，获取行数
                        if (exists) {
                            try {
                                ResultSet countRs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
                                if (countRs.next()) {
                                    tableStatus.put(tableName + "_rows", countRs.getLong(1));
                                }
                                countRs.close();
                            } catch (Exception e) {
                                tableStatus.put(tableName + "_rows", "错误: " + e.getMessage());
                            }
                        }
                    }
                } catch (SQLException e) {
                    log.error("检查表 {} 失败: {}", tableName, e.getMessage());
                    tableStatus.put(tableName, false);
                    tableStatus.put(tableName + "_error", e.getMessage());
                }
            }
            
            // 检查PostgreSQL中的视图或物化视图
            if (dbType.contains("PostgreSQL")) {
                try {
                    String checkViewSql = "SELECT EXISTS (SELECT FROM pg_matviews WHERE matviewname = 'payment_reconciliation')";
                    ResultSet rs = stmt.executeQuery(checkViewSql);
                    boolean matViewExists = false;
                    if (rs.next()) {
                        matViewExists = rs.getBoolean(1);
                    }
                    rs.close();
                    
                    if (!matViewExists) {
                        // 检查普通视图
                        String checkNormalViewSql = "SELECT EXISTS (SELECT FROM pg_views WHERE viewname = 'payment_reconciliation')";
                        rs = stmt.executeQuery(checkNormalViewSql);
                        boolean normalViewExists = false;
                        if (rs.next()) {
                            normalViewExists = rs.getBoolean(1);
                        }
                        rs.close();
                        
                        tableStatus.put("payment_reconciliation_normal_view", normalViewExists);
                    }
                    
                    tableStatus.put("payment_reconciliation_mat_view", matViewExists);
                    
                    // 检查刷新函数
                    String checkFunctionSql = "SELECT EXISTS (SELECT FROM pg_proc WHERE proname = 'refresh_payment_reconciliation')";
                    rs = stmt.executeQuery(checkFunctionSql);
                    boolean functionExists = false;
                    if (rs.next()) {
                        functionExists = rs.getBoolean(1);
                    }
                    rs.close();
                    
                    tableStatus.put("refresh_function_exists", functionExists);
                } catch (SQLException e) {
                    log.error("检查物化视图失败: {}", e.getMessage());
                    tableStatus.put("payment_reconciliation_view_error", e.getMessage());
                }
            }
            
            return ApiRes.ok(tableStatus);
        } catch (SQLException e) {
            log.error("检查数据库表失败", e);
            return ApiRes.customFail("检查失败：" + e.getMessage());
        }
    }
    
    /**
     * 检查表是否存在
     */
    private boolean checkTableExists(String tableName) {
        try {
            String sql = "SELECT EXISTS (" +
                         "   SELECT FROM pg_catalog.pg_tables " +
                         "   WHERE schemaname = 'public' " +
                         "   AND tablename = ?" +
                         ");";
            
            Boolean exists = jdbcTemplate.queryForObject(sql, Boolean.class, tableName);
            return exists != null && exists;
        } catch (Exception e) {
            // 尝试MySQL方式检查
            try {
                String mysqlSql = "SELECT COUNT(*) FROM information_schema.tables " +
                                  "WHERE table_schema = DATABASE() AND table_name = ?";
                Integer count = jdbcTemplate.queryForObject(mysqlSql, Integer.class, tableName);
                return count != null && count > 0;
            } catch (Exception ex) {
                log.error("检查表是否存在时出错: {}", ex.getMessage());
                return false;
            }
        }
    }
    
    /**
     * 检查物化视图是否存在
     */
    private boolean checkMaterializedViewExists() {
        try {
            String sql = "SELECT EXISTS (" +
                         "   SELECT FROM pg_catalog.pg_matviews " +
                         "   WHERE matviewname = 'payment_reconciliation'" +
                         ");";
            
            Boolean exists = jdbcTemplate.queryForObject(sql, Boolean.class);
            return exists != null && exists;
        } catch (Exception e) {
            log.debug("检查物化视图失败（可能是MySQL环境）: {}", e.getMessage());
            return false;
        }
    }
} 