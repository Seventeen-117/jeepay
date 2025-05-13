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

import com.jeequan.jeepay.core.model.ApiRes;
import com.jeequan.jeepay.pay.config.DatabaseSchemaInitializer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据库表结构管理接口
 * 提供手动初始化和检查数据库表的功能
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/10/21
 */
@Slf4j
@RestController
@RequestMapping("/api/schema")
public class SchemaController {

    @Autowired
    private DatabaseSchemaInitializer schemaInitializer;
    
    @Autowired
    private DataSource dataSource;

    /**
     * 手动触发数据库表初始化
     */
    @GetMapping("/init")
    public ApiRes initSchema() {
        try {
            log.info("手动触发数据库表初始化");
            schemaInitializer.init();
            log.info("手动初始化数据库表完成");
            return ApiRes.ok("初始化成功");
        } catch (Exception e) {
            log.error("初始化数据库表失败", e);
            return ApiRes.customFail("初始化失败：" + e.getMessage());
        }
    }
    
    /**
     * 检查关键数据库表是否存在
     */
    @GetMapping("/check")
    public ApiRes checkSchema() {
        Map<String, Boolean> tableStatus = new HashMap<>();
        String[] tableNames = {"t_pay_order", "t_refund_order", "payment_records", "payment_reconciliation", "t_pay_order_compensation"};
        
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            // 获取数据库类型
            String dbType = conn.getMetaData().getDatabaseProductName();
            
            for (String tableName : tableNames) {
                try {
                    String checkSql;
                    if (dbType.contains("PostgreSQL")) {
                        checkSql = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = '" + tableName + "')";
                        ResultSet rs = stmt.executeQuery(checkSql);
                        if (rs.next()) {
                            tableStatus.put(tableName, rs.getBoolean(1));
                        } else {
                            tableStatus.put(tableName, false);
                        }
                        rs.close();
                    } else {
                        // MySQL或其他数据库
                        checkSql = "SHOW TABLES LIKE '" + tableName + "'";
                        ResultSet rs = stmt.executeQuery(checkSql);
                        tableStatus.put(tableName, rs.next());
                        rs.close();
                    }
                } catch (SQLException e) {
                    log.error("检查表 {} 失败: {}", tableName, e.getMessage());
                    tableStatus.put(tableName, false);
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
                        if (rs.next()) {
                            matViewExists = rs.getBoolean(1);
                        }
                        rs.close();
                    }
                    
                    tableStatus.put("payment_reconciliation_view", matViewExists);
                } catch (SQLException e) {
                    log.error("检查物化视图失败: {}", e.getMessage());
                    tableStatus.put("payment_reconciliation_view", false);
                }
            }
            
            return ApiRes.ok(tableStatus);
        } catch (SQLException e) {
            log.error("检查数据库表失败", e);
            return ApiRes.customFail("检查失败：" + e.getMessage());
        }
    }
} 