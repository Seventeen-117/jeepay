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
package com.jeequan.jeepay.pay.task;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * PostgreSQL物化视图刷新定时任务
 * 仅在使用PostgreSQL数据库时启用
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/9/15
 */
@Slf4j
@Configuration
@ConditionalOnProperty(name = "spring.datasource.dynamic.primary", havingValue = "postgres")
public class PostgreSQLRefreshTask {

    @Autowired
    private DataSource dataSource;
    
    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    @Value("${jeepay.task.postgres-refresh-enabled:true}")
    private boolean refreshEnabled;

    /**
     * 每小时刷新一次支付对账物化视图
     * 可通过配置禁用
     */
    @Scheduled(cron = "${jeepay.task.postgres-refresh-cron:0 0 * * * ?}")
    public void refreshPaymentReconciliationView() {
        if (!refreshEnabled) {
            return;
        }
        
        try {
            log.info("开始刷新PostgreSQL支付对账物化视图...");
            
            // 检查数据库是否为PostgreSQL
            if (!isPostgreSQL()) {
                log.debug("当前数据库不是PostgreSQL，跳过物化视图刷新");
                return;
            }
            
            // 执行刷新函数
            jdbcTemplate.execute("SELECT refresh_payment_reconciliation()");
            
            log.info("PostgreSQL支付对账物化视图刷新完成");
        } catch (Exception e) {
            log.error("刷新PostgreSQL物化视图失败", e);
        }
    }
    
    /**
     * 判断当前数据库是否为PostgreSQL
     */
    private boolean isPostgreSQL() {
        try (Connection conn = dataSource.getConnection()) {
            String dbProductName = conn.getMetaData().getDatabaseProductName().toLowerCase();
            return dbProductName.contains("postgresql");
        } catch (SQLException e) {
            log.error("获取数据库类型时出错", e);
            return false;
        }
    }
} 