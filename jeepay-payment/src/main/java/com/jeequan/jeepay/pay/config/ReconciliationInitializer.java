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
package com.jeequan.jeepay.pay.config;

import com.jeequan.jeepay.pay.service.PostgreSQLReconciliationServiceAdapter;
import com.jeequan.jeepay.service.mapper.PaymentReconciliationMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import javax.sql.DataSource;
import jakarta.annotation.PostConstruct;
import java.util.concurrent.Executor;

/**
 * 支付对账初始化器
 * 根据数据库类型初始化相应的视图或物化视图
 * 同时配置对账服务适配器使用MyBatis-Plus或JdbcTemplate实现
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/10/15
 */
@Configuration
@Slf4j
@EnableScheduling
@ConditionalOnProperty(name = "jeepay.reconciliation.use-jpa", havingValue = "true", matchIfMissing = true)
public class ReconciliationInitializer implements SchedulingConfigurer {
    
    @Autowired
    @Qualifier("reconciliationDataSource")
    private DataSource reconciliationDataSource;
    
    @Autowired
    @Qualifier("reconciliationJdbcTemplate")
    private JdbcTemplate jdbcTemplate;
    
    @Autowired
    private PaymentReconciliationMapper paymentReconciliationMapper;
    
    @Autowired
    private PostgreSQLReconciliationServiceAdapter reconciliationServiceAdapter;
    
    @Value("${jeepay.reconciliation.use-mybatis-plus:true}")
    private boolean useMybatisPlus;
    
    @PostConstruct
    public void initialize() {
        try {
            // 确定数据库类型
            String dbType = determineDatabaseType();
            log.info("初始化支付对账系统，数据库类型: {}", dbType);
            
            // 根据数据库类型执行不同的清理和创建逻辑
            if ("PostgreSQL".equalsIgnoreCase(dbType)) {
                initializePostgreSQL();
            } else {
                initializeMySQL();
            }
            
            // 强制设置为使用MyBatis-Plus实现，不考虑配置参数
            reconciliationServiceAdapter.setUseNewImplementation(true);
            log.info("支付对账服务适配器初始化完成，强制使用MyBatis-Plus实现");
        } catch (Exception e) {
            log.error("初始化支付对账系统失败", e);
        }
    }
    
    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        // 使用线程池执行对账任务
        taskRegistrar.setScheduler(reconciliationTaskExecutor());
    }
    
    @Bean(destroyMethod = "shutdown")
    public ThreadPoolTaskScheduler reconciliationTaskExecutor() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(3);
        scheduler.setThreadNamePrefix("reconciliation-scheduler-");
        scheduler.setRemoveOnCancelPolicy(true);
        scheduler.setErrorHandler(throwable -> log.error("定时任务执行出错", throwable));
        scheduler.initialize();
        return scheduler;
    }
    
    /**
     * 对账视图刷新器
     * 提供手动触发对账视图刷新的能力
     */
    @Bean
    public ReconciliationViewRefresher reconciliationViewRefresher() {
        return new ReconciliationViewRefresher();
    }
    
    /**
     * 初始化PostgreSQL环境
     */
    private void initializePostgreSQL() {
        // PostgreSQL 环境初始化
        try {
            // 不再删除物化视图，由postgresql_reconciliation_schema.sql脚本管理
            log.info("物化视图由postgresql_reconciliation_schema.sql脚本管理，不在代码中删除或创建");
        } catch (Exception e) {
            log.error("PostgreSQL环境初始化失败", e);
        }
    }
    
    /**
     * 初始化MySQL环境
     */
    private void initializeMySQL() {
        // MySQL环境初始化
        try {
            // 不再删除视图，由SQL脚本管理
            log.info("视图由SQL脚本管理，不在代码中删除或创建");
        } catch (Exception e) {
            log.error("MySQL环境初始化失败", e);
        }
    }
    
    /**
     * 确定当前使用的数据库类型
     */
    private String determineDatabaseType() {
        try {
            String productName = jdbcTemplate.getDataSource().getConnection().getMetaData().getDatabaseProductName();
            return productName;
        } catch (Exception e) {
            log.error("无法确定数据库类型", e);
            return "Unknown";
        }
    }
    
    /**
     * 对账视图刷新器实现
     * 提供手动刷新视图和切换实现方式的功能
     */
    @Slf4j
    public static class ReconciliationViewRefresher {
        
        @Autowired
        private PostgreSQLReconciliationServiceAdapter reconciliationServiceAdapter;
        
        /**
         * 手动刷新对账视图
         */
        public void refreshView() {
            try {
                log.info("手动刷新支付对账视图...");
                reconciliationServiceAdapter.refreshReconciliationView();
                log.info("手动刷新支付对账视图完成");
            } catch (Exception e) {
                log.error("手动刷新支付对账视图失败: {}", e.getMessage(), e);
            }
        }
        
        /**
         * 设置使用MyBatis-Plus或JdbcTemplate实现
         * @param useMyBatisPlus 是否使用MyBatis-Plus实现
         */
        public void setUseMyBatisPlus(boolean useMyBatisPlus) {
            // 忽略参数，强制使用MyBatis-Plus实现
            reconciliationServiceAdapter.setUseNewImplementation(true);
            log.info("强制使用MyBatis-Plus实现，忽略传入参数");
        }
    }
} 