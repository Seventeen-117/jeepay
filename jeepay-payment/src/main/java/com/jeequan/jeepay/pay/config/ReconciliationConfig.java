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

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.spring.boot.autoconfigure.DruidDataSourceBuilder;
import com.baomidou.mybatisplus.annotation.DbType;
import com.baomidou.mybatisplus.core.config.GlobalConfig;
import com.baomidou.mybatisplus.extension.plugins.MybatisPlusInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.PaginationInnerInterceptor;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.jeequan.jeepay.service.mapper.PayOrderMapper;
import com.jeequan.jeepay.service.mapper.PaymentCompensationRecordMapper;
import com.jeequan.jeepay.service.mapper.PaymentRecordMapper;
import jakarta.persistence.EntityManagerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * 资金对账配置
 * 该配置允许在主数据源为MySQL的情况下，
 * 仍然使用PostgreSQL的物化视图进行对账操作
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/9/16
 */
@Configuration
@Slf4j
@EnableScheduling
public class ReconciliationConfig implements SchedulingConfigurer {

    /**
     * 创建PostgreSQL数据源，用于资金对账
     * 即使主数据源是MySQL，也会创建这个数据源
     */
    @Bean(name = "reconciliationDataSource")
    @ConfigurationProperties("spring.datasource.dynamic.datasource.postgres.druid")
    public DataSource reconciliationDataSource() {
        log.info("初始化资金对账PostgreSQL数据源...");
        DruidDataSource dataSource = DruidDataSourceBuilder.create().build();
        dataSource.setUrl("jdbc:postgresql://8.133.246.113:5432/jiangyangpay?currentSchema=public");
        dataSource.setUsername("postgres");
        dataSource.setPassword("Postgres@123456");
        dataSource.setDriverClassName("org.postgresql.Driver");
        
        // 配置PostgreSQL适用的验证查询 - 修复验证查询错误
        dataSource.setValidationQuery("SELECT 1");
        
        return dataSource;
    }

    /**
     * 为资金对账PostgreSQL数据源创建JdbcTemplate
     */
    @Bean(name = "reconciliationJdbcTemplate")
    public JdbcTemplate reconciliationJdbcTemplate(
            @Qualifier("reconciliationDataSource") DataSource dataSource) {
        log.info("初始化资金对账JdbcTemplate...");
        return new JdbcTemplate(dataSource);
    }

    /**
     * 为PostgreSQL数据源创建MyBatis-Plus的SqlSessionFactory
     */
    @Bean(name = "reconciliationSqlSessionFactory")
    @ConditionalOnProperty(name = "jeepay.sync.enabled", havingValue = "true", matchIfMissing = true)
    public SqlSessionFactory reconciliationSqlSessionFactory(
            @Qualifier("reconciliationDataSource") DataSource dataSource) throws Exception {
        log.info("初始化PostgreSQL SqlSessionFactory...");
        MybatisSqlSessionFactoryBean sqlSessionFactory = new MybatisSqlSessionFactoryBean();
        sqlSessionFactory.setDataSource(dataSource);
        
        // 配置类型别名
        sqlSessionFactory.setTypeAliasesPackage("com.jeequan.jeepay.core.entity");
        
        // 设置PostgreSQL方言
        GlobalConfig globalConfig = new GlobalConfig();
        
        // 修复 setDbType 方法不存在的问题 - 直接创建带有数据库类型的 DbConfig
        // 在 MyBatis-Plus 3.5.7 中，DbConfig 的创建方式有所变化
        GlobalConfig.DbConfig dbConfig = new GlobalConfig.DbConfig();
        // 不使用 setDbType 方法，而是在配置其他参数时，PostgreSQL 类型会被正确识别
        globalConfig.setDbConfig(dbConfig);
        sqlSessionFactory.setGlobalConfig(globalConfig);
        
        // 配置分页插件（这里显式指定了 PostgreSQL 数据库类型）
        MybatisPlusInterceptor interceptor = new MybatisPlusInterceptor();
        interceptor.addInnerInterceptor(new PaginationInnerInterceptor(DbType.POSTGRE_SQL));
        sqlSessionFactory.setPlugins(interceptor);
        
        return sqlSessionFactory.getObject();
    }

    /**
     * 为支付订单表提供PostgreSQL Mapper
     */
    @Bean(name = "reconciliationPayOrderMapper")
    @ConditionalOnProperty(name = "jeepay.sync.enabled", havingValue = "true", matchIfMissing = true)
    public PayOrderMapper reconciliationPayOrderMapper(
            @Qualifier("reconciliationSqlSessionFactory") SqlSessionFactory sqlSessionFactory) {
        
        log.info("初始化PostgreSQL支付订单Mapper...");
        SqlSession sqlSession = sqlSessionFactory.openSession();
        return sqlSession.getMapper(PayOrderMapper.class);
    }
    
    /**
     * 为支付记录表提供PostgreSQL Mapper
     */
    @Bean(name = "reconciliationPaymentRecordMapper")
    @ConditionalOnProperty(name = "jeepay.sync.enabled", havingValue = "true", matchIfMissing = true)
    public PaymentRecordMapper reconciliationPaymentRecordMapper(
            @Qualifier("reconciliationSqlSessionFactory") SqlSessionFactory sqlSessionFactory) {
        
        log.info("初始化PostgreSQL支付记录Mapper...");
        SqlSession sqlSession = sqlSessionFactory.openSession();
        return sqlSession.getMapper(PaymentRecordMapper.class);
    }

    /**
     * 为支付补偿记录表提供PostgreSQL Mapper
     */
    @Bean(name = "reconciliationPaymentCompensationRecordMapper")
    @ConditionalOnProperty(name = "jeepay.sync.enabled", havingValue = "true", matchIfMissing = true)
    public PaymentCompensationRecordMapper reconciliationPaymentCompensationRecordMapper(
            @Qualifier("reconciliationSqlSessionFactory") SqlSessionFactory sqlSessionFactory) {
        
        log.info("初始化PostgreSQL支付补偿记录Mapper...");
        SqlSession sqlSession = sqlSessionFactory.openSession();
        return sqlSession.getMapper(PaymentCompensationRecordMapper.class);
    }

    /**
     * 为PostgreSQL数据源创建EntityManagerFactory
     * 仅用于支付对账实体的映射
     */
    @Bean(name = "reconciliationEntityManagerFactory")
    @ConditionalOnProperty(name = "jeepay.reconciliation.use-jpa", havingValue = "true")
    public LocalContainerEntityManagerFactoryBean reconciliationEntityManagerFactory(
            @Qualifier("reconciliationDataSource") DataSource dataSource) {
        
        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
        em.setDataSource(dataSource);
        // 只扫描支付对账相关实体
        em.setPackagesToScan("com.jeequan.jeepay.pay.model");
        
        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        vendorAdapter.setGenerateDdl(false);
        vendorAdapter.setShowSql(false);
        em.setJpaVendorAdapter(vendorAdapter);
        
        // 配置JPA属性
        Map<String, Object> properties = new HashMap<>();
        properties.put("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        properties.put("hibernate.physical_naming_strategy", 
                "org.hibernate.boot.model.naming.PhysicalNamingStrategyStandardImpl");
        properties.put("hibernate.jdbc.batch_size", 100);
        properties.put("hibernate.order_inserts", true);
        properties.put("hibernate.order_updates", true);
        em.setJpaPropertyMap(properties);
        
        return em;
    }

    /**
     * 为PostgreSQL数据源创建事务管理器
     */
    @Bean(name = "reconciliationTransactionManager")
    @ConditionalOnProperty(name = "jeepay.reconciliation.use-jpa", havingValue = "true")
    public PlatformTransactionManager reconciliationTransactionManager(
            @Qualifier("reconciliationEntityManagerFactory") LocalContainerEntityManagerFactoryBean entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory.getObject());
    }

    /**
     * 刷新物化视图方法
     * 该Bean用于手动触发物化视图刷新
     */
    @Bean(name = "reconciliationConfigViewRefresher")
    public ReconciliationConfigViewRefresher reconciliationViewRefresher(
            @Qualifier("reconciliationJdbcTemplate") JdbcTemplate jdbcTemplate) {
        return new ReconciliationConfigViewRefresher(jdbcTemplate);
    }

    /**
     * 物化视图刷新器
     */
    public static class ReconciliationConfigViewRefresher {
        private final JdbcTemplate jdbcTemplate;

        public ReconciliationConfigViewRefresher(JdbcTemplate jdbcTemplate) {
            this.jdbcTemplate = jdbcTemplate;
        }

        /**
         * 刷新资金对账物化视图
         */
        public void refreshReconciliationView() {
            try {
                log.info("刷新支付对账物化视图...");
                
                // 首先检查物化视图是否存在
                if (materialized_view_exists()) {
                    // 如果存在，检查是否有唯一索引
                    boolean hasUniqueIndex = has_unique_index();
                    
                    if (hasUniqueIndex) {
                        // 如果有唯一索引，尝试并发刷新
                        try {
                            jdbcTemplate.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY payment_reconciliation");
                            log.info("支付对账物化视图并发刷新完成");
                        } catch (Exception e) {
                            log.warn("并发刷新失败，尝试普通刷新: {}", e.getMessage());
                            jdbcTemplate.execute("REFRESH MATERIALIZED VIEW payment_reconciliation");
                            log.info("支付对账物化视图普通刷新完成");
                        }
                    } else {
                        // 如果没有唯一索引，只能普通刷新
                        jdbcTemplate.execute("REFRESH MATERIALIZED VIEW payment_reconciliation");
                        log.info("支付对账物化视图普通刷新完成");
                    }
                } else {
                    // 如果不存在，先创建刷新函数
                    createRefreshFunctionIfNotExists();
                    
                    // 物化视图不存在，记录日志提示
                    log.warn("支付对账物化视图不存在，请使用SQL脚本创建");
                    
                    // 不再创建物化视图，因为现在由SQL脚本处理
                    log.info("物化视图由postgresql_reconciliation_schema.sql负责创建，不在代码中处理");
                }
            } catch (Exception e) {
                log.error("刷新物化视图失败", e);
                throw e; // 重新抛出异常以便调用者可以处理
            }
        }
        
        /**
         * 检查物化视图是否存在
         */
        private boolean materialized_view_exists() {
            try {
                String checkViewSql = 
                    "SELECT EXISTS (" +
                    "   SELECT FROM pg_catalog.pg_matviews " +
                    "   WHERE matviewname = 'payment_reconciliation'" +
                    ");";
                
                Boolean viewExists = jdbcTemplate.queryForObject(checkViewSql, Boolean.class);
                return viewExists != null && viewExists;
            } catch (Exception e) {
                log.error("检查物化视图是否存在时出错", e);
                return false;
            }
        }
        
        /**
         * 检查物化视图是否有唯一索引
         */
        private boolean has_unique_index() {
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
         * 如果刷新函数不存在则创建
         */
        private void createRefreshFunctionIfNotExists() {
            try {
                // 检查函数是否存在
                String checkFunctionSql = 
                    "SELECT EXISTS (" +
                    "   SELECT FROM pg_proc " +
                    "   WHERE proname = 'refresh_payment_reconciliation'" +
                    ");";
                
                Boolean functionExists = jdbcTemplate.queryForObject(checkFunctionSql, Boolean.class);
                
                if (functionExists == null || !functionExists) {
                    log.info("刷新函数不存在，正在创建...");
                    
                    // 创建函数
                    String createFunctionSql = 
                        "CREATE OR REPLACE FUNCTION refresh_payment_reconciliation() " +
                        "RETURNS VOID AS $$ " +
                        "BEGIN " +
                        "    REFRESH MATERIALIZED VIEW CONCURRENTLY payment_reconciliation; " +
                        "END; " +
                        "$$ LANGUAGE plpgsql;";
                    
                    jdbcTemplate.execute(createFunctionSql);
                    
                    // 添加函数注释
                    String commentSql = 
                        "COMMENT ON FUNCTION refresh_payment_reconciliation() " +
                        "IS '刷新支付对账物化视图的函数';";
                    
                    jdbcTemplate.execute(commentSql);
                    
                    log.info("刷新函数创建完成");
                }
            } catch (Exception e) {
                log.error("创建刷新函数时出错", e);
                throw e;
            }
        }
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        // 使用线程池执行对账任务
        taskRegistrar.setScheduler(taskExecutor());
    }
    
    @Bean(destroyMethod = "shutdown")
    public Executor taskExecutor() {
        return Executors.newScheduledThreadPool(2);
    }
} 