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
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Spring Batch 数据源配置
 * Spring Batch需要一个名为'dataSource'的Bean作为JobRepository的数据源
 * 由于项目中使用了多数据源，这里显式指定主数据源作为Spring Batch的数据源
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2025/5/12
 */
@Configuration
@Slf4j
public class BatchDataSourceConfig {

    @Value("${spring.datasource.dynamic.datasource.mysql.url}")
    private String jdbcUrl;
    
    @Value("${spring.datasource.dynamic.datasource.mysql.username}")
    private String username;
    
    @Value("${spring.datasource.dynamic.datasource.mysql.password}")
    private String password;
    
    @Value("${spring.datasource.dynamic.datasource.mysql.driver-class-name}")
    private String driverClassName;

    /**
     * Spring Batch需要一个名为'dataSource'的Bean作为JobRepository的数据源
     * 由于我们使用了reconciliationDataSource作为PostgreSQL的数据源，
     * 这里手动创建一个名为'dataSource'的Bean，并标记为@Primary，
     * 使Spring Batch自动使用这个数据源
     */
    @Bean(name = "dataSource")
    @Primary
    public DataSource primaryDataSource() {
        log.info("配置Spring Batch主数据源...");
        
        // 创建一个专用于Spring Batch的数据源
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setDriverClassName(driverClassName);
        
        // MySQL验证查询
        dataSource.setValidationQuery("SELECT 1 FROM DUAL");
        dataSource.setTestWhileIdle(true);
        dataSource.setTestOnBorrow(false);
        dataSource.setTestOnReturn(false);
        
        return dataSource;
    }
    
    /**
     * 为Spring Batch创建一个事务管理器
     * 该事务管理器使用主数据源
     */
    @Bean(name = "transactionManager")
    @Primary
    public PlatformTransactionManager batchTransactionManager(@Qualifier("dataSource") DataSource dataSource) {
        log.info("配置Spring Batch事务管理器...");
        return new DataSourceTransactionManager(dataSource);
    }
} 