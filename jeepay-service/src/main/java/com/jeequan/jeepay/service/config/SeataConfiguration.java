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
package com.jeequan.jeepay.service.config;

import io.seata.common.util.StringUtils;
import io.seata.saga.engine.StateMachineEngine;
import io.seata.saga.engine.config.DbStateMachineConfig;
import io.seata.saga.engine.impl.ProcessCtrlStateMachineEngine;
import io.seata.saga.statelang.domain.ExecutionStatus;
import io.seata.saga.statelang.domain.StateMachine;
import io.seata.saga.statelang.domain.StateMachineInstance;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import io.seata.spring.annotation.datasource.EnableAutoDataSourceProxy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

import io.seata.saga.engine.repo.impl.StateMachineRepositoryImpl;

/**
 * Seata 分布式事务配置
 * 
 * 包含：
 * 1. AT 模式配置 (Two-Phase Commit)
 * 2. Saga 模式配置 (长事务补偿)
 *
 * @author jiangyangpay
 * @since 2023/05/28
 */
@Slf4j
@Configuration
@EnableAutoDataSourceProxy // 启用 Seata 数据源自动代理
public class SeataConfiguration {

    /**
     * 配置 Saga 状态机引擎
     */
    @Bean
    @ConditionalOnMissingBean
    public StateMachineEngine stateMachineEngine(@Autowired DataSource dataSource, 
                                                @Autowired Environment environment) {
        log.info("初始化 Saga 状态机引擎...");
        
        // Saga 状态机配置
        DbStateMachineConfig stateMachineConfig = new DbStateMachineConfig();
        stateMachineConfig.setDataSource(dataSource);
        stateMachineConfig.setApplicationId(environment.getProperty("spring.application.name", "jeepay-service"));
        stateMachineConfig.setTxServiceGroup(environment.getProperty("seata.tx-service-group", "jeepay-tx-group"));
        stateMachineConfig.setTablePrefix("seata_");
        stateMachineConfig.setEnableAsync(true);
        
        // 显式设置状态机仓库
        StateMachineRepositoryImpl stateMachineRepository = new StateMachineRepositoryImpl();
        stateMachineConfig.setStateMachineRepository(stateMachineRepository);
        
        // 创建并配置状态机引擎
        ProcessCtrlStateMachineEngine engine = new ProcessCtrlStateMachineEngine();
        engine.setStateMachineConfig(stateMachineConfig);
        
        // 注意：不需要显式调用init()，Spring会负责初始化
        
        log.info("Saga 状态机引擎初始化完成，使用的数据源: {}, 应用ID: {}, 事务组: {}", 
                dataSource.getClass().getName(),
                stateMachineConfig.getApplicationId(),
                stateMachineConfig.getTxServiceGroup());
                
        return engine;
    }
} 