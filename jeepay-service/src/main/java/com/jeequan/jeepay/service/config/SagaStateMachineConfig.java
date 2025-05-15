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

import io.seata.saga.engine.StateMachineEngine;
import io.seata.saga.statelang.domain.ExecutionStatus;
import io.seata.saga.statelang.domain.StateMachine;
import io.seata.saga.statelang.domain.StateMachineInstance;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * 自动加载Saga状态机定义配置
 * 
 * @author jiangyangpay
 * @since 2023/05/28
 */
@Slf4j
@Component
@Order(Ordered.LOWEST_PRECEDENCE) // 确保在其他组件之后运行
public class SagaStateMachineConfig implements ApplicationRunner {

    @Autowired
    private StateMachineEngine stateMachineEngine;
    
    @Autowired
    private JsonStateMachineParser jsonStateMachineParser;
    
    // 记录已加载成功的状态机数量
    private int successfullyLoadedCount = 0;
    
    @Override
    public void run(ApplicationArguments args) {
        try {
            log.info("ApplicationRunner开始初始化Saga状态机...");
            
            // 添加延迟确保StateMachineEngine完全初始化
            try {
                log.info("等待3秒确保StateMachineEngine完全初始化...");
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            loadStateMachineDefinitions();
        } catch (Exception e) {
            log.error("加载状态机定义过程中发生错误", e);
            // 不抛出异常，允许应用继续启动
        }
    }
    
    /**
     * 加载状态机定义
     */
    private void loadStateMachineDefinitions() {
        try {
            log.info("开始加载Saga状态机定义...");
            
            ResourcePatternResolver resourcePatternResolver = new PathMatchingResourcePatternResolver();
            Resource[] resources = resourcePatternResolver.getResources("classpath:statelang/*.json");
            
            if (resources.length == 0) {
                log.warn("未找到任何状态机定义文件");
                return;
            }
            
            log.info("找到{}个状态机定义文件，开始逐个加载...", resources.length);
            for (Resource resource : resources) {
                processStateMachineResource(resource);
            }
            
            log.info("Saga状态机定义加载完成，共{}个定义文件，成功加载{}个状态机", 
                    resources.length, successfullyLoadedCount);
            
            if (successfullyLoadedCount == 0) {
                log.warn("没有成功加载任何状态机，请检查状态机定义和配置是否正确");
            }
        } catch (FileNotFoundException e) {
            log.warn("未找到状态机定义文件: {}", e.getMessage());
        } catch (IOException e) {
            log.error("加载状态机定义资源时发生IO错误", e);
        } catch (Exception e) {
            log.error("加载Saga状态机定义失败", e);
        }
    }
    
    /**
     * 处理单个状态机资源文件
     */
    private void processStateMachineResource(Resource resource) {
        String stateMachineName = null;
        try {
            stateMachineName = resource.getFilename();
            if (stateMachineName == null) {
                log.warn("状态机文件名为空，跳过");
                return;
            }
            
            stateMachineName = stateMachineName.replace(".json", "");
            log.info("正在加载状态机定义: {}", stateMachineName);
            
            // 读取JSON内容
            String json = readResourceContent(resource);
            if (json == null || json.trim().isEmpty()) {
                log.warn("状态机定义文件为空: {}", stateMachineName);
                return;
            }
            
            // 解析状态机定义，使用文件名作为备用名称
            StateMachine stateMachine = parseStateMachine(json, stateMachineName);
            if (stateMachine == null) {
                log.error("状态机解析失败: {}", stateMachineName);
                return;
            }
            
            log.info("状态机[{}]解析成功，名称: {}, 版本: {}, 开始状态: {}", 
                    stateMachineName, stateMachine.getName(), stateMachine.getVersion(), stateMachine.getStartState());
            
            // 注册状态机
            registerStateMachine(stateMachine, stateMachineName);
            
        } catch (Exception e) {
            log.error("处理状态机定义失败: {}", stateMachineName, e);
        }
    }
    
    /**
     * 读取资源内容
     */
    private String readResourceContent(Resource resource) throws IOException {
        try (Reader reader = new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8)) {
            return FileCopyUtils.copyToString(reader);
        }
    }
    
    /**
     * 解析状态机定义
     * 
     * @param json 状态机JSON内容
     * @param stateMachineName 文件名（作为备用名称）
     * @return 状态机对象
     */
    private StateMachine parseStateMachine(String json, String stateMachineName) {
        try {
            // 使用文件名作为备用名称，如果JSON中未指定Name属性
            StateMachine stateMachine = jsonStateMachineParser.parse(json, stateMachineName);
            if (stateMachine == null) {
                log.error("状态机解析结果为空: {}", stateMachineName);
                return null;
            }
            return stateMachine;
        } catch (Exception e) {
            log.error("状态机定义解析失败: {}", stateMachineName, e);
            return null;
        }
    }
    
    /**
     * 注册状态机到引擎
     */
    private void registerStateMachine(StateMachine stateMachine, String stateMachineName) {
        try {
            log.info("开始注册状态机: {}", stateMachineName);
            
            // 方法1：直接使用StateMachine对象注册（不启动实例）
            try {
                log.info("尝试直接注册状态机定义: {}", stateMachineName);
                stateMachineEngine.getStateMachineConfig().getStateMachineRepository().registryStateMachine(stateMachine);
                log.info("状态机[{}]直接注册成功", stateMachineName);
                successfullyLoadedCount++;
                return; // 如果成功直接返回
            } catch (Exception e) {
                log.warn("直接注册状态机失败，尝试使用startWithBusinessKey方法: {}", e.getMessage());
            }
            
            // 方法2：使用引擎的startWithBusinessKey方法间接注册状态机
            Map<String, Object> startParams = new HashMap<>();
            String businessKey = "registration_" + stateMachineName + "_" + System.currentTimeMillis();
            
            log.info("调用stateMachineEngine.startWithBusinessKey: name={}, tenant=null, businessKey={}", 
                    stateMachine.getName(), businessKey);
                    
            StateMachineInstance instance = stateMachineEngine.startWithBusinessKey(
                    stateMachine.getName(), 
                    null, // tenantId 
                    businessKey, 
                    startParams
            );
            
            if (instance != null) {
                log.info("状态机实例创建成功 - ID: {}, 状态: {}", 
                        instance.getId(), instance.getStatus());
                        
                if (ExecutionStatus.SU.equals(instance.getStatus())) {
                    log.info("状态机定义[{}]加载成功", stateMachineName);
                    successfullyLoadedCount++;
                } else {
                    log.warn("状态机定义[{}]加载异常，实例状态: {}, 异常信息: {}", 
                            stateMachineName, instance.getStatus(), instance.getException());
                }
            } else {
                log.error("状态机实例创建失败，返回为null");
            }
        } catch (Exception e) {
            log.error("注册状态机到引擎失败: {} - {}", stateMachineName, e.getMessage(), e);
        }
    }
} 