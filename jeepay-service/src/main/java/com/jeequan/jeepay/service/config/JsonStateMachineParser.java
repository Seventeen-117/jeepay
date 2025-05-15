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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.seata.saga.statelang.domain.StateMachine;
import io.seata.saga.statelang.domain.impl.StateMachineImpl;
import io.seata.saga.statelang.parser.StateMachineParser;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * 自定义Saga状态机JSON解析器
 * 适用于Seata 2.0.0版本
 * 
 * @author jiangyangpay
 * @since 2023/05/28
 */
@Slf4j
@Component
public class JsonStateMachineParser implements StateMachineParser {
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public StateMachine parse(String json) {
        return parse(json, null);
    }
    
    /**
     * 解析状态机定义，可提供文件名作为备用名称
     * 
     * @param json 状态机JSON定义
     * @param defaultName 默认名称（通常为文件名）
     * @return 状态机对象
     */
    public StateMachine parse(String json, String defaultName) {
        if (json == null || json.trim().isEmpty()) {
            throw new IllegalArgumentException("状态机JSON定义不能为空");
        }
        
        try {
            // 解析JSON字符串为JsonNode
            JsonNode rootNode = objectMapper.readTree(json);
            
            // 构建基本的StateMachine对象
            StateMachineImpl stateMachine = new StateMachineImpl();
            
            // 设置基本属性
            String name = null;
            if (rootNode.has("Name") && !rootNode.get("Name").isNull()) {
                name = rootNode.get("Name").asText();
            }
            
            // 如果JSON中没有Name属性或为空，则使用默认名称
            if (!StringUtils.hasText(name) && StringUtils.hasText(defaultName)) {
                name = defaultName;
                log.info("状态机定义中未指定Name，使用文件名: {}", name);
            }
            
            // 确保名称非空
            if (!StringUtils.hasText(name)) {
                name = "unknown_state_machine_" + System.currentTimeMillis();
                log.warn("状态机没有名称，使用生成的名称: {}", name);
            }
            
            stateMachine.setName(name);
            
            // 设置其他属性（检查节点是否存在且非空）
            if (rootNode.has("Comment") && !rootNode.get("Comment").isNull()) {
                stateMachine.setComment(rootNode.get("Comment").asText());
            } else {
                stateMachine.setComment("State machine: " + name);
            }
            
            if (rootNode.has("Version") && !rootNode.get("Version").isNull()) {
                stateMachine.setVersion(rootNode.get("Version").asText());
            } else {
                stateMachine.setVersion("1.0.0");
            }
            
            if (rootNode.has("StartState") && !rootNode.get("StartState").isNull()) {
                stateMachine.setStartState(rootNode.get("StartState").asText());
            } else {
                // StartState是必需的
                log.error("状态机缺少StartState属性: {}", name);
                throw new IllegalArgumentException("状态机定义缺少必需的StartState属性");
            }
            
            // 将原始JSON字符串保存到stateMachine对象
            stateMachine.setContent(json);
            
            log.debug("成功解析状态机: {}, 开始状态: {}", name, stateMachine.getStartState());
            return stateMachine;
        } catch (Exception e) {
            log.error("解析状态机定义失败: {}", e.getMessage(), e);
            throw new RuntimeException("解析状态机定义失败: " + e.getMessage(), e);
        }
    }
} 