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

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Redisson配置类 - 提供分布式锁支持
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/8/12
 */
@Configuration
public class RedissonConfig {

    @Value("${spring.data.redis.host}")
    private String host;

    @Value("${spring.data.redis.port}")
    private int port;

    @Value("${spring.data.redis.password}")
    private String password;

    @Value("${spring.data.redis.database}")
    private int database;

    @Bean
    public RedissonClient redissonClient() {
        Config config = new Config();
        
        // 单节点Redis配置
        config.useSingleServer()
              .setAddress("redis://" + host + ":" + port)
              .setDatabase(database);
        
        // 如果设置了密码，则添加密码
        if (password != null && !password.isEmpty()) {
            config.useSingleServer().setPassword(password);
        }
        
        // 创建RedissonClient实例
        return Redisson.create(config);
    }
} 