/*
 * Copyright (c) 2021-2031, 河北计全科技有限公司 (https://www.jeequan.com & jeequan@126.com).
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
package com.jeequan.jeepay.components.mq.vender.rocketmq;

import com.jeequan.jeepay.components.mq.constant.MQVenderCS;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.spring.autoconfigure.RocketMQProperties;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * RocketMQ Configuration
 * 
 * @author jeequan
 * @site https://www.jeequan.com
 */
@Configuration
@ConditionalOnProperty(name = MQVenderCS.YML_VENDER_KEY, havingValue = MQVenderCS.ROCKET_MQ)
@EnableConfigurationProperties(RocketMQProperties.class)
public class RocketMQConfiguration {

    @Autowired
    private RocketMQProperties rocketMQProperties;

    /**
     * 创建默认的RocketMQ生产者
     */
    @Bean
    @ConditionalOnMissingBean(DefaultMQProducer.class)
    public DefaultMQProducer defaultMQProducer() {
        DefaultMQProducer producer = new DefaultMQProducer();
        producer.setProducerGroup(rocketMQProperties.getProducer().getGroup());
        producer.setNamesrvAddr(rocketMQProperties.getNameServer());
        producer.setSendMsgTimeout(rocketMQProperties.getProducer().getSendMessageTimeout());
        producer.setRetryTimesWhenSendFailed(rocketMQProperties.getProducer().getRetryTimesWhenSendFailed());
        producer.setRetryTimesWhenSendAsyncFailed(rocketMQProperties.getProducer().getRetryTimesWhenSendAsyncFailed());
        producer.setMaxMessageSize(rocketMQProperties.getProducer().getMaxMessageSize());
        // 默认压缩消息的阈值设置为4K
        producer.setCompressMsgBodyOverHowmuch(4096);
        producer.setRetryAnotherBrokerWhenNotStoreOK(rocketMQProperties.getProducer().isRetryNextServer());
        return producer;
    }

    /**
     * 创建RocketMQ消息转换器
     */
    @Bean
    @ConditionalOnMissingBean(RocketMQMessageConverter.class)
    public RocketMQMessageConverter rocketMQMessageConverter() {
        return new RocketMQMessageConverter();
    }

    /**
     * 创建RocketMQTemplate
     */
    @Bean
    @ConditionalOnMissingBean(RocketMQTemplate.class)
    public RocketMQTemplate rocketMQTemplate(DefaultMQProducer defaultMQProducer, RocketMQMessageConverter rocketMQMessageConverter) {
        RocketMQTemplate rocketMQTemplate = new RocketMQTemplate();
        rocketMQTemplate.setProducer(defaultMQProducer);
        rocketMQTemplate.setMessageConverter(rocketMQMessageConverter.getMessageConverter());
        return rocketMQTemplate;
    }
} 