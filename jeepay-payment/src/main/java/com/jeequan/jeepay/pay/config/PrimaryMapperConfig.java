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

import com.jeequan.jeepay.service.mapper.PayOrderMapper;
import com.jeequan.jeepay.service.mapper.PaymentCompensationRecordMapper;
import com.jeequan.jeepay.service.mapper.PaymentRecordMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * 配置主要的Mapper Bean为@Primary，解决多个相同类型bean冲突的问题
 * 
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/10/15
 */
@Configuration
public class PrimaryMapperConfig {
    
    @Autowired
    private PaymentRecordMapper paymentRecordMapper;
    
    @Autowired
    private PayOrderMapper payOrderMapper;
    
    @Autowired
    private PaymentCompensationRecordMapper paymentCompensationRecordMapper;
    
    /**
     * 将主数据源的PaymentRecordMapper设为@Primary，避免与reconciliationPaymentRecordMapper冲突
     */
    @Bean
    @Primary
    public PaymentRecordMapper primaryPaymentRecordMapper() {
        return paymentRecordMapper;
    }
    
    /**
     * 将主数据源的PayOrderMapper设为@Primary，避免与reconciliationPayOrderMapper冲突
     */
    @Bean
    @Primary
    public PayOrderMapper primaryPayOrderMapper() {
        return payOrderMapper;
    }
    
    /**
     * 将主数据源的PaymentCompensationRecordMapper设为@Primary，避免与reconciliationPaymentCompensationRecordMapper冲突
     */
    @Bean
    @Primary
    public PaymentCompensationRecordMapper primaryPaymentCompensationRecordMapper() {
        return paymentCompensationRecordMapper;
    }
} 