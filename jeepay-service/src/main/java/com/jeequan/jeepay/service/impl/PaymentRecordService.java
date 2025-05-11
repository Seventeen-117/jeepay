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
package com.jeequan.jeepay.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.jeequan.jeepay.core.entity.PaymentRecord;
import com.jeequan.jeepay.service.mapper.PaymentRecordMapper;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * <p>
 * 支付记录表 服务实现类
 * </p>
 *
 * @author jiangyangpay
 * @since 2023-08-12
 */
@Service
public class PaymentRecordService extends ServiceImpl<PaymentRecordMapper, PaymentRecord> {

    /**
     * 查询实际支付金额
     * @param orderNo 订单号
     * @return 实际支付金额，如果没有支付记录则返回null
     */
    public BigDecimal queryActualPaymentAmount(String orderNo) {
        PaymentRecord record = lambdaQuery()
                .eq(PaymentRecord::getOrderNo, orderNo)
                .select(PaymentRecord::getAmount)
                .one();
        
        return record != null ? record.getAmount() : null;
    }
} 