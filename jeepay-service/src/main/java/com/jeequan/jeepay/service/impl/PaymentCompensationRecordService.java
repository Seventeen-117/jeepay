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
import com.jeequan.jeepay.core.entity.PaymentCompensationRecord;
import com.jeequan.jeepay.service.mapper.PaymentCompensationRecordMapper;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Date;

/**
 * <p>
 * 支付补偿记录表 服务实现类
 * </p>
 *
 * @author jiangyangpay
 * @since 2023-08-12
 */
@Service
public class PaymentCompensationRecordService extends ServiceImpl<PaymentCompensationRecordMapper, PaymentCompensationRecord> {

    /**
     * 创建支付补偿记录
     * @param orderNo 订单号
     * @param primaryChannel 主支付渠道
     * @param backupIfCode 备用支付渠道
     * @param amount 金额
     * @return 是否创建成功
     */
    public boolean createCompensationRecord(String orderNo, String primaryChannel, String backupIfCode, BigDecimal amount) {
        PaymentCompensationRecord record = new PaymentCompensationRecord();
        record.setOrderNo(orderNo);
        record.setPrimaryChannel(primaryChannel);
        record.setBackupIfCode(backupIfCode);
        record.setAmount(amount);
        record.setCreateTime(new Date());
        
        return this.save(record);
    }
    
    /**
     * 根据订单号查询支付补偿记录
     * @param orderNo 订单号
     * @return 补偿记录
     */
    public PaymentCompensationRecord getByOrderNo(String orderNo) {
        return this.getOne(PaymentCompensationRecord.gw().eq(PaymentCompensationRecord::getOrderNo, orderNo));
    }
} 