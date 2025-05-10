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
package com.jeequan.jeepay.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.jeequan.jeepay.core.entity.PayOrderCompensation;
import com.jeequan.jeepay.service.mapper.PayOrderCompensationMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Date;

/**
 * 支付订单补偿记录服务实现类
 *
 * @author jeepay
 * @site https://www.jeequan.com
 * @date 2023/8/12
 */
@Service
@Slf4j
public class PayOrderCompensationService extends ServiceImpl<PayOrderCompensationMapper, PayOrderCompensation> {

    /**
     * 创建支付补偿记录
     */
    public boolean createCompensation(String payOrderId, String mchNo, String appId, 
                                   String originalIfCode, String compensationIfCode, Long amount) {
        
        PayOrderCompensation compensation = new PayOrderCompensation();
        compensation.setPayOrderId(payOrderId);
        compensation.setMchNo(mchNo);
        compensation.setAppId(appId);
        compensation.setOriginalIfCode(originalIfCode);
        compensation.setCompensationIfCode(compensationIfCode);
        compensation.setAmount(amount);
        compensation.setState((byte)0); // 处理中
        compensation.setCreatedAt(new Date());
        
        return this.save(compensation);
    }
    
    /**
     * 更新补偿记录状态
     */
    public boolean updateCompensationState(Long compensationId, byte state, String resultInfo) {
        PayOrderCompensation compensation = new PayOrderCompensation();
        compensation.setCompensationId(compensationId);
        compensation.setState(state);
        compensation.setResultInfo(resultInfo);
        compensation.setUpdatedAt(new Date());
        
        return this.updateById(compensation);
    }
    
    /**
     * 根据支付订单号查询补偿记录
     */
    public PayOrderCompensation getByPayOrderId(String payOrderId) {
        return this.getOne(PayOrderCompensation.gw().eq(PayOrderCompensation::getPayOrderId, payOrderId));
    }
} 