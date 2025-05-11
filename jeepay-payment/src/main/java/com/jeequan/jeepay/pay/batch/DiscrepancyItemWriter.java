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
package com.jeequan.jeepay.pay.batch;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.jeequan.jeepay.core.entity.PaymentReconciliation;
import com.jeequan.jeepay.pay.model.Discrepancy;
import com.jeequan.jeepay.service.impl.PaymentReconciliationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;

/**
 * 支付差异写入器
 * 用于批处理中将检测到的差异写入数据库
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/8/12
 */
@Component
@Slf4j
public class DiscrepancyItemWriter implements ItemWriter<Discrepancy> {

    @Autowired 
    @Qualifier("paymentReconciliationService")
    private PaymentReconciliationService paymentReconciliationService;

    @Override
    public void write(Chunk<? extends Discrepancy> chunk) throws Exception {
        List<? extends Discrepancy> discrepancies = chunk.getItems();
        if (discrepancies == null || discrepancies.isEmpty()) {
            return;
        }
        
        log.info("写入 {} 条支付差异记录", discrepancies.size());
        
        for (Discrepancy discrepancy : discrepancies) {
            try {
                // 检查是否已存在该订单的差异记录
                PaymentReconciliation existingRecord = paymentReconciliationService.getById(discrepancy.getOrderNo());
                
                if (existingRecord != null) {
                    // 已存在，更新记录
                    updateDiscrepancy(discrepancy);
                } else {
                    // 不存在，插入新记录
                    insertDiscrepancy(discrepancy);
                }
                
            } catch (Exception e) {
                log.error("写入支付差异记录失败，订单号: {}", discrepancy.getOrderNo(), e);
            }
        }
    }
    
    /**
     * 插入新的差异记录
     * @param discrepancy 差异对象
     */
    private void insertDiscrepancy(Discrepancy discrepancy) {
        PaymentReconciliation reconciliation = new PaymentReconciliation();
        reconciliation.setOrderNo(discrepancy.getOrderNo());
        reconciliation.setExpected(discrepancy.getExpected());
        reconciliation.setActual(discrepancy.getActual());
        reconciliation.setDiscrepancyType(discrepancy.getType());
        reconciliation.setDiscrepancyAmount(discrepancy.calculateDiscrepancy());
        reconciliation.setIsFixed(0);
        reconciliation.setChannel(discrepancy.getChannel());
        reconciliation.setBackupChannel(discrepancy.getBackupChannel());
        reconciliation.setCreateTime(new Date());
        reconciliation.setUpdateTime(new Date());
        
        paymentReconciliationService.save(reconciliation);
        
        log.debug("插入支付差异记录，订单号: {}, 类型: {}", discrepancy.getOrderNo(), discrepancy.getType());
    }
    
    /**
     * 更新已存在的差异记录
     * @param discrepancy 差异对象
     */
    private void updateDiscrepancy(Discrepancy discrepancy) {
        PaymentReconciliation reconciliation = new PaymentReconciliation();
        reconciliation.setOrderNo(discrepancy.getOrderNo());
        reconciliation.setExpected(discrepancy.getExpected());
        reconciliation.setActual(discrepancy.getActual());
        reconciliation.setDiscrepancyType(discrepancy.getType());
        reconciliation.setDiscrepancyAmount(discrepancy.calculateDiscrepancy());
        reconciliation.setChannel(discrepancy.getChannel());
        reconciliation.setBackupChannel(discrepancy.getBackupChannel());
        reconciliation.setUpdateTime(new Date());
        
        paymentReconciliationService.updateById(reconciliation);
        
        log.debug("更新支付差异记录，订单号: {}, 类型: {}", discrepancy.getOrderNo(), discrepancy.getType());
    }
} 