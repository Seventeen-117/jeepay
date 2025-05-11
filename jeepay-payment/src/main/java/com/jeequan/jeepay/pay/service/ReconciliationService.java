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
package com.jeequan.jeepay.pay.service;

import com.jeequan.jeepay.core.entity.PaymentRecord;
import com.jeequan.jeepay.core.entity.PaymentReconciliation;
import com.jeequan.jeepay.service.impl.PaymentReconciliationService;
import com.jeequan.jeepay.service.impl.PaymentRecordService;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

/**
 * 支付对账服务
 * 负责自动对账和修复支付差异
 *
 * @author jeepay
 * @site https://www.jeequan.com
 * @date 2023/8/12
 */
@Service
@Slf4j
public class ReconciliationService {

    @Autowired
    @Qualifier("paymentReconciliationService")
    private PaymentReconciliationService reconciliationService;
    @Autowired private PaymentRecordService paymentRecordService;

    /**
     * 服务启动时创建对账视图
     */
    @PostConstruct
    public void init() {
        try {
            reconciliationService.createMaterializedView();
            log.info("支付对账视图初始化成功");
        } catch (Exception e) {
            log.error("初始化支付对账视图失败", e);
        }
    }

    /**
     * 定时刷新对账数据（MySQL视图会自动更新，此方法仅作为兼容保留）
     */
    @Scheduled(fixedDelay = 5000)
    public void refreshReconciliationView() {
        try {
            // MySQL视图会自动更新，不需要刷新操作
            reconciliationService.refreshMaterializedView();
            log.debug("支付对账数据刷新成功");
        } catch (Exception e) {
            log.error("刷新支付对账数据失败", e);
        }
    }

    /**
     * 定时自动修复差异（每分钟）
     */
    @Scheduled(fixedDelay = 60000)
    @Transactional(rollbackFor = Exception.class)
    public void autoFixDiscrepancies() {
        try {
            // 查找所有未处理的差异记录
            List<PaymentReconciliation> unfixedDiscrepancies = reconciliationService.findUnfixedDiscrepancies();
            
            if (unfixedDiscrepancies.isEmpty()) {
                return;
            }
            
            log.info("开始自动修复 {} 条支付差异", unfixedDiscrepancies.size());
            
            for (PaymentReconciliation discrepancy : unfixedDiscrepancies) {
                try {
                    if ("MISSING_PAYMENT".equals(discrepancy.getDiscrepancyType())) {
                        // 处理缺失支付记录的情况
                        fixMissingPayment(discrepancy);
                    } else if ("AMOUNT_MISMATCH".equals(discrepancy.getDiscrepancyType())) {
                        // 处理金额不匹配的情况
                        fixAmountMismatch(discrepancy);
                    }
                    
                    // 标记为已处理
                    reconciliationService.markAsFixed(discrepancy.getOrderNo());
                    
                } catch (Exception e) {
                    log.error("修复支付差异失败，订单号: {}, 类型: {}", 
                            discrepancy.getOrderNo(), discrepancy.getDiscrepancyType(), e);
                }
            }
            
        } catch (Exception e) {
            log.error("自动修复支付差异任务失败", e);
        }
    }
    
    /**
     * 修复缺失的支付记录
     * @param discrepancy 差异记录
     */
    private void fixMissingPayment(PaymentReconciliation discrepancy) {
        log.info("修复缺失支付记录，订单号: {}, 金额: {}", discrepancy.getOrderNo(), discrepancy.getExpected());
        
        // 创建支付记录
        PaymentRecord paymentRecord = new PaymentRecord();
        paymentRecord.setOrderNo(discrepancy.getOrderNo());
        paymentRecord.setAmount(discrepancy.getExpected());
        paymentRecord.setChannel(discrepancy.getChannel());
        paymentRecord.setBackupChannel(discrepancy.getBackupChannel());
        paymentRecord.setCreateTime(new Date());
        paymentRecord.setUpdateTime(new Date());
        
        paymentRecordService.save(paymentRecord);
        
        log.info("成功修复缺失支付记录，订单号: {}", discrepancy.getOrderNo());
    }
    
    /**
     * 修复金额不匹配的情况
     * @param discrepancy 差异记录
     */
    private void fixAmountMismatch(PaymentReconciliation discrepancy) {
        log.info("修复金额不匹配，订单号: {}, 预期金额: {}, 实际金额: {}, 差额: {}", 
                discrepancy.getOrderNo(), discrepancy.getExpected(), 
                discrepancy.getActual(), discrepancy.getDiscrepancyAmount());
        
        // 更新支付记录金额
        PaymentRecord record = paymentRecordService.lambdaQuery()
                .eq(PaymentRecord::getOrderNo, discrepancy.getOrderNo())
                .one();
        
        if (record != null) {
            record.setAmount(discrepancy.getExpected());
            record.setUpdateTime(new Date());
            paymentRecordService.updateById(record);
            
            log.info("成功修复金额不匹配，订单号: {}", discrepancy.getOrderNo());
        } else {
            log.warn("无法修复金额不匹配，未找到支付记录，订单号: {}", discrepancy.getOrderNo());
        }
    }
    
    /**
     * 根据订单号修复支付差异
     * 供批处理任务调用
     * @param orderNo 订单号
     */
    public void autoFixDiscrepancy(String orderNo) {
        try {
            // 查询差异记录
            PaymentReconciliation discrepancy = reconciliationService.getById(orderNo);
            
            if (discrepancy == null) {
                log.warn("未找到支付差异记录，订单号: {}", orderNo);
                return;
            }
            
            if ("MISSING_PAYMENT".equals(discrepancy.getDiscrepancyType())) {
                // 处理缺失支付记录的情况
                fixMissingPayment(discrepancy);
            } else if ("AMOUNT_MISMATCH".equals(discrepancy.getDiscrepancyType())) {
                // 处理金额不匹配的情况
                fixAmountMismatch(discrepancy);
            }
            
            // 标记为已处理
            reconciliationService.markAsFixed(orderNo);
            log.info("成功修复支付差异，订单号: {}, 类型: {}", orderNo, discrepancy.getDiscrepancyType());
            
        } catch (Exception e) {
            log.error("修复支付差异失败，订单号: {}", orderNo, e);
            throw new RuntimeException("修复支付差异失败", e);
        }
    }
} 