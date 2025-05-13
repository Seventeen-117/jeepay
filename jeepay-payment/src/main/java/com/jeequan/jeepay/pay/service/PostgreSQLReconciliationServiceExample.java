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
package com.jeequan.jeepay.pay.service;

import com.jeequan.jeepay.core.entity.PaymentReconciliation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * PostgreSQL支付对账服务使用示例
 * 演示如何使用基于MyBatis-Plus的PostgreSQL对账服务
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/9/16
 */
@Service
@Slf4j
public class PostgreSQLReconciliationServiceExample {

    @Autowired
    private PostgreSQLReconciliationServiceMybatis reconciliationService;
    
    /**
     * 执行对账流程示例
     */
    public void executeReconciliation() {
        try {
            log.info("开始执行对账流程...");
            
            // 刷新对账数据
            reconciliationService.refreshReconciliationView();
            
            // 查询未处理的差异记录
            List<PaymentReconciliation> discrepancies = reconciliationService.findAllUnfixedDiscrepancies();
            log.info("发现{}条未处理的差异记录", discrepancies.size());
            
            // 处理差异记录
            for (PaymentReconciliation discrepancy : discrepancies) {
                try {
                    processSingleDiscrepancy(discrepancy);
                } catch (Exception e) {
                    log.error("处理差异记录失败: {}", e.getMessage(), e);
                }
            }
            
            // 获取对账统计信息
            Map<String, Object> stats = reconciliationService.getReconciliationStats();
            log.info("对账统计信息: {}", stats);
            
            // 按渠道查看差异情况
            List<Map<String, Object>> channelStats = reconciliationService.getDiscrepanciesByChannel();
            log.info("各渠道对账差异统计: {}", channelStats);
            
            log.info("对账流程执行完成");
        } catch (Exception e) {
            log.error("执行对账流程失败: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 处理单个差异记录
     */
    private void processSingleDiscrepancy(PaymentReconciliation discrepancy) {
        String orderNo = discrepancy.getOrderNo();
        log.info("处理差异记录[{}], 差异类型: {}, 差异金额: {}", 
                orderNo, discrepancy.getDiscrepancyType(), discrepancy.getDiscrepancyAmount());
        
        // 根据差异类型处理
        switch (discrepancy.getDiscrepancyType()) {
            case "MISSING_PAYMENT":
                // 处理缺失支付记录的情况
                handleMissingPayment(discrepancy);
                break;
                
            case "AMOUNT_MISMATCH":
                // 处理金额不匹配的情况
                handleAmountMismatch(discrepancy);
                break;
                
            default:
                log.warn("未知的差异类型: {}", discrepancy.getDiscrepancyType());
        }
        
        // 标记差异记录为已处理
        boolean marked = reconciliationService.markAsFixed(orderNo);
        log.info("差异记录[{}]标记为已处理: {}", orderNo, marked);
    }
    
    /**
     * 处理缺失支付记录的情况
     */
    private void handleMissingPayment(PaymentReconciliation discrepancy) {
        // 这里只是示例，实际处理逻辑需要根据业务需求实现
        log.info("处理缺失支付记录: 订单号[{}], 金额[{}]", 
                discrepancy.getOrderNo(), discrepancy.getExpected());
        
        // 可能的处理方法:
        // 1. 向支付渠道查询支付状态
        // 2. 如果支付成功，创建支付记录
        // 3. 如果支付失败，可能需要重新发起支付或取消订单
    }
    
    /**
     * 处理金额不匹配的情况
     */
    private void handleAmountMismatch(PaymentReconciliation discrepancy) {
        // 这里只是示例，实际处理逻辑需要根据业务需求实现
        log.info("处理金额不匹配: 订单号[{}], 预期金额[{}], 实际金额[{}], 差异金额[{}]", 
                discrepancy.getOrderNo(), discrepancy.getExpected(), 
                discrepancy.getActual(), discrepancy.getDiscrepancyAmount());
        
        // 可能的处理方法:
        // 1. 如果差异金额很小，可能视为四舍五入误差，直接忽略
        // 2. 如果实际支付金额大于预期，可能需要退款
        // 3. 如果实际支付金额小于预期，可能需要追加支付或取消订单
    }
    
    /**
     * 按日期范围查询对账差异示例
     */
    public void searchDiscrepanciesByDateRange() {
        try {
            // 查询最近7天的差异记录
            LocalDate endDate = LocalDate.now();
            LocalDate startDate = endDate.minusDays(7);
            
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            String startDateStr = startDate.format(formatter);
            String endDateStr = endDate.format(formatter);
            
            List<PaymentReconciliation> discrepancies = 
                    reconciliationService.findDiscrepanciesByDateRange(startDateStr, endDateStr);
            
            log.info("找到{}条最近7天的差异记录", discrepancies.size());
            
            // 处理差异记录
            for (PaymentReconciliation discrepancy : discrepancies) {
                log.info("差异记录: 订单号[{}], 类型[{}], 金额[{}]", 
                        discrepancy.getOrderNo(), discrepancy.getDiscrepancyType(), 
                        discrepancy.getDiscrepancyAmount());
            }
        } catch (Exception e) {
            log.error("按日期范围查询对账差异失败: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 通过订单号查询并处理对账记录示例
     */
    public void findAndProcessByOrderNo(String orderNo) {
        try {
            // 查询指定订单的对账记录
            PaymentReconciliation reconciliation = reconciliationService.findByOrderNo(orderNo);
            
            if (reconciliation == null) {
                log.info("未找到订单[{}]的对账记录", orderNo);
                return;
            }
            
            log.info("订单[{}]对账记录: 预期金额[{}], 实际金额[{}], 差异类型[{}], 差异金额[{}], 是否已处理[{}]", 
                    orderNo, reconciliation.getExpected(), reconciliation.getActual(), 
                    reconciliation.getDiscrepancyType(), reconciliation.getDiscrepancyAmount(), 
                    reconciliation.getIsFixed());
            
            // 如果存在差异且未处理，进行处理
            if (!"NONE".equals(reconciliation.getDiscrepancyType()) && 
                    (reconciliation.getIsFixed() == null || reconciliation.getIsFixed() == 0)) {
                processSingleDiscrepancy(reconciliation);
            }
        } catch (Exception e) {
            log.error("查询并处理订单[{}]的对账记录失败: {}", orderNo, e.getMessage(), e);
        }
    }
    
    /**
     * 批量修复所有差异记录示例
     */
    public void fixAllDiscrepancies() {
        try {
            int count = reconciliationService.fixAllDiscrepancies();
            log.info("成功修复{}条差异记录", count);
        } catch (Exception e) {
            log.error("批量修复所有差异记录失败: {}", e.getMessage(), e);
        }
    }
} 