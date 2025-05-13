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
package com.jeequan.jeepay.pay.task;

import com.jeequan.jeepay.pay.model.PaymentReconciliation;
import com.jeequan.jeepay.pay.service.PostgreSQLReconciliationServiceAdapter;
import com.jeequan.jeepay.pay.service.PaymentReconciliationEngineService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * 支付对账定时任务
 * 使用PostgreSQL物化视图进行支付对账，无论主数据源是什么
 * 通过适配器模式支持同时使用JdbcTemplate和MyBatis-Plus实现
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/9/16
 */
@Slf4j
@Configuration
public class PaymentReconciliationTask {

    @Autowired
    private PostgreSQLReconciliationServiceAdapter reconciliationService;
    
    @Autowired
    private PaymentReconciliationEngineService reconciliationEngineService;
    
    @Value("${jeepay.reconciliation.refresh-interval:5000}")
    private long refreshInterval;
    
    @Value("${jeepay.reconciliation.fix-interval:60000}")
    private long fixInterval;
    
    /**
     * 定时刷新支付对账物化视图
     * 默认每5秒执行一次
     */
    @Scheduled(fixedDelayString = "${jeepay.reconciliation.refresh-interval:5000}")
    public void refreshReconciliationView() {
        try {
            log.debug("开始定时刷新支付对账视图...");
            reconciliationService.refreshReconciliationView();
            
            // 获取对账统计
            logReconciliationStats();
            
            log.debug("支付对账视图刷新完成");
        } catch (Exception e) {
            log.error("刷新支付对账视图失败", e);
        }
    }
    
    /**
     * 定时处理未修复的支付差异
     * 默认每分钟执行一次
     */
    @Scheduled(fixedDelayString = "${jeepay.reconciliation.fix-interval:60000}")
    public void processUnfixedDiscrepancies() {
        try {
            log.info("开始处理未修复的支付差异...");
            
            // 获取所有未修复的差异
            List<PaymentReconciliation> unfixedDiscrepancies = reconciliationService.findAllUnfixedDiscrepancies();
            if (unfixedDiscrepancies == null || unfixedDiscrepancies.isEmpty()) {
                log.info("没有未修复的支付差异");
                return;
            }
            
            log.info("发现 {} 条未修复的支付差异", unfixedDiscrepancies.size());
            
            // 处理每个差异
            for (PaymentReconciliation discrepancy : unfixedDiscrepancies) {
                try {
                    if (discrepancy == null) {
                        continue;
                    }
                    
                    log.info("处理支付差异，订单号: {}, 差异类型: {}, 差异金额: {}", 
                            discrepancy.getOrderNo(), 
                            discrepancy.getDiscrepancyType(), 
                            discrepancy.getDiscrepancyAmount());
                    
                    // 根据差异类型进行处理
                    switch (discrepancy.getDiscrepancyType()) {
                        case "AMOUNT_MISMATCH":
                            // 处理金额不匹配，这里调用资金修正逻辑
                            boolean amountFixed = reconciliationEngineService.handleAmountMismatch(
                                    discrepancy.getOrderNo(), 
                                    discrepancy.getExpected(), 
                                    discrepancy.getActual());
                            
                            if (amountFixed) {
                                // 标记为已修复
                                reconciliationService.markAsFixed(discrepancy.getOrderNo());
                                log.info("金额不匹配差异已修复，订单号: {}", discrepancy.getOrderNo());
                            }
                            break;
                            
                        case "MISSING_PAYMENT":
                            // 处理缺失支付记录，这里调用支付记录补偿逻辑
                            boolean recordFixed = reconciliationEngineService.handleMissingPayment(
                                    discrepancy.getOrderNo(), 
                                    discrepancy.getExpected());
                            
                            if (recordFixed) {
                                // 标记为已修复
                                reconciliationService.markAsFixed(discrepancy.getOrderNo());
                                log.info("缺失支付记录差异已修复，订单号: {}", discrepancy.getOrderNo());
                            }
                            break;
                            
                        default:
                            log.warn("未知的差异类型: {}, 订单号: {}", 
                                    discrepancy.getDiscrepancyType(), discrepancy.getOrderNo());
                    }
                    
                } catch (Exception e) {
                    log.error("处理支付差异失败，订单号: {}", discrepancy.getOrderNo(), e);
                }
            }
            
            log.info("支付差异处理完成");
            
        } catch (Exception e) {
            log.error("处理未修复的支付差异失败", e);
        }
    }
    
    /**
     * 记录对账统计信息
     */
    private void logReconciliationStats() {
        try {
            Map<String, Object> stats = reconciliationService.getReconciliationStats();
            if (stats == null || stats.isEmpty()) {
                log.warn("无法获取对账统计信息");
                return;
            }
            
            Object totalDiscrepanciesObj = stats.get("total_discrepancies");
            if (totalDiscrepanciesObj == null) {
                return;
            }
            
            long totalDiscrepancies = ((Number) totalDiscrepanciesObj).longValue();
            
            if (totalDiscrepancies > 0) {
                log.info("当前对账统计 - 总记录数: {}, 差异总数: {}, 已修复: {}, 金额不匹配: {}, 缺失支付记录: {}, 差异总金额: {}",
                        stats.get("total_records"),
                        stats.get("total_discrepancies"),
                        stats.get("fixed_discrepancies"),
                        stats.get("amount_mismatches"),
                        stats.get("missing_payments"),
                        stats.get("total_discrepancy_amount"));
            }
        } catch (Exception e) {
            log.warn("获取对账统计信息失败: {}", e.getMessage());
        }
    }
} 