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
package com.jeequan.jeepay.pay.config;

import com.jeequan.jeepay.core.entity.PayOrder;
import com.jeequan.jeepay.core.entity.PaymentReconciliation;
import com.jeequan.jeepay.pay.batch.DiscrepancyItemProcessor;
import com.jeequan.jeepay.pay.batch.DiscrepancyItemWriter;
import com.jeequan.jeepay.pay.batch.PayOrderItemReader;
import com.jeequan.jeepay.pay.model.Discrepancy;
import com.jeequan.jeepay.pay.service.ReconciliationService;
import com.jeequan.jeepay.service.impl.PaymentReconciliationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.List;

/**
 * Spring Batch配置类
 * 用于配置支付对账批处理任务
 *
 * @author jeepay
 * @site https://www.jeequan.com
 * @date 2023/8/12
 */
@Configuration
@EnableBatchProcessing
@Slf4j
public class SpringBatchConfig {

    @Autowired private PayOrderItemReader payOrderItemReader;
    @Autowired private DiscrepancyItemProcessor discrepancyItemProcessor;
    @Autowired private DiscrepancyItemWriter discrepancyItemWriter;
    @Autowired private ReconciliationService reconciliationService;
    @Autowired 
    @Qualifier("paymentReconciliationService")
    private PaymentReconciliationService paymentReconciliationService;
    @Autowired private JobRepository jobRepository;
    @Autowired private PlatformTransactionManager transactionManager;

    /**
     * 配置支付对账批处理任务
     * @return 批处理任务
     */
    @Bean
    public Job reconciliationJob() {
        return new JobBuilder("reconciliationJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(loadOrdersStep())
                .next(fixDiscrepanciesStep())
                .build();
    }

    /**
     * 加载订单并处理差异的步骤
     * @return 批处理步骤
     */
    @Bean
    public Step loadOrdersStep() {
        return new StepBuilder("loadOrders", jobRepository)
                .<PayOrder, Discrepancy>chunk(100, transactionManager)
                .reader(payOrderItemReader)
                .processor(discrepancyItemProcessor)
                .writer(discrepancyItemWriter)
                .build();
    }

    /**
     * 修复差异的步骤
     * @return 批处理步骤
     */
    @Bean
    public Step fixDiscrepanciesStep() {
        return new StepBuilder("fixDiscrepancies", jobRepository)
                .tasklet(fixDiscrepancyTasklet(), transactionManager)
                .build();
    }

    /**
     * 修复差异的任务
     * @return 任务实例
     */
    @Bean
    public Tasklet fixDiscrepancyTasklet() {
        return (contribution, chunkContext) -> {
            try {
                log.info("开始修复支付差异...");
                
                // 使用MyBatis-Plus查询未修复的差异记录
                List<PaymentReconciliation> unfixedDiscrepancies = paymentReconciliationService.lambdaQuery()
                    .ne(PaymentReconciliation::getDiscrepancyType, "NONE")
                    .eq(PaymentReconciliation::getIsFixed, false)
                    .select(PaymentReconciliation::getOrderNo)
                    .list();
                
                // 处理每一条差异记录
                for (PaymentReconciliation discrepancy : unfixedDiscrepancies) {
                    String orderNo = discrepancy.getOrderNo();
                    try {
                        // 自动修复差异
                        reconciliationService.autoFixDiscrepancy(orderNo);
                    } catch (Exception e) {
                        log.error("修复支付差异失败，订单号: {}", orderNo, e);
                    }
                }
                
                log.info("支付差异修复完成");
                return null;
            } catch (Exception e) {
                log.error("修复支付差异步骤执行失败", e);
                throw e;
            }
        };
    }
} 