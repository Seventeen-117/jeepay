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

import com.jeequan.jeepay.pay.model.PaymentReconciliation;
import com.jeequan.jeepay.pay.util.ReconciliationModelConverter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * PostgreSQL支付对账服务适配器
 * 用于在原JdbcTemplate-based service和新的MyBatis-Plus-based service之间提供兼容性
 * 这个适配器类使得原有的代码可以无缝切换到新的MyBatis-Plus实现
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/9/16
 */
@Service
@Slf4j
public class PostgreSQLReconciliationServiceAdapter {
    
    @Autowired
    private PostgreSQLReconciliationServiceMybatis mybatisService;
    
    // 默认使用MyBatis-Plus实现
    private boolean useNewImplementation = true;
    
    @PostConstruct
    public void init() {
        // 强制使用MyBatis-Plus实现
        this.useNewImplementation = true;
        log.info("PostgreSQL支付对账服务适配器初始化，强制使用MyBatis-Plus实现");
    }
    
    /**
     * 设置是否使用新的MyBatis-Plus实现
     * 该方法被重写为始终使用MyBatis-Plus实现
     * @param useNewImplementation 该参数被忽略，始终设置为true
     */
    public void setUseNewImplementation(boolean useNewImplementation) {
        // 忽略传入参数，强制使用MyBatis-Plus实现
        this.useNewImplementation = true;
        log.info("强制使用MyBatis-Plus实现，忽略传入参数");
    }
    
    /**
     * 刷新支付对账物化视图
     */
    public void refreshReconciliationView() {
        try {
            // 强制使用MyBatis-Plus实现
            mybatisService.refreshReconciliationView();
        } catch (Exception e) {
            log.error("使用MyBatis-Plus实现刷新对账视图失败: {}", e.getMessage(), e);
            try {
                // 尝试重试MyBatis-Plus实现
                log.info("尝试重新刷新...");
                mybatisService.refreshReconciliationView();
            } catch (Exception fallbackEx) {
                log.error("重试刷新对账视图失败: {}", fallbackEx.getMessage());
            }
        }
    }
    
    /**
     * 查找所有未处理的差异记录
     */
    public List<PaymentReconciliation> findAllUnfixedDiscrepancies() {
        try {
            // 强制使用MyBatis-Plus实现
            List<com.jeequan.jeepay.core.entity.PaymentReconciliation> mybatisResult = 
                mybatisService.findAllUnfixedDiscrepancies();
            if (mybatisResult == null) {
                return Collections.emptyList();
            }
            return ReconciliationModelConverter.toJpaModelList(mybatisResult);
        } catch (Exception e) {
            log.error("使用MyBatis-Plus查询未处理差异记录失败: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }
    
    /**
     * 标记差异记录为已处理
     */
    public boolean markAsFixed(String orderNo) {
        try {
            // 强制使用MyBatis-Plus实现
            return mybatisService.markAsFixed(orderNo);
        } catch (Exception e) {
            log.error("使用MyBatis-Plus标记差异记录为已处理失败: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 获取对账统计信息
     */
    public Map<String, Object> getReconciliationStats() {
        try {
            // 强制使用MyBatis-Plus实现
            return mybatisService.getReconciliationStats();
        } catch (Exception e) {
            log.error("使用MyBatis-Plus获取对账统计信息失败: {}", e.getMessage(), e);
            return Collections.emptyMap();
        }
    }
    
    /**
     * 按渠道查看差异情况
     */
    public List<Map<String, Object>> getDiscrepanciesByChannel() {
        try {
            // 强制使用MyBatis-Plus实现
            return mybatisService.getDiscrepanciesByChannel();
        } catch (Exception e) {
            log.error("使用MyBatis-Plus按渠道查看差异情况失败: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }
    
    /**
     * 按日期范围查询对账差异
     */
    public List<PaymentReconciliation> findDiscrepanciesByDateRange(String startDate, String endDate) {
        try {
            // 强制使用MyBatis-Plus实现
            List<com.jeequan.jeepay.core.entity.PaymentReconciliation> mybatisResult = 
                mybatisService.findDiscrepanciesByDateRange(startDate, endDate);
            if (mybatisResult == null) {
                return Collections.emptyList();
            }
            return ReconciliationModelConverter.toJpaModelList(mybatisResult);
        } catch (Exception e) {
            log.error("使用MyBatis-Plus按日期范围查询对账差异失败: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }
    
    /**
     * 通过订单号查询对账记录
     */
    public PaymentReconciliation findByOrderNo(String orderNo) {
        try {
            // 强制使用MyBatis-Plus实现
            return ReconciliationModelConverter.toJpaModel(
                mybatisService.findByOrderNo(orderNo));
        } catch (Exception e) {
            log.error("使用MyBatis-Plus通过订单号查询对账记录失败: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * 修复所有差异记录
     */
    public int fixAllDiscrepancies() {
        try {
            // 强制使用MyBatis-Plus实现
            return mybatisService.fixAllDiscrepancies();
        } catch (Exception e) {
            log.error("使用MyBatis-Plus修复所有差异记录失败: {}", e.getMessage(), e);
            return 0;
        }
    }
} 