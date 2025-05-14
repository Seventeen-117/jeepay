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
import com.jeequan.jeepay.core.entity.PaymentReconciliation;
import com.jeequan.jeepay.service.mapper.PaymentReconciliationMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * <p>
 * 支付对账表 服务实现类
 * </p>
 *
 * @author jiangyangpay
 * @since 2023-08-12
 */
@Service("paymentReconciliationService")
@Slf4j
public class PaymentReconciliationService extends ServiceImpl<PaymentReconciliationMapper, PaymentReconciliation> {

    @Autowired
    private DataSource dataSource;
    
    @Value("${spring.datasource.dynamic.primary:mysql}")
    private String primaryDataSource;

    /**
     * 判断当前使用的数据库类型
     * @return true如果是PostgreSQL，false如果是MySQL或其他数据库
     */
    private boolean isPostgreSQLDatabase() {
        // 如果明确配置了使用PostgreSQL作为主数据源
        if ("postgres".equalsIgnoreCase(primaryDataSource)) {
            return true;
        }
        
        // 通过检查数据库连接信息来判断
        try (Connection connection = dataSource.getConnection()) {
            String dbProductName = connection.getMetaData().getDatabaseProductName().toLowerCase();
            return dbProductName.contains("postgresql");
        } catch (SQLException e) {
            log.error("获取数据库类型出错", e);
            // 默认返回false，使用MySQL模式
            return false;
        }
    }

    /**
     * 创建视图或物化视图（根据数据库类型自动选择）
     */
    public void createMaterializedView() {
        try {
            // 不再删除物化视图，由postgresql_reconciliation_schema.sql脚本管理
            log.info("物化视图由postgresql_reconciliation_schema.sql脚本管理，不在代码中操作");

            // 根据数据库类型创建相应的视图
            if (isPostgreSQLDatabase()) {
                log.info("使用PostgreSQL物化视图来实现实时资金对账");
            } else {
                log.info("使用MySQL视图来实现对账");
            }

            log.info("对账视图创建完成");
        } catch (Exception e) {
            log.error("创建视图失败", e);
            throw e;
        }
    }

    /**
     * 刷新视图（对于MySQL视图不需要刷新，对于PostgreSQL需要刷新物化视图）
     */
    public void refreshMaterializedView() {
        if (isPostgreSQLDatabase()) {
            // PostgreSQL需要刷新物化视图
            baseMapper.refreshPostgreSQLMaterializedView();
        } else {
            // MySQL视图不需要刷新，执行空操作
            baseMapper.refreshMySQLView();
        }
    }

    /**
     * 查找未处理的差异记录
     * @return 未处理的差异记录列表
     */
    public List<PaymentReconciliation> findUnfixedDiscrepancies() {
        return lambdaQuery()
                .ne(PaymentReconciliation::getDiscrepancyType, "NONE")
                .eq(PaymentReconciliation::getIsFixed, 0)
                .list();
    }

    /**
     * 查找特定类型的未处理差异记录
     * @param discrepancyType 差异类型
     * @return 特定类型的未处理差异记录列表
     */
    public List<PaymentReconciliation> findUnfixedDiscrepanciesByType(String discrepancyType) {
        return lambdaQuery()
                .eq(PaymentReconciliation::getDiscrepancyType, discrepancyType)
                .eq(PaymentReconciliation::getIsFixed, 0)
                .list();
    }

    /**
     * 更新差异记录为已处理状态
     * @param orderNo 订单号
     * @return 更新的记录数
     */
    public int markAsFixed(String orderNo) {
        return baseMapper.markAsFixed(orderNo);
    }
} 