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
import org.springframework.stereotype.Service;

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
public class PaymentReconciliationService extends ServiceImpl<PaymentReconciliationMapper, PaymentReconciliation> {

    /**
     * 创建视图
     */
    public void createMaterializedView() {
        try {
            // 先尝试删除可能存在的表，以便创建视图
            baseMapper.dropTableIfExists();
            // 也尝试删除可能存在的视图
            baseMapper.dropViewIfExists();
        } catch (Exception e) {
            // 忽略删除时的错误，可能表或视图不存在
        }
        
        // 创建视图
        baseMapper.createMaterializedView();
    }

    /**
     * 刷新视图
     */
    public void refreshMaterializedView() {
        baseMapper.refreshMaterializedView();
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