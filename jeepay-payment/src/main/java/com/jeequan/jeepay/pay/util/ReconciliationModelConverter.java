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
package com.jeequan.jeepay.pay.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * 对账模型转换工具类
 * 用于JPA实体类和MyBatis-Plus实体类之间的转换
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/9/16
 */
public class ReconciliationModelConverter {

    /**
     * 将JPA模型转换为MyBatis-Plus模型
     * @param source JPA模型
     * @return MyBatis-Plus模型
     */
    public static com.jeequan.jeepay.core.entity.PaymentReconciliation toMybatisModel(
            com.jeequan.jeepay.pay.model.PaymentReconciliation source) {
        
        if (source == null) {
            return null;
        }
        
        com.jeequan.jeepay.core.entity.PaymentReconciliation target = 
                new com.jeequan.jeepay.core.entity.PaymentReconciliation();
        
        target.setOrderNo(source.getOrderNo());
        target.setExpected(source.getExpectedAmount());
        target.setActual(source.getActualAmount());
        target.setDiscrepancyType(source.getDiscrepancyType());
        target.setDiscrepancyAmount(source.getDiscrepancyAmount());
        
        // JPA模型中isFixed是Boolean，而MyBatis-Plus模型中是Integer
        target.setIsFixed(source.getIsFixed() != null && source.getIsFixed() ? 1 : 0);
        
        target.setChannel(source.getChannel());
        target.setBackupIfCode(source.getBackupIfCode());
        
        // 设置时间字段
        Date now = new Date();
        target.setCreateTime(now);
        target.setUpdateTime(now);
        
        return target;
    }
    
    /**
     * 将MyBatis-Plus模型转换为JPA模型
     * @param source MyBatis-Plus模型
     * @return JPA模型，如果source为null则返回null
     */
    public static com.jeequan.jeepay.pay.model.PaymentReconciliation toJpaModel(
            com.jeequan.jeepay.core.entity.PaymentReconciliation source) {
        
        if (source == null) {
            return null;
        }
        
        com.jeequan.jeepay.pay.model.PaymentReconciliation target = 
                new com.jeequan.jeepay.pay.model.PaymentReconciliation();
        
        target.setOrderNo(source.getOrderNo());
        target.setExpectedAmount(source.getExpected());
        target.setActualAmount(source.getActual());
        target.setDiscrepancyType(source.getDiscrepancyType());
        target.setDiscrepancyAmount(source.getDiscrepancyAmount());
        
        // MyBatis-Plus模型中isFixed是Integer，而JPA模型中是Boolean
        target.setIsFixed(source.getIsFixed() != null && source.getIsFixed() == 1);
        
        target.setChannel(source.getChannel());
        target.setBackupIfCode(source.getBackupIfCode());
        
        return target;
    }
    
    /**
     * 将JPA模型列表转换为MyBatis-Plus模型列表
     * @param sourceList JPA模型列表
     * @return MyBatis-Plus模型列表，如果sourceList为null则返回空列表
     */
    public static List<com.jeequan.jeepay.core.entity.PaymentReconciliation> toMybatisModelList(
            List<com.jeequan.jeepay.pay.model.PaymentReconciliation> sourceList) {
        
        if (sourceList == null || sourceList.isEmpty()) {
            return Collections.emptyList();
        }
        
        List<com.jeequan.jeepay.core.entity.PaymentReconciliation> targetList = 
                new ArrayList<>(sourceList.size());
        
        for (com.jeequan.jeepay.pay.model.PaymentReconciliation source : sourceList) {
            if (source != null) {
                com.jeequan.jeepay.core.entity.PaymentReconciliation target = toMybatisModel(source);
                if (target != null) {
                    targetList.add(target);
                }
            }
        }
        
        return targetList;
    }
    
    /**
     * 将MyBatis-Plus模型列表转换为JPA模型列表
     * @param sourceList MyBatis-Plus模型列表
     * @return JPA模型列表，如果sourceList为null则返回空列表
     */
    public static List<com.jeequan.jeepay.pay.model.PaymentReconciliation> toJpaModelList(
            List<com.jeequan.jeepay.core.entity.PaymentReconciliation> sourceList) {
        
        if (sourceList == null || sourceList.isEmpty()) {
            return Collections.emptyList();
        }
        
        List<com.jeequan.jeepay.pay.model.PaymentReconciliation> targetList = 
                new ArrayList<>(sourceList.size());
        
        for (com.jeequan.jeepay.core.entity.PaymentReconciliation source : sourceList) {
            if (source != null) {
                com.jeequan.jeepay.pay.model.PaymentReconciliation target = toJpaModel(source);
                if (target != null) {
                    targetList.add(target);
                }
            }
        }
        
        return targetList;
    }
} 