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
package com.jeequan.jeepay.core.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.Date;

/**
 * 支付订单补偿记录
 * 用于SAGA事务补偿机制
 *
 * @author jeepay
 * @site https://www.jeequan.com
 * @date 2023/8/12
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@TableName("t_pay_order_compensation")
public class PayOrderCompensation implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 补偿记录ID
     */
    @TableId(value = "compensation_id", type = IdType.AUTO)
    private Long compensationId;

    /**
     * 支付订单号
     */
    private String payOrderId;

    /**
     * 商户号
     */
    private String mchNo;

    /**
     * 应用ID
     */
    private String appId;

    /**
     * 原支付接口代码
     */
    private String originalIfCode;

    /**
     * 补偿支付接口代码
     */
    private String compensationIfCode;

    /**
     * 订单金额
     */
    private Long amount;

    /**
     * 状态: 0-处理中, 1-成功, 2-失败
     */
    private Byte state;

    /**
     * 补偿结果信息
     */
    private String resultInfo;

    /**
     * 创建时间
     */
    private Date createdAt;

    /**
     * 更新时间
     */
    private Date updatedAt;

    /**
     * 构造查询条件
     */
    public static LambdaQueryWrapper<PayOrderCompensation> gw() {
        return new LambdaQueryWrapper<>();
    }
} 