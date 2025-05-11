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
package com.jeequan.jeepay.core.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.IdType;

import java.io.Serializable;

/**
 * <p>
 * 系统角色权限关联表
 * </p>
 *
 * @author [mybatis plus generator]
 * @since 2021-04-23
 */
@Schema(description = "系统角色权限关联表")
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@TableName("t_sys_role_ent_rela")
public class SysRoleEntRela implements Serializable {

    //gw
    public static final LambdaQueryWrapper<SysRoleEntRela> gw(){
        return new LambdaQueryWrapper<>();
    }

    private static final long serialVersionUID=1L;

    // 新增复合主键类
    @Data
    @EqualsAndHashCode
    public static class SysRoleEntRelaId implements Serializable {
        private String roleId;
        private String entId;
    }

    /**
     * 角色ID
     */
    @TableId(value = "role_id", type = IdType.INPUT)
    @Schema(title = "roleId", description = "角色ID")
    private String roleId;

    /**
     * 权限ID
     */
    @Schema(title = "entId", description = "权限ID")
    private String entId;


}
