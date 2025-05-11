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
package com.jeequan.jeepay.pay.rqrs.payorder;

import lombok.Data;

/*
* 通用支付数据RQ
*
* @author terrfly
* @site curverun.com
* @date 2021/6/8 17:31
*/
@Data
public class CommonPayDataRQ extends UnifiedOrderRQ {

    /** 请求参数： 支付数据包类型 **/
    private String payDataType;

}
