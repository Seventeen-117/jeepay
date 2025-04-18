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
package com.jeequan.jeepay.pay.rqrs.payorder.payway;

import com.jeequan.jeepay.core.constants.CS;
import com.jeequan.jeepay.pay.rqrs.payorder.UnifiedOrderRQ;
import lombok.Data;

import jakarta.validation.constraints.NotBlank;

/*
 * 支付方式： UP_JSAPI
 *
 * @author jmdhappy
 * @site https://www.jeequan.com
 * @date 2021/6/8 17:34
 */
@Data
public class UpJsapiOrderRQ extends UnifiedOrderRQ {

    /** 支付宝用户ID **/
    @NotBlank(message = "用户ID不能为空")
    private String userId;

    /** 构造函数 **/
    public UpJsapiOrderRQ(){
        this.setWayCode(CS.PAY_WAY_CODE.UP_JSAPI);
    }

    @Override
    public String getChannelUserId(){
        return this.userId;
    }

}
