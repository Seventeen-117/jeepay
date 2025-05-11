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
package com.jeequan.jeepay.pay.model;

import com.jeequan.jeepay.core.entity.IsvInfo;
import com.jeequan.jeepay.core.model.params.IsvParams;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/*
 * Isv支付参数信息 放置到内存， 避免多次查询操作
 *
 * @author terrfly
 * @site curverun.com
 * @date 2021/6/8 17:28
 */
@Data
public class IsvConfigContext {

    /** isv信息缓存 */
    private String isvNo;
    private IsvInfo isvInfo;

    /** 商户支付配置信息缓存 */
    private Map<String, IsvParams> isvParamsMap = new HashMap<>();


    /** 缓存支付宝client 对象 **/
    private AlipayClientWrapper alipayClientWrapper;

    /** 缓存 wxServiceWrapper 对象 **/
    private WxServiceWrapper wxServiceWrapper;


    /** 获取isv配置信息 **/
    public IsvParams getIsvParamsByIfCode(String ifCode){
        return isvParamsMap.get(ifCode);
    }

    /** 获取isv配置信息 **/
    public <T> T getIsvParamsByIfCode(String ifCode, Class<? extends IsvParams> cls){
        return (T)isvParamsMap.get(ifCode);
    }

}
