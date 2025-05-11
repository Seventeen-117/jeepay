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

import com.jeequan.jeepay.core.entity.MchApp;
import com.jeequan.jeepay.core.entity.MchInfo;
import lombok.Data;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/*
* 商户配置信息
* 放置到内存， 避免多次查询操作
*
* @author terrfly
* @site curverun.com
* @date 2021/6/8 17:29
*/
@Data
public class MchInfoConfigContext {


    /** 商户信息缓存 */
    private String mchNo;
    private Byte mchType;
    private MchInfo mchInfo;
    private Map<String, MchApp> appMap = new ConcurrentHashMap<>();

    /** 重置商户APP **/
    public void putMchApp(MchApp mchApp){
        appMap.put(mchApp.getAppId(), mchApp);
    }

    /** get商户APP **/
    public MchApp getMchApp(String appId){
        return appMap.get(appId);
    }

}
