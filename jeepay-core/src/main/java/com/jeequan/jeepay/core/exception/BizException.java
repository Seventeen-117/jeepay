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
package com.jeequan.jeepay.core.exception;

import com.jeequan.jeepay.core.constants.ApiCodeEnum;
import com.jeequan.jeepay.core.model.ApiRes;
import lombok.Getter;
/*
* 自定义业务异常
*
* @author terrfly
* @site curverun.com
* @date 2021/6/8 16:33
*/
@Getter
public class BizException extends RuntimeException{

	private static final long serialVersionUID = 1L;

	private ApiRes apiRes;

	/** 业务自定义异常 **/
	public BizException(String msg) {
		super(msg);
		this.apiRes = ApiRes.customFail(msg);
	}

	public BizException(ApiCodeEnum apiCodeEnum, String... params) {
		super();
		apiRes = ApiRes.fail(apiCodeEnum, params);
	}

	public BizException(ApiRes apiRes) {
		super(apiRes.getMsg());
		this.apiRes = apiRes;
	}
}
