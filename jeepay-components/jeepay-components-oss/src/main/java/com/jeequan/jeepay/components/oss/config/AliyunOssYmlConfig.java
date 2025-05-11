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
package com.jeequan.jeepay.components.oss.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
* aliyun oss 的yml配置参数
*
* @author terrfly
* @site curverun.com
* @date 2021/7/12 18:18
*/
@Data
@Component
@ConfigurationProperties(prefix="isys.oss.aliyun-oss")
public class AliyunOssYmlConfig {

	private String endpoint;
	private String publicBucketName;
	private String privateBucketName;
	private String accessKeyId;
	private String accessKeySecret;
}



