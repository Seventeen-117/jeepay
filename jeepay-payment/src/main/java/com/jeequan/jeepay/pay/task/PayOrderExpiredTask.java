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
package com.jeequan.jeepay.pay.task;

import com.jeequan.jeepay.service.impl.PayOrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/*
* 订单过期定时任务
*
* @author terrfly
* @site curverun.com
* @date 2021/6/8 17:47
*/
@Slf4j
@Component
public class PayOrderExpiredTask {

    @Autowired private PayOrderService payOrderService;

    @Scheduled(cron="0 0/1 * * * ?") // 每分钟执行一次
    public void start() {

        int updateCount = payOrderService.updateOrderExpired();
        log.info("处理订单超时{}条.", updateCount);
    }


}
