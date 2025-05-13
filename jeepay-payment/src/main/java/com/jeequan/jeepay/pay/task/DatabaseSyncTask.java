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

import com.jeequan.jeepay.pay.service.DatabaseSyncService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * 数据库同步定时任务
 * 负责定期将MySQL数据同步到PostgreSQL
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/9/16
 */
@Slf4j
@Configuration
@ConditionalOnProperty(name = "jeepay.sync.enabled", havingValue = "true", matchIfMissing = true)
public class DatabaseSyncTask {

    @Autowired
    private DatabaseSyncService databaseSyncService;

    /**
     * 定时同步数据库
     * 默认每5分钟执行一次
     */
    @Scheduled(fixedDelayString = "${jeepay.sync.interval:300000}")
    public void syncDatabases() {
        try {
            log.debug("开始执行定时数据同步任务...");
            databaseSyncService.syncAllTables();
            log.debug("数据同步任务完成");
        } catch (Exception e) {
            log.error("数据同步任务执行失败", e);
        }
    }
} 