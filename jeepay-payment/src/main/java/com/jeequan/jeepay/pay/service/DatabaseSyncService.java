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
package com.jeequan.jeepay.pay.service;

import com.jeequan.jeepay.core.entity.PayOrderCompensation;
import jakarta.annotation.PostConstruct;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.jeequan.jeepay.core.entity.PayOrder;
import com.jeequan.jeepay.core.entity.PaymentRecord;
import com.jeequan.jeepay.core.entity.PaymentCompensationRecord;
import com.jeequan.jeepay.service.impl.PayOrderService;
import com.jeequan.jeepay.service.impl.PaymentRecordService;
import com.jeequan.jeepay.service.impl.PaymentCompensationRecordService;
import com.jeequan.jeepay.service.impl.PayOrderCompensationService;
import com.jeequan.jeepay.service.mapper.PayOrderMapper;
import com.jeequan.jeepay.service.mapper.PaymentRecordMapper;
import com.jeequan.jeepay.service.mapper.PaymentCompensationRecordMapper;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 数据库同步服务
 * 负责将MySQL数据同步到PostgreSQL，以支持资金对账功能
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/9/16
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "jeepay.sync.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(DatabaseSyncService.SyncProperties.class)
public class DatabaseSyncService {

    // 主数据源服务（MySQL）
    @Autowired
    private PayOrderService payOrderService;

    @Autowired
    private PaymentRecordService paymentRecordService;
    
    @Autowired
    private PaymentCompensationRecordService paymentCompensationRecordService;
    
    @Autowired
    private PayOrderCompensationService payOrderCompensationService;
    
    // 对账数据源（PostgreSQL）服务，使用对应的Mapper
    @Autowired
    @Qualifier("reconciliationPayOrderMapper")
    private PayOrderMapper reconciliationPayOrderMapper;
    
    @Autowired
    @Qualifier("reconciliationPaymentRecordMapper")
    private PaymentRecordMapper reconciliationPaymentRecordMapper;
    
    @Autowired
    @Qualifier("reconciliationPaymentCompensationRecordMapper")
    private PaymentCompensationRecordMapper reconciliationPaymentCompensationRecordMapper;
    
    @Autowired
    @Qualifier("reconciliationJdbcTemplate")
    private JdbcTemplate reconciliationJdbcTemplate;
    
    // 同步配置属性
    @Autowired
    private SyncProperties syncProperties;
    
    // 记录每个表的最后同步时间
    private final Map<String, LocalDateTime> lastSyncTimeMap = new ConcurrentHashMap<>();
    
    @PostConstruct
    public void init() {
        // 确保补偿表存在
        ensureCompensationTableExists();
        
        // 初始化最后同步时间
        initializeLastSyncTimes();
        
        // 启动时执行一次初始同步
        performInitialSync();
    }
    
    /**
     * 确保支付订单补偿表在PostgreSQL中存在
     */
    private void ensureCompensationTableExists() {
        log.info("检查PostgreSQL中支付订单补偿表是否存在...");
        try {
            JdbcTemplate jdbcTemplate = reconciliationJdbcTemplate;
            
            // 确保更新时间戳函数存在
            ensureUpdateTimestampFunctionExists(jdbcTemplate);
            
            // 检查表是否存在
            String checkTableSql = 
                "SELECT EXISTS (" +
                "   SELECT FROM pg_tables " +
                "   WHERE schemaname = 'public' " +
                "   AND tablename = 't_pay_order_compensation'" +
                ");";
            
            Boolean tableExists = jdbcTemplate.queryForObject(checkTableSql, Boolean.class);
            
            if (tableExists == null || !tableExists) {
                log.info("PostgreSQL中支付订单补偿表不存在，正在创建...");
                
                // 创建表的SQL语句
                String createTableSql = 
                    "CREATE TABLE t_pay_order_compensation (" +
                    "  compensation_id BIGSERIAL PRIMARY KEY," +
                    "  pay_order_id VARCHAR(30) NOT NULL," +
                    "  mch_no VARCHAR(64) NOT NULL," +
                    "  app_id VARCHAR(64) NOT NULL," +
                    "  original_if_code VARCHAR(20) NOT NULL," +
                    "  compensation_if_code VARCHAR(20) NOT NULL," +
                    "  amount BIGINT NOT NULL," +
                    "  state SMALLINT NOT NULL DEFAULT 0," +
                    "  result_info VARCHAR(256)," +
                    "  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                    "  updated_at TIMESTAMP," +
                    "  CONSTRAINT uk_t_pay_order_compensation_pay_order_id UNIQUE (pay_order_id)" +
                    ");";
                
                jdbcTemplate.execute(createTableSql);
                
                // 创建索引
                String createIndexSql = 
                    "CREATE INDEX idx_t_pay_order_compensation_created_at " +
                    "ON t_pay_order_compensation(created_at);";
                
                jdbcTemplate.execute(createIndexSql);
                
                // 添加表注释
                String tableCommentSql = 
                    "COMMENT ON TABLE t_pay_order_compensation IS '支付订单补偿记录';";
                
                jdbcTemplate.execute(tableCommentSql);
                
                // 添加列注释
                String[] columnCommentsSql = {
                    "COMMENT ON COLUMN t_pay_order_compensation.compensation_id IS '补偿记录ID';",
                    "COMMENT ON COLUMN t_pay_order_compensation.pay_order_id IS '支付订单号';",
                    "COMMENT ON COLUMN t_pay_order_compensation.mch_no IS '商户号';",
                    "COMMENT ON COLUMN t_pay_order_compensation.app_id IS '应用ID';",
                    "COMMENT ON COLUMN t_pay_order_compensation.original_if_code IS '原支付接口代码';",
                    "COMMENT ON COLUMN t_pay_order_compensation.compensation_if_code IS '补偿支付接口代码';",
                    "COMMENT ON COLUMN t_pay_order_compensation.amount IS '订单金额';",
                    "COMMENT ON COLUMN t_pay_order_compensation.state IS '状态: 0-处理中, 1-成功, 2-失败';",
                    "COMMENT ON COLUMN t_pay_order_compensation.result_info IS '补偿结果信息';",
                    "COMMENT ON COLUMN t_pay_order_compensation.created_at IS '创建时间';",
                    "COMMENT ON COLUMN t_pay_order_compensation.updated_at IS '更新时间';"
                };
                
                for (String commentSql : columnCommentsSql) {
                    jdbcTemplate.execute(commentSql);
                }
                
                // 添加触发器
                String createTriggerSql = 
                    "DO $$ " +
                    "BEGIN " +
                    "  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_t_pay_order_compensation_update_timestamp') THEN " +
                    "    CREATE TRIGGER trigger_t_pay_order_compensation_update_timestamp " +
                    "    BEFORE UPDATE ON t_pay_order_compensation " +
                    "    FOR EACH ROW " +
                    "    EXECUTE FUNCTION update_timestamp(); " +
                    "  END IF; " +
                    "END $$;";
                
                jdbcTemplate.execute(createTriggerSql);
                
                log.info("PostgreSQL中支付订单补偿表创建完成");
            } else {
                log.info("PostgreSQL中支付订单补偿表已存在");
            }
        } catch (Exception e) {
            log.error("检查或创建PostgreSQL中支付订单补偿表时出错", e);
        }
    }
    
    /**
     * 确保更新时间戳函数存在
     */
    private void ensureUpdateTimestampFunctionExists(JdbcTemplate jdbcTemplate) {
        try {
            // 检查函数是否存在
            String checkFunctionSql = 
                "SELECT EXISTS (" +
                "   SELECT FROM pg_proc " +
                "   WHERE proname = 'update_timestamp'" +
                ");";
            
            Boolean functionExists = jdbcTemplate.queryForObject(checkFunctionSql, Boolean.class);
            
            if (functionExists == null || !functionExists) {
                log.info("PostgreSQL中update_timestamp函数不存在，正在创建...");
                
                // 创建函数
                String createFunctionSql = 
                    "CREATE OR REPLACE FUNCTION update_timestamp() " +
                    "RETURNS TRIGGER AS $$ " +
                    "BEGIN " +
                    "    NEW.update_time = CURRENT_TIMESTAMP; " +
                    "    RETURN NEW; " +
                    "END; " +
                    "$$ LANGUAGE plpgsql;";
                
                jdbcTemplate.execute(createFunctionSql);
                
                log.info("PostgreSQL中update_timestamp函数创建完成");
            } else {
                log.debug("PostgreSQL中update_timestamp函数已存在");
            }
        } catch (Exception e) {
            log.error("检查或创建PostgreSQL中update_timestamp函数时出错", e);
            // 继续执行，不阻断表创建过程
        }
    }
    
    /**
     * 初始化最后同步时间
     * 对于每个配置的表，设置初始同步时间为当前时间减去配置的天数
     */
    private void initializeLastSyncTimes() {
        LocalDateTime initialTime = LocalDateTime.now().minusDays(syncProperties.getInitialDays());
        
        // 确保我们有所有需要同步的表
        List<String> requiredTables = List.of("t_pay_order", "payment_records", "payment_compensation_records", "t_pay_order_compensation");
        
        // 添加配置表
        for (SyncTableConfig table : syncProperties.getTables()) {
            lastSyncTimeMap.put(table.getName(), initialTime);
            log.info("初始化表 {} 的同步起始时间: {}", table.getName(), initialTime);
        }
        
        // 确保所有必需的表都被初始化
        for (String tableName : requiredTables) {
            if (!lastSyncTimeMap.containsKey(tableName)) {
                lastSyncTimeMap.put(tableName, initialTime);
                log.info("添加默认表 {} 的同步起始时间: {}", tableName, initialTime);
            }
        }
    }
    
    /**
     * 执行初始同步
     * 系统启动时进行一次完整同步
     */
    private void performInitialSync() {
        log.info("开始执行初始数据同步...");
        
        // 同步配置的表
        for (SyncTableConfig table : syncProperties.getTables()) {
            try {
                syncTable(table);
            } catch (Exception e) {
                log.error("初始同步表 {} 失败", table.getName(), e);
            }
        }
        
        // 确保同步补偿记录表
        if (lastSyncTimeMap.containsKey("payment_compensation_records")) {
            try {
                SyncTableConfig config = new SyncTableConfig();
                config.setName("payment_compensation_records");
                config.setPrimaryKey("id");
                config.setLastUpdateColumn("update_time");
                syncTable(config);
            } catch (Exception e) {
                log.error("初始同步表 payment_compensation_records 失败", e);
            }
        }
        
        // 确保同步支付订单补偿表
        if (lastSyncTimeMap.containsKey("t_pay_order_compensation")) {
            try {
                SyncTableConfig config = new SyncTableConfig();
                config.setName("t_pay_order_compensation");
                config.setPrimaryKey("compensation_id");
                config.setLastUpdateColumn("updated_at");
                syncTable(config);
            } catch (Exception e) {
                log.error("初始同步表 t_pay_order_compensation 失败", e);
            }
        }
        
        log.info("初始数据同步完成");
    }
    
    /**
     * 同步单个表的数据
     * @param tableConfig 表配置信息
     */
    @Transactional(rollbackFor = Exception.class)
    public void syncTable(SyncTableConfig tableConfig) {
        String tableName = tableConfig.getName();
        LocalDateTime lastSyncTime = lastSyncTimeMap.get(tableName);
        
        log.debug("开始同步表 {}, 上次同步时间: {}", tableName, lastSyncTime);
        
        try {
            if ("t_pay_order".equals(tableName)) {
                syncPayOrders(lastSyncTime);
            } else if ("payment_records".equals(tableName)) {
                syncPaymentRecords(lastSyncTime);
            } else if ("payment_compensation_records".equals(tableName)) {
                syncPaymentCompensationRecords(lastSyncTime);
            } else if ("t_pay_order_compensation".equals(tableName)) {
                syncPayOrderCompensations(lastSyncTime);
            } else {
                log.warn("未知的表名: {}, 跳过同步", tableName);
            }
        } catch (Exception e) {
            // 特殊处理 "relation does not exist" 错误
            String errorMsg = e.getMessage();
            if (errorMsg != null && errorMsg.contains("relation") && errorMsg.contains("does not exist")) {
                log.warn("表 {} 不存在，尝试创建表...", tableName);
                
                // 尝试创建表
                if ("t_pay_order_compensation".equals(tableName)) {
                    try {
                        ensureCompensationTableExists();
                        log.info("表 {} 创建完成，重新尝试同步...", tableName);
                        
                        // 重新尝试同步
                        if ("t_pay_order_compensation".equals(tableName)) {
                            syncPayOrderCompensations(lastSyncTime);
                        }
                    } catch (Exception ex) {
                        log.error("创建表 {} 失败", tableName, ex);
                    }
                }
            } else {
                log.error("同步表 {} 时发生错误", tableName, e);
            }
        }
    }
    
    /**
     * 同步支付订单表
     * @param lastSyncTime 上次同步时间
     */
    private void syncPayOrders(LocalDateTime lastSyncTime) {
        // 最新的同步时间，将在处理完成后更新
        LocalDateTime latestSyncTime = lastSyncTime;
        
        // 分页查询更新过的订单
        int pageSize = syncProperties.getBatchSize();
        long pageIndex = 1;
        boolean hasMore = true;
        
        while (hasMore) {
            // 查询源数据库中的更新记录
            Page<PayOrder> page = new Page<>(pageIndex, pageSize);
            LambdaQueryWrapper<PayOrder> queryWrapper = new LambdaQueryWrapper<>();
            queryWrapper.gt(PayOrder::getUpdatedAt, java.sql.Timestamp.valueOf(lastSyncTime))
                        .orderByAsc(PayOrder::getUpdatedAt);
            
            page = payOrderService.page(page, queryWrapper);
            List<PayOrder> records = page.getRecords();
            
            if (records.isEmpty()) {
                hasMore = false;
                continue;
            }
            
            // 处理每一条记录
            for (PayOrder payOrder : records) {
                try {
                    // 检查该记录在目标数据库是否存在
                    PayOrder existingOrder = reconciliationPayOrderMapper.selectById(payOrder.getPayOrderId());
                    
                    if (existingOrder == null) {
                        // 插入新记录
                        reconciliationPayOrderMapper.insert(payOrder);
                    } else {
                        // 更新已有记录
                        reconciliationPayOrderMapper.updateById(payOrder);
                    }
                    
                    // 更新最新同步时间
                    if (payOrder.getUpdatedAt() != null) {
                        LocalDateTime recordTime = payOrder.getUpdatedAt().toInstant()
                            .atZone(java.time.ZoneId.systemDefault())
                            .toLocalDateTime();
                        
                        if (recordTime.isAfter(latestSyncTime)) {
                            latestSyncTime = recordTime;
                        }
                    }
                } catch (Exception e) {
                    log.error("同步支付订单记录失败: {}", payOrder.getPayOrderId(), e);
                }
            }
            
            // 判断是否需要继续查询下一页
            hasMore = records.size() == pageSize;
            pageIndex++;
            
            log.debug("已同步支付订单表的第 {} 页数据，共 {} 条记录", pageIndex - 1, records.size());
        }
        
        // 更新最后同步时间
        lastSyncTimeMap.put("t_pay_order", latestSyncTime);
        log.info("完成同步支付订单表，最新同步时间更新为: {}", latestSyncTime);
    }
    
    /**
     * 同步支付记录表
     * @param lastSyncTime 上次同步时间
     */
    private void syncPaymentRecords(LocalDateTime lastSyncTime) {
        // 最新的同步时间，将在处理完成后更新
        LocalDateTime latestSyncTime = lastSyncTime;
        
        // 分页查询更新过的支付记录
        int pageSize = syncProperties.getBatchSize();
        long pageIndex = 1;
        boolean hasMore = true;
        
        while (hasMore) {
            // 查询源数据库中的更新记录
            Page<PaymentRecord> page = new Page<>(pageIndex, pageSize);
            LambdaQueryWrapper<PaymentRecord> queryWrapper = new LambdaQueryWrapper<>();
            queryWrapper.gt(PaymentRecord::getUpdateTime, java.sql.Timestamp.valueOf(lastSyncTime))
                       .orderByAsc(PaymentRecord::getUpdateTime);
            
            page = paymentRecordService.page(page, queryWrapper);
            List<PaymentRecord> records = page.getRecords();
            
            if (records.isEmpty()) {
                hasMore = false;
                continue;
            }
            
            // 处理每一条记录
            for (PaymentRecord record : records) {
                try {
                    // 检查该记录在目标数据库是否存在
                    PaymentRecord existingRecord = null;
                    LambdaQueryWrapper<PaymentRecord> existsWrapper = new LambdaQueryWrapper<>();
                    existsWrapper.eq(PaymentRecord::getOrderNo, record.getOrderNo());
                    existingRecord = reconciliationPaymentRecordMapper.selectOne(existsWrapper);
                    
                    if (existingRecord == null) {
                        // 插入新记录
                        reconciliationPaymentRecordMapper.insert(record);
                    } else {
                        // 更新已有记录
                        record.setId(existingRecord.getId()); // 确保ID正确
                        reconciliationPaymentRecordMapper.updateById(record);
                    }
                    
                    // 更新最新同步时间
                    if (record.getUpdateTime() != null) {
                        LocalDateTime recordTime = record.getUpdateTime().toInstant()
                            .atZone(java.time.ZoneId.systemDefault())
                            .toLocalDateTime();
                        
                        if (recordTime.isAfter(latestSyncTime)) {
                            latestSyncTime = recordTime;
                        }
                    }
                } catch (Exception e) {
                    log.error("同步支付记录失败: {}", record.getOrderNo(), e);
                }
            }
            
            // 判断是否需要继续查询下一页
            hasMore = records.size() == pageSize;
            pageIndex++;
            
            log.debug("已同步支付记录表的第 {} 页数据，共 {} 条记录", pageIndex - 1, records.size());
        }
        
        // 更新最后同步时间
        lastSyncTimeMap.put("payment_records", latestSyncTime);
        log.info("完成同步支付记录表，最新同步时间更新为: {}", latestSyncTime);
    }
    
    /**
     * 同步支付补偿记录表
     * @param lastSyncTime 上次同步时间
     */
    private void syncPaymentCompensationRecords(LocalDateTime lastSyncTime) {
        // 最新的同步时间，将在处理完成后更新
        LocalDateTime latestSyncTime = lastSyncTime;
        
        // 分页查询更新过的支付补偿记录
        int pageSize = syncProperties.getBatchSize();
        long pageIndex = 1;
        boolean hasMore = true;
        
        while (hasMore) {
            // 查询源数据库中的更新记录
            Page<PaymentCompensationRecord> page = new Page<>(pageIndex, pageSize);
            LambdaQueryWrapper<PaymentCompensationRecord> queryWrapper = new LambdaQueryWrapper<>();
            queryWrapper.gt(PaymentCompensationRecord::getUpdateTime, java.sql.Timestamp.valueOf(lastSyncTime))
                       .orderByAsc(PaymentCompensationRecord::getUpdateTime);
            
            page = paymentCompensationRecordService.page(page, queryWrapper);
            List<PaymentCompensationRecord> records = page.getRecords();
            
            if (records.isEmpty()) {
                hasMore = false;
                continue;
            }
            
            // 处理每一条记录
            for (PaymentCompensationRecord record : records) {
                try {
                    // 检查该记录在目标数据库是否存在
                    PaymentCompensationRecord existingRecord = null;
                    LambdaQueryWrapper<PaymentCompensationRecord> existsWrapper = new LambdaQueryWrapper<>();
                    existsWrapper.eq(PaymentCompensationRecord::getOrderNo, record.getOrderNo());
                    existingRecord = reconciliationPaymentCompensationRecordMapper.selectOne(existsWrapper);
                    
                    if (existingRecord == null) {
                        // 插入新记录
                        reconciliationPaymentCompensationRecordMapper.insert(record);
                    } else {
                        // 更新已有记录
                        record.setId(existingRecord.getId()); // 确保ID正确
                        reconciliationPaymentCompensationRecordMapper.updateById(record);
                    }
                    
                    // 更新最新同步时间
                    if (record.getUpdateTime() != null) {
                        LocalDateTime recordTime = record.getUpdateTime().toInstant()
                            .atZone(java.time.ZoneId.systemDefault())
                            .toLocalDateTime();
                        
                        if (recordTime.isAfter(latestSyncTime)) {
                            latestSyncTime = recordTime;
                        }
                    }
                } catch (Exception e) {
                    log.error("同步支付补偿记录失败: {}", record.getOrderNo(), e);
                }
            }
            
            // 判断是否需要继续查询下一页
            hasMore = records.size() == pageSize;
            pageIndex++;
            
            log.debug("已同步支付补偿记录表的第 {} 页数据，共 {} 条记录", pageIndex - 1, records.size());
        }
        
        // 更新最后同步时间
        lastSyncTimeMap.put("payment_compensation_records", latestSyncTime);
        log.info("完成同步支付补偿记录表，最新同步时间更新为: {}", latestSyncTime);
    }
    
    /**
     * 同步支付订单补偿表
     * @param lastSyncTime 上次同步时间
     */
    private void syncPayOrderCompensations(LocalDateTime lastSyncTime) {
        // 首先确保表存在
        ensureCompensationTableExists();
        
        // 最新的同步时间，将在处理完成后更新
        LocalDateTime latestSyncTime = lastSyncTime;
        
        // 分页查询更新过的补偿记录
        int pageSize = syncProperties.getBatchSize();
        long pageIndex = 1;
        boolean hasMore = true;
        
        while (hasMore) {
            // 查询源数据库中的更新记录
            Page<PayOrderCompensation> page = new Page<>(pageIndex, pageSize);
            LambdaQueryWrapper<PayOrderCompensation> queryWrapper = new LambdaQueryWrapper<>();
            queryWrapper.gt(PayOrderCompensation::getUpdatedAt, java.sql.Timestamp.valueOf(lastSyncTime))
                        .orderByAsc(PayOrderCompensation::getUpdatedAt);
            
            page = payOrderCompensationService.page(page, queryWrapper);
            List<PayOrderCompensation> records = page.getRecords();
            
            if (records.isEmpty()) {
                hasMore = false;
                continue;
            }
            
            // 处理每一条记录
            for (PayOrderCompensation compensation : records) {
                try {
                    // 创建SQL语句手动同步数据
                    JdbcTemplate jdbcTemplate = reconciliationJdbcTemplate;
                    
                    // 检查记录是否存在
                    String checkSql = "SELECT COUNT(*) FROM t_pay_order_compensation WHERE compensation_id = ?";
                    int count = jdbcTemplate.queryForObject(checkSql, Integer.class, compensation.getCompensationId());
                    
                    if (count > 0) {
                        // 更新记录
                        String updateSql = "UPDATE t_pay_order_compensation SET " +
                                "pay_order_id = ?, mch_no = ?, app_id = ?, " +
                                "original_if_code = ?, compensation_if_code = ?, " +
                                "amount = ?, state = ?, result_info = ?, " +
                                "created_at = ?, updated_at = ? " +
                                "WHERE compensation_id = ?";
                        
                        jdbcTemplate.update(updateSql,
                                compensation.getPayOrderId(),
                                compensation.getMchNo(),
                                compensation.getAppId(),
                                compensation.getOriginalIfCode(),
                                compensation.getCompensationIfCode(),
                                compensation.getAmount(),
                                compensation.getState(),
                                compensation.getResultInfo(),
                                compensation.getCreatedAt(),
                                compensation.getUpdatedAt(),
                                compensation.getCompensationId());
                    } else {
                        // 插入记录
                        String insertSql = "INSERT INTO t_pay_order_compensation " +
                                "(compensation_id, pay_order_id, mch_no, app_id, " +
                                "original_if_code, compensation_if_code, " +
                                "amount, state, result_info, created_at, updated_at) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                        
                        jdbcTemplate.update(insertSql,
                                compensation.getCompensationId(),
                                compensation.getPayOrderId(),
                                compensation.getMchNo(),
                                compensation.getAppId(),
                                compensation.getOriginalIfCode(),
                                compensation.getCompensationIfCode(),
                                compensation.getAmount(),
                                compensation.getState(),
                                compensation.getResultInfo(),
                                compensation.getCreatedAt(),
                                compensation.getUpdatedAt());
                    }
                    
                    // 更新最新同步时间
                    if (compensation.getUpdatedAt() != null) {
                        LocalDateTime recordTime = compensation.getUpdatedAt().toInstant()
                                .atZone(java.time.ZoneId.systemDefault())
                                .toLocalDateTime();
                        
                        if (recordTime.isAfter(latestSyncTime)) {
                            latestSyncTime = recordTime;
                        }
                    }
                } catch (Exception e) {
                    log.error("同步支付订单补偿记录失败: {}", compensation.getCompensationId(), e);
                }
            }
            
            // 判断是否需要继续查询下一页
            hasMore = records.size() == pageSize;
            pageIndex++;
        }
        
        // 更新最后同步时间
        if (latestSyncTime.isAfter(lastSyncTime)) {
            lastSyncTimeMap.put("t_pay_order_compensation", latestSyncTime.plusNanos(1000000)); // 加1毫秒，避免边界问题
            log.debug("更新表 t_pay_order_compensation 的同步时间为: {}", lastSyncTimeMap.get("t_pay_order_compensation"));
        }
    }
    
    /**
     * 执行同步所有表
     * 可以由定时任务或手动触发
     */
    public void syncAllTables() {
        log.info("开始执行所有表的同步任务...");
        
        // 定义必须要同步的表
        List<String> requiredTables = List.of("t_pay_order", "payment_records", "payment_compensation_records", "t_pay_order_compensation");
        
        // 同步配置的表
        for (SyncTableConfig table : syncProperties.getTables()) {
            try {
                syncTable(table);
            } catch (Exception e) {
                log.error("同步表 {} 失败", table.getName(), e);
            }
        }
        
        // 检查并同步必需的表
        for (String tableName : requiredTables) {
            if (lastSyncTimeMap.containsKey(tableName)) {
                boolean found = false;
                for (SyncTableConfig table : syncProperties.getTables()) {
                    if (table.getName().equals(tableName)) {
                        found = true;
                        break;
                    }
                }
                
                if (!found) {
                    try {
                        SyncTableConfig config = new SyncTableConfig();
                        config.setName(tableName);
                        
                        if ("t_pay_order".equals(tableName)) {
                            config.setPrimaryKey("pay_order_id");
                            config.setLastUpdateColumn("updated_at");
                        } else {
                            config.setPrimaryKey("id");
                            config.setLastUpdateColumn("update_time");
                        }
                        
                        syncTable(config);
                    } catch (Exception e) {
                        log.error("同步必需表 {} 失败", tableName, e);
                    }
                }
            }
        }
        
        log.info("所有表同步完成");
    }
    
    /**
     * 同步配置属性类
     */
    @Data
    @ConfigurationProperties(prefix = "jeepay.sync")
    public static class SyncProperties {
        // 初始同步天数
        private int initialDays = 7;
        
        // 同步间隔（毫秒）
        private long interval = 300000;
        
        // 同步批次大小
        private int batchSize = 500;
        
        // 同步表配置
        private List<SyncTableConfig> tables;
    }
    
    /**
     * 表同步配置
     */
    @Data
    public static class SyncTableConfig {
        // 表名
        private String name;
        
        // 主键列名
        private String primaryKey;
        
        // 最后更新时间列名
        private String lastUpdateColumn;
    }
} 