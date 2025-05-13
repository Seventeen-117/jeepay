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
package com.jeequan.jeepay.pay.config;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 数据库初始化脚本加载器
 * 根据数据库类型自动加载对应的初始化脚本
 *
 * @author jiangyangpay
 * @site curverun.com
 * @date 2023/9/15
 */
@Component
@Slf4j
public class DatabaseSchemaInitializer {

    @Autowired
    private DataSource dataSource;
    
    @Value("${spring.datasource.dynamic.primary:mysql}")
    private String primaryDataSource;

    /**
     * 应用启动时自动加载数据库初始化脚本
     */
    @PostConstruct
    public void init() {
        try {
            if (isPostgreSQLDatabase()) {
                loadPostgreSQLSchema();
                // 确保关键表存在
                ensureRefundOrderTableExists();
                ensurePayOrderDivisionRecordTableExists();
                ensureTransferOrderTableExists();
            } else {
                loadMySQLSchema();
            }
        } catch (Exception e) {
            log.error("加载数据库初始化脚本失败", e);
        }
    }

    /**
     * 判断当前使用的数据库类型
     * @return true如果是PostgreSQL，false如果是MySQL或其他数据库
     */
    private boolean isPostgreSQLDatabase() {
        // 如果明确配置了使用PostgreSQL作为主数据源
        if ("postgres".equalsIgnoreCase(primaryDataSource)) {
            return true;
        }
        
        // 通过检查数据库连接信息来判断
        try (Connection connection = dataSource.getConnection()) {
            String dbProductName = connection.getMetaData().getDatabaseProductName().toLowerCase();
            return dbProductName.contains("postgresql");
        } catch (SQLException e) {
            log.error("获取数据库类型出错", e);
            // 默认返回false，使用MySQL模式
            return false;
        }
    }

    /**
     * 加载PostgreSQL初始化脚本
     */
    private void loadPostgreSQLSchema() {
        log.info("加载PostgreSQL初始化脚本...");
        try {
            // 首先创建表
            ResourceDatabasePopulator tableCreator = new ResourceDatabasePopulator();
            tableCreator.addScript(new ClassPathResource("sql/postgresql_reconciliation_schema.sql"));
            tableCreator.setIgnoreFailedDrops(true);
            tableCreator.setContinueOnError(true);
            tableCreator.execute(dataSource);
            log.info("PostgreSQL表创建完成");
            
            // 确保补偿记录表存在
            ensurePayOrderCompensationTableExists();
            
            // 等待一秒确保表创建完成
            try { Thread.sleep(1000); } catch (InterruptedException e) { /* ignore */ }
            
            // 然后创建索引
            ResourceDatabasePopulator indexCreator = new ResourceDatabasePopulator();
            indexCreator.addScript(new ClassPathResource("sql/postgresql_reconciliation_indexes.sql"));
            indexCreator.setIgnoreFailedDrops(true);
            indexCreator.setContinueOnError(true);
            indexCreator.execute(dataSource);
            log.info("PostgreSQL索引创建完成");
            
            // 刷新物化视图
            try (Connection conn = dataSource.getConnection();
                 java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT refresh_payment_reconciliation()");
                log.info("PostgreSQL物化视图刷新完成");
            } catch (SQLException e) {
                log.warn("刷新物化视图失败，可能是首次运行: {}", e.getMessage());
            }
        } catch (Exception e) {
            log.error("加载PostgreSQL初始化脚本失败", e);
        }
    }

    /**
     * 确保退款订单表存在
     * 单独检查并创建退款订单表，确保不会出现"relation does not exist"错误
     */
    private void ensureRefundOrderTableExists() {
        try (Connection conn = dataSource.getConnection();
             java.sql.Statement stmt = conn.createStatement()) {
            
            // 首先检查表是否存在
            String checkTableSql = 
                "SELECT EXISTS (" +
                "   SELECT FROM pg_tables " +
                "   WHERE schemaname = 'public' " +
                "   AND tablename = 't_refund_order'" +
                ");";
            
            ResultSet rs = stmt.executeQuery(checkTableSql);
            boolean tableExists = false;
            if (rs.next()) {
                tableExists = rs.getBoolean(1);
            }
            rs.close();
            
            if (!tableExists) {
                log.info("退款订单表不存在，正在创建...");
                
                // 创建表的SQL语句
                String createTableSql = 
                    "CREATE TABLE t_refund_order (" +
                    "  refund_order_id VARCHAR(30) PRIMARY KEY," +
                    "  pay_order_id VARCHAR(30) NOT NULL," +
                    "  channel_pay_order_no VARCHAR(64)," +
                    "  mch_no VARCHAR(64) NOT NULL," +
                    "  isv_no VARCHAR(64)," +
                    "  app_id VARCHAR(64) NOT NULL," +
                    "  mch_name VARCHAR(30) NOT NULL," +
                    "  mch_type SMALLINT NOT NULL," +
                    "  mch_refund_no VARCHAR(64) NOT NULL," +
                    "  way_code VARCHAR(20) NOT NULL," +
                    "  if_code VARCHAR(20)," +
                    "  pay_amount BIGINT NOT NULL," +
                    "  refund_amount BIGINT NOT NULL," +
                    "  currency VARCHAR(3) NOT NULL DEFAULT 'cny'," +
                    "  state SMALLINT NOT NULL DEFAULT 0," +
                    "  client_ip VARCHAR(32)," +
                    "  refund_reason VARCHAR(128)," +
                    "  channel_order_no VARCHAR(64)," +
                    "  err_code VARCHAR(128)," +
                    "  err_msg VARCHAR(2048)," +
                    "  channel_extra VARCHAR(512)," +
                    "  notify_url VARCHAR(128)," +
                    "  ext_param VARCHAR(128)," +
                    "  success_time TIMESTAMP," +
                    "  expired_time TIMESTAMP," +
                    "  created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                    "  updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                    "  CONSTRAINT uk_mch_no_mch_refund_no UNIQUE (mch_no, mch_refund_no)" +
                    ");";
                
                stmt.execute(createTableSql);
                
                // 创建索引
                String[] createIndexSqls = {
                    "CREATE INDEX idx_t_refund_order_created_at ON t_refund_order(created_at);",
                    "CREATE INDEX idx_t_refund_order_pay_order_id ON t_refund_order(pay_order_id);"
                };
                
                for (String indexSql : createIndexSqls) {
                    stmt.execute(indexSql);
                }
                
                // 添加表注释
                String tableCommentSql = 
                    "COMMENT ON TABLE t_refund_order IS '退款订单表';";
                
                stmt.execute(tableCommentSql);
                
                // 添加列注释
                String[] columnCommentsSql = {
                    "COMMENT ON COLUMN t_refund_order.refund_order_id IS '退款订单号';",
                    "COMMENT ON COLUMN t_refund_order.pay_order_id IS '支付订单号';",
                    "COMMENT ON COLUMN t_refund_order.channel_pay_order_no IS '渠道支付单号';",
                    "COMMENT ON COLUMN t_refund_order.mch_no IS '商户号';",
                    "COMMENT ON COLUMN t_refund_order.isv_no IS '服务商号';",
                    "COMMENT ON COLUMN t_refund_order.app_id IS '应用ID';",
                    "COMMENT ON COLUMN t_refund_order.mch_name IS '商户名称';",
                    "COMMENT ON COLUMN t_refund_order.mch_type IS '类型:1-普通商户,2-特约商户(服务商模式)';",
                    "COMMENT ON COLUMN t_refund_order.mch_refund_no IS '商户退款单号';",
                    "COMMENT ON COLUMN t_refund_order.way_code IS '支付方式代码';",
                    "COMMENT ON COLUMN t_refund_order.if_code IS '支付接口代码';",
                    "COMMENT ON COLUMN t_refund_order.pay_amount IS '支付金额,单位分';",
                    "COMMENT ON COLUMN t_refund_order.refund_amount IS '退款金额,单位分';",
                    "COMMENT ON COLUMN t_refund_order.currency IS '三位货币代码,人民币:cny';",
                    "COMMENT ON COLUMN t_refund_order.state IS '退款状态:0-订单生成,1-退款中,2-退款成功,3-退款失败,4-退款任务关闭';",
                    "COMMENT ON COLUMN t_refund_order.client_ip IS '客户端IP';",
                    "COMMENT ON COLUMN t_refund_order.refund_reason IS '退款原因';",
                    "COMMENT ON COLUMN t_refund_order.channel_order_no IS '渠道订单号';",
                    "COMMENT ON COLUMN t_refund_order.err_code IS '渠道错误码';",
                    "COMMENT ON COLUMN t_refund_order.err_msg IS '渠道错误描述';",
                    "COMMENT ON COLUMN t_refund_order.channel_extra IS '特定渠道发起额外参数';",
                    "COMMENT ON COLUMN t_refund_order.notify_url IS '通知地址';",
                    "COMMENT ON COLUMN t_refund_order.ext_param IS '扩展参数';",
                    "COMMENT ON COLUMN t_refund_order.success_time IS '订单退款成功时间';",
                    "COMMENT ON COLUMN t_refund_order.expired_time IS '退款失效时间';",
                    "COMMENT ON COLUMN t_refund_order.created_at IS '创建时间';",
                    "COMMENT ON COLUMN t_refund_order.updated_at IS '更新时间';"
                };
                
                for (String commentSql : columnCommentsSql) {
                    stmt.execute(commentSql);
                }
                
                // 添加触发器
                String createTriggerSql = 
                    "DO $$ " +
                    "BEGIN " +
                    "  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_t_refund_order_update_timestamp') THEN " +
                    "    CREATE TRIGGER trigger_t_refund_order_update_timestamp " +
                    "    BEFORE UPDATE ON t_refund_order " +
                    "    FOR EACH ROW " +
                    "    EXECUTE FUNCTION update_timestamp(); " +
                    "  END IF; " +
                    "END $$;";
                
                stmt.execute(createTriggerSql);
                
                log.info("退款订单表创建完成");
            } else {
                log.debug("退款订单表已存在");
            }
            
        } catch (SQLException e) {
            log.error("创建退款订单表失败", e);
        }
    }

    /**
     * 确保分账记录表存在
     * 单独检查并创建分账记录表，确保不会出现"relation does not exist"错误
     */
    private void ensurePayOrderDivisionRecordTableExists() {
        try (Connection conn = dataSource.getConnection();
             java.sql.Statement stmt = conn.createStatement()) {
            
            // 首先检查表是否存在
            String checkTableSql = 
                "SELECT EXISTS (" +
                "   SELECT FROM pg_tables " +
                "   WHERE schemaname = 'public' " +
                "   AND tablename = 't_pay_order_division_record'" +
                ");";
            
            ResultSet rs = stmt.executeQuery(checkTableSql);
            boolean tableExists = false;
            if (rs.next()) {
                tableExists = rs.getBoolean(1);
            }
            rs.close();
            
            if (!tableExists) {
                log.info("分账记录表不存在，正在创建...");
                
                // 创建表的SQL语句
                String createTableSql = 
                    "CREATE TABLE t_pay_order_division_record (" +
                    "  record_id BIGSERIAL PRIMARY KEY," +
                    "  mch_no VARCHAR(64) NOT NULL," +
                    "  isv_no VARCHAR(64)," +
                    "  app_id VARCHAR(64) NOT NULL," +
                    "  mch_name VARCHAR(30) NOT NULL," +
                    "  mch_type SMALLINT NOT NULL," +
                    "  if_code VARCHAR(20) NOT NULL," +
                    "  pay_order_id VARCHAR(30) NOT NULL," +
                    "  pay_order_channel_order_no VARCHAR(64)," +
                    "  pay_order_amount BIGINT NOT NULL," +
                    "  pay_order_division_amount BIGINT NOT NULL," +
                    "  batch_order_id VARCHAR(30) NOT NULL," +
                    "  channel_batch_order_id VARCHAR(64)," +
                    "  state SMALLINT NOT NULL," +
                    "  channel_resp_result TEXT," +
                    "  receiver_id BIGINT NOT NULL," +
                    "  receiver_group_id BIGINT," +
                    "  receiver_alias VARCHAR(64)," +
                    "  acc_type SMALLINT NOT NULL," +
                    "  acc_no VARCHAR(50) NOT NULL," +
                    "  acc_name VARCHAR(30) NOT NULL DEFAULT ''," +
                    "  relation_type VARCHAR(30) NOT NULL," +
                    "  relation_type_name VARCHAR(30) NOT NULL," +
                    "  division_profit NUMERIC(20,6) NOT NULL," +
                    "  cal_division_amount BIGINT NOT NULL," +
                    "  created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                    "  updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP" +
                    ");";
                
                stmt.execute(createTableSql);
                
                // 创建索引
                String[] createIndexSqls = {
                    "CREATE INDEX idx_t_pay_order_division_record_created_at ON t_pay_order_division_record(created_at);",
                    "CREATE INDEX idx_t_pay_order_division_record_pay_order_id ON t_pay_order_division_record(pay_order_id);",
                    "CREATE INDEX idx_t_pay_order_division_record_batch_order_id ON t_pay_order_division_record(batch_order_id);"
                };
                
                for (String indexSql : createIndexSqls) {
                    stmt.execute(indexSql);
                }
                
                // 添加表注释
                String tableCommentSql = 
                    "COMMENT ON TABLE t_pay_order_division_record IS '分账记录表';";
                
                stmt.execute(tableCommentSql);
                
                // 添加列注释
                String[] columnCommentsSql = {
                    "COMMENT ON COLUMN t_pay_order_division_record.record_id IS '分账记录ID';",
                    "COMMENT ON COLUMN t_pay_order_division_record.mch_no IS '商户号';",
                    "COMMENT ON COLUMN t_pay_order_division_record.isv_no IS '服务商号';",
                    "COMMENT ON COLUMN t_pay_order_division_record.app_id IS '应用ID';",
                    "COMMENT ON COLUMN t_pay_order_division_record.mch_name IS '商户名称';",
                    "COMMENT ON COLUMN t_pay_order_division_record.mch_type IS '类型: 1-普通商户, 2-特约商户(服务商模式)';",
                    "COMMENT ON COLUMN t_pay_order_division_record.if_code IS '支付接口代码';",
                    "COMMENT ON COLUMN t_pay_order_division_record.pay_order_id IS '系统支付订单号';",
                    "COMMENT ON COLUMN t_pay_order_division_record.pay_order_channel_order_no IS '支付订单渠道支付订单号';",
                    "COMMENT ON COLUMN t_pay_order_division_record.pay_order_amount IS '订单金额,单位分';",
                    "COMMENT ON COLUMN t_pay_order_division_record.pay_order_division_amount IS '订单实际分账金额, 单位：分（订单金额 - 商户手续费 - 已退款金额）';",
                    "COMMENT ON COLUMN t_pay_order_division_record.batch_order_id IS '系统分账批次号';",
                    "COMMENT ON COLUMN t_pay_order_division_record.channel_batch_order_id IS '上游分账批次号';",
                    "COMMENT ON COLUMN t_pay_order_division_record.state IS '状态: 0-待分账 1-分账成功（明确成功）, 2-分账失败（明确失败）, 3-分账已受理（上游受理）';",
                    "COMMENT ON COLUMN t_pay_order_division_record.channel_resp_result IS '上游返回数据包';",
                    "COMMENT ON COLUMN t_pay_order_division_record.receiver_id IS '账号快照》 分账接收者ID';",
                    "COMMENT ON COLUMN t_pay_order_division_record.receiver_group_id IS '账号快照》 组ID（便于商户接口使用）';",
                    "COMMENT ON COLUMN t_pay_order_division_record.receiver_alias IS '接收者账号别名';",
                    "COMMENT ON COLUMN t_pay_order_division_record.acc_type IS '账号快照》 分账接收账号类型: 0-个人 1-商户';",
                    "COMMENT ON COLUMN t_pay_order_division_record.acc_no IS '账号快照》 分账接收账号';",
                    "COMMENT ON COLUMN t_pay_order_division_record.acc_name IS '账号快照》 分账接收账号名称';",
                    "COMMENT ON COLUMN t_pay_order_division_record.relation_type IS '账号快照》 分账关系类型（参考微信）， 如： SERVICE_PROVIDER 服务商等';",
                    "COMMENT ON COLUMN t_pay_order_division_record.relation_type_name IS '账号快照》 当选择自定义时，需要录入该字段。 否则为对应的名称';",
                    "COMMENT ON COLUMN t_pay_order_division_record.division_profit IS '账号快照》 配置的实际分账比例';",
                    "COMMENT ON COLUMN t_pay_order_division_record.cal_division_amount IS '计算该接收方的分账金额,单位分';",
                    "COMMENT ON COLUMN t_pay_order_division_record.created_at IS '创建时间';",
                    "COMMENT ON COLUMN t_pay_order_division_record.updated_at IS '更新时间';"
                };
                
                for (String commentSql : columnCommentsSql) {
                    stmt.execute(commentSql);
                }
                
                // 添加触发器
                String createTriggerSql = 
                    "DO $$ " +
                    "BEGIN " +
                    "  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_t_pay_order_division_record_update_timestamp') THEN " +
                    "    CREATE TRIGGER trigger_t_pay_order_division_record_update_timestamp " +
                    "    BEFORE UPDATE ON t_pay_order_division_record " +
                    "    FOR EACH ROW " +
                    "    EXECUTE FUNCTION update_timestamp(); " +
                    "  END IF; " +
                    "END $$;";
                
                stmt.execute(createTriggerSql);
                
                log.info("分账记录表创建完成");
            } else {
                log.debug("分账记录表已存在");
            }
            
        } catch (SQLException e) {
            log.error("创建分账记录表失败", e);
        }
    }

    /**
     * 确保支付订单补偿记录表存在
     * 单独检查并创建这个表，确保不会出现"relation does not exist"错误
     */
    private void ensurePayOrderCompensationTableExists() {
        try (Connection conn = dataSource.getConnection();
             java.sql.Statement stmt = conn.createStatement()) {
            
            // 首先检查表是否存在
            String checkTableSql = 
                "SELECT EXISTS (" +
                "   SELECT FROM pg_tables " +
                "   WHERE schemaname = 'public' " +
                "   AND tablename = 't_pay_order_compensation'" +
                ");";
            
            ResultSet rs = stmt.executeQuery(checkTableSql);
            boolean tableExists = false;
            if (rs.next()) {
                tableExists = rs.getBoolean(1);
            }
            rs.close();
            
            if (!tableExists) {
                log.info("支付订单补偿记录表不存在，正在创建...");
                
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
                
                stmt.execute(createTableSql);
                
                // 创建索引
                String createIndexSql = 
                    "CREATE INDEX idx_t_pay_order_compensation_created_at " +
                    "ON t_pay_order_compensation(created_at);";
                
                stmt.execute(createIndexSql);
                
                // 添加表注释
                String tableCommentSql = 
                    "COMMENT ON TABLE t_pay_order_compensation IS '支付订单补偿记录';";
                
                stmt.execute(tableCommentSql);
                
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
                    stmt.execute(commentSql);
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
                
                stmt.execute(createTriggerSql);
                
                log.info("支付订单补偿记录表创建完成");
            } else {
                log.debug("支付订单补偿记录表已存在");
            }
            
        } catch (SQLException e) {
            log.error("创建支付订单补偿记录表失败", e);
        }
    }

    /**
     * 确保转账订单表存在
     * 单独检查并创建转账订单表，确保不会出现"relation does not exist"错误
     */
    private void ensureTransferOrderTableExists() {
        try (Connection conn = dataSource.getConnection();
             java.sql.Statement stmt = conn.createStatement()) {
            
            // 首先检查表是否存在
            String checkTableSql = 
                "SELECT EXISTS (" +
                "   SELECT FROM pg_tables " +
                "   WHERE schemaname = 'public' " +
                "   AND tablename = 't_transfer_order'" +
                ");";
            
            ResultSet rs = stmt.executeQuery(checkTableSql);
            boolean tableExists = false;
            if (rs.next()) {
                tableExists = rs.getBoolean(1);
            }
            rs.close();
            
            if (!tableExists) {
                log.info("转账订单表不存在，正在创建...");
                
                // 创建表的SQL语句
                String createTableSql = 
                    "CREATE TABLE t_transfer_order (" +
                    "  transfer_id VARCHAR(30) PRIMARY KEY," +
                    "  mch_no VARCHAR(64) NOT NULL," +
                    "  isv_no VARCHAR(64)," +
                    "  app_id VARCHAR(64) NOT NULL," +
                    "  mch_name VARCHAR(30) NOT NULL," +
                    "  mch_type SMALLINT NOT NULL," +
                    "  mch_order_no VARCHAR(64) NOT NULL," +
                    "  if_code VARCHAR(20)," +
                    "  entry_type VARCHAR(20)," +
                    "  amount BIGINT NOT NULL," +
                    "  currency VARCHAR(3) NOT NULL DEFAULT 'cny'," +
                    "  account_no VARCHAR(64)," +
                    "  account_name VARCHAR(64)," +
                    "  bank_name VARCHAR(64)," +
                    "  transfer_desc VARCHAR(128)," +
                    "  client_ip VARCHAR(32)," +
                    "  state SMALLINT NOT NULL DEFAULT 0," +
                    "  channel_extra VARCHAR(512)," +
                    "  channel_order_no VARCHAR(64)," +
                    "  channel_res_data TEXT," +
                    "  err_code VARCHAR(128)," +
                    "  err_msg VARCHAR(256)," +
                    "  ext_param VARCHAR(128)," +
                    "  notify_url VARCHAR(128)," +
                    "  success_time TIMESTAMP," +
                    "  created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                    "  updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                    "  CONSTRAINT uk_mch_no_mch_order_no_transfer UNIQUE (mch_no, mch_order_no)" +
                    ");";
                
                stmt.execute(createTableSql);
                
                // 创建索引
                String createIndexSql = 
                    "CREATE INDEX idx_t_transfer_order_created_at ON t_transfer_order(created_at);";
                
                stmt.execute(createIndexSql);
                
                // 添加表注释
                String tableCommentSql = 
                    "COMMENT ON TABLE t_transfer_order IS '转账订单表';";
                
                stmt.execute(tableCommentSql);
                
                // 添加列注释
                String[] columnCommentsSql = {
                    "COMMENT ON COLUMN t_transfer_order.transfer_id IS '转账订单号';",
                    "COMMENT ON COLUMN t_transfer_order.mch_no IS '商户号';",
                    "COMMENT ON COLUMN t_transfer_order.isv_no IS '服务商号';",
                    "COMMENT ON COLUMN t_transfer_order.app_id IS '应用ID';",
                    "COMMENT ON COLUMN t_transfer_order.mch_name IS '商户名称';",
                    "COMMENT ON COLUMN t_transfer_order.mch_type IS '类型: 1-普通商户, 2-特约商户(服务商模式)';",
                    "COMMENT ON COLUMN t_transfer_order.mch_order_no IS '商户订单号';",
                    "COMMENT ON COLUMN t_transfer_order.if_code IS '支付接口代码';",
                    "COMMENT ON COLUMN t_transfer_order.entry_type IS '入账方式： WX_CASH-微信零钱; ALIPAY_CASH-支付宝转账; BANK_CARD-银行卡';",
                    "COMMENT ON COLUMN t_transfer_order.amount IS '转账金额,单位分';",
                    "COMMENT ON COLUMN t_transfer_order.currency IS '三位货币代码,人民币:cny';",
                    "COMMENT ON COLUMN t_transfer_order.account_no IS '收款账号';",
                    "COMMENT ON COLUMN t_transfer_order.account_name IS '收款人姓名';",
                    "COMMENT ON COLUMN t_transfer_order.bank_name IS '收款人开户行名称';",
                    "COMMENT ON COLUMN t_transfer_order.transfer_desc IS '转账备注信息';",
                    "COMMENT ON COLUMN t_transfer_order.client_ip IS '客户端IP';",
                    "COMMENT ON COLUMN t_transfer_order.state IS '支付状态: 0-订单生成, 1-转账中, 2-转账成功, 3-转账失败, 4-订单关闭';",
                    "COMMENT ON COLUMN t_transfer_order.channel_extra IS '特定渠道发起额外参数';",
                    "COMMENT ON COLUMN t_transfer_order.channel_order_no IS '渠道订单号';",
                    "COMMENT ON COLUMN t_transfer_order.channel_res_data IS '渠道响应数据';",
                    "COMMENT ON COLUMN t_transfer_order.err_code IS '渠道支付错误码';",
                    "COMMENT ON COLUMN t_transfer_order.err_msg IS '渠道支付错误描述';",
                    "COMMENT ON COLUMN t_transfer_order.ext_param IS '商户扩展参数';",
                    "COMMENT ON COLUMN t_transfer_order.notify_url IS '异步通知地址';",
                    "COMMENT ON COLUMN t_transfer_order.success_time IS '转账成功时间';",
                    "COMMENT ON COLUMN t_transfer_order.created_at IS '创建时间';",
                    "COMMENT ON COLUMN t_transfer_order.updated_at IS '更新时间';"
                };
                
                for (String commentSql : columnCommentsSql) {
                    stmt.execute(commentSql);
                }
                
                // 添加触发器
                String createTriggerSql = 
                    "DO $$ " +
                    "BEGIN " +
                    "  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_t_transfer_order_update_timestamp') THEN " +
                    "    CREATE TRIGGER trigger_t_transfer_order_update_timestamp " +
                    "    BEFORE UPDATE ON t_transfer_order " +
                    "    FOR EACH ROW " +
                    "    EXECUTE FUNCTION update_timestamp(); " +
                    "  END IF; " +
                    "END $$;";
                
                stmt.execute(createTriggerSql);
                
                log.info("转账订单表创建完成");
            } else {
                log.debug("转账订单表已存在");
            }
            
        } catch (SQLException e) {
            log.error("创建转账订单表失败", e);
        }
    }

    /**
     * 加载MySQL初始化脚本
     */
    private void loadMySQLSchema() {
        log.info("加载MySQL初始化脚本...");
        try {
            // 首先创建表
            ResourceDatabasePopulator tableCreator = new ResourceDatabasePopulator();
            tableCreator.addScript(new ClassPathResource("sql/payment_reconciliation_schema.sql"));
            tableCreator.setIgnoreFailedDrops(true);
            tableCreator.setContinueOnError(true);
            tableCreator.execute(dataSource);
            log.info("MySQL表创建完成");
            
            // 等待一秒确保表创建完成
            try { Thread.sleep(1000); } catch (InterruptedException e) { /* ignore */ }
            
            // 然后创建索引 - 对于MySQL需要单独执行每个CREATE INDEX语句，因为MySQL不支持IF NOT EXISTS
            try {
                executeCreateIndexStatementsIndividually("sql/payment_reconciliation_indexes.sql");
                log.info("MySQL索引创建完成");
            } catch (Exception e) {
                log.error("创建MySQL索引时发生错误，但这不会影响表的使用", e);
            }
        } catch (Exception e) {
            log.error("加载MySQL初始化脚本失败", e);
        }
    }
    
    /**
     * 单独执行每个CREATE INDEX语句，忽略已存在的索引错误
     * MySQL不支持IF NOT EXISTS语法，所以需要单独捕获每个语句的错误
     */
    private void executeCreateIndexStatementsIndividually(String scriptPath) throws Exception {
        ClassPathResource resource = new ClassPathResource(scriptPath);
        String script = new String(resource.getInputStream().readAllBytes());
        String[] statements = script.split(";");
        
        try (Connection conn = dataSource.getConnection()) {
            // 首先检查表中是否存在相应的列，避免在不存在的列上创建索引
            checkAndCreateColumns(conn);
            
            // 检查payment_reconciliation是否为视图
            boolean isPaymentReconciliationView = false;
            DatabaseMetaData dbMeta = conn.getMetaData();
            try (ResultSet tables = dbMeta.getTables(null, null, "payment_reconciliation", new String[]{"VIEW"})) {
                isPaymentReconciliationView = tables.next();
                if (isPaymentReconciliationView) {
                    log.info("payment_reconciliation是视图，将跳过其索引创建");
                }
            }
            
            for (String statement : statements) {
                String sql = statement.trim();
                if (sql.isEmpty() || sql.startsWith("--")) {
                    continue; // 跳过空行和注释
                }
                
                // 如果payment_reconciliation是视图，跳过其索引创建
                if (isPaymentReconciliationView && 
                    (sql.contains("payment_reconciliation") && sql.toUpperCase().contains("CREATE INDEX"))) {
                    log.info("跳过视图上的索引创建: {}", sql);
                    continue;
                }
                
                try (java.sql.Statement stmt = conn.createStatement()) {
                    log.debug("执行索引创建语句: {}", sql);
                    stmt.execute(sql);
                } catch (SQLException e) {
                    // 1061: Duplicate key name (索引已存在)
                    // 1091: Can't DROP ... (索引不存在)
                    // 1072: Key column doesn't exist
                    // 1347: 'xxx' is not BASE TABLE
                    if (e.getErrorCode() == 1061 || e.getErrorCode() == 1091) {
                        log.debug("索引已存在或不存在，忽略错误: {}", e.getMessage());
                    } else if (e.getErrorCode() == 1072) {
                        log.warn("列不存在，无法创建索引: {}", e.getMessage());
                    } else if (e.getErrorCode() == 1347) {
                        log.warn("对象不是基础表，无法创建索引: {}", e.getMessage());
                    } else {
                        log.warn("执行索引语句时出现错误: {} - {}", sql, e.getMessage());
                    }
                }
            }
        }
    }
    
    /**
     * 检查并创建必要的列
     */
    private void checkAndCreateColumns(Connection conn) {
        try {
            // 获取数据库类型
            String dbProductName = conn.getMetaData().getDatabaseProductName().toLowerCase();
            boolean isMySQL = dbProductName.contains("mysql");
            
            // 只对MySQL数据库进行列检查，PostgreSQL通过SQL脚本的IF NOT EXISTS处理
            if (isMySQL) {
                // 检查payment_records表是否包含backup_if_code列
                if (!columnExists(conn, "payment_records", "backup_if_code")) {
                    log.info("payment_records表缺少backup_if_code列，正在添加...");
                    try (java.sql.Statement stmt = conn.createStatement()) {
                        stmt.execute("ALTER TABLE payment_records ADD COLUMN backup_if_code VARCHAR(32) DEFAULT NULL COMMENT '备用支付渠道'");
                        log.info("成功添加backup_if_code列到payment_records表");
                    }
                }
                
                // 检查payment_compensation_records表是否包含backup_if_code列
                if (!columnExists(conn, "payment_compensation_records", "backup_if_code")) {
                    log.info("payment_compensation_records表缺少backup_if_code列，正在添加...");
                    try (java.sql.Statement stmt = conn.createStatement()) {
                        stmt.execute("ALTER TABLE payment_compensation_records ADD COLUMN backup_if_code VARCHAR(32) DEFAULT NULL COMMENT '备用支付渠道'");
                        log.info("成功添加backup_if_code列到payment_compensation_records表");
                    }
                }
                
                // 检查payment_reconciliation表类型
                checkTableType(conn, "payment_reconciliation");
            }
        } catch (SQLException e) {
            log.error("检查或创建列时出错", e);
        }
    }
    
    /**
     * 检查表的类型（是否为视图）
     */
    private void checkTableType(Connection conn, String tableName) throws SQLException {
        try {
            // 检查payment_reconciliation是否为视图
            DatabaseMetaData dbMeta = conn.getMetaData();
            boolean isView = false;
            
            try (ResultSet tables = dbMeta.getTables(null, null, tableName, new String[]{"VIEW"})) {
                if (tables.next()) {
                    isView = true;
                    log.info("表 {} 是视图，MySQL不支持在视图上创建索引", tableName);
                }
            }
            
            // 如果是视图，确保我们不会尝试在其上创建索引
            if (isView && "payment_reconciliation".equalsIgnoreCase(tableName)) {
                log.warn("检测到payment_reconciliation是视图，MySQL中不会创建索引");
            }
        } catch (SQLException e) {
            log.warn("检查表 {} 类型时出错", tableName, e);
        }
    }
    
    /**
     * 检查表中是否存在指定的列
     */
    private boolean columnExists(Connection conn, String tableName, String columnName) throws SQLException {
        DatabaseMetaData dbMeta = conn.getMetaData();
        try (ResultSet columns = dbMeta.getColumns(null, null, tableName, columnName)) {
            return columns.next();
        } catch (SQLException e) {
            log.warn("检查列 {} 是否存在时出错", columnName, e);
            return false;
        }
    }
} 