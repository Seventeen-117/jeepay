-- PostgreSQL数据库支付对账相关表结构

CREATE TABLE IF NOT EXISTS payment_records (
  id SERIAL PRIMARY KEY,
  order_no VARCHAR(64) NOT NULL,
  amount NUMERIC(20,6) NOT NULL,
  channel VARCHAR(32) NOT NULL,
  backup_if_code VARCHAR(32),
  create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uk_payment_records_order_no UNIQUE (order_no)
);
COMMENT ON TABLE payment_records IS '支付记录表';
COMMENT ON COLUMN payment_records.id IS '自增ID';
COMMENT ON COLUMN payment_records.order_no IS '订单号';
COMMENT ON COLUMN payment_records.amount IS '支付金额';
COMMENT ON COLUMN payment_records.channel IS '支付渠道';
COMMENT ON COLUMN payment_records.backup_if_code IS '备用支付渠道';
COMMENT ON COLUMN payment_records.create_time IS '创建时间';
COMMENT ON COLUMN payment_records.update_time IS '更新时间';

-- 支付补偿记录表
CREATE TABLE IF NOT EXISTS payment_compensation_records (
  id SERIAL PRIMARY KEY,
  order_no VARCHAR(64) NOT NULL,
  primary_channel VARCHAR(32) NOT NULL,
  backup_if_code VARCHAR(32) NOT NULL,
  amount NUMERIC(20,6) NOT NULL,
  create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE payment_compensation_records IS '支付补偿记录表';
COMMENT ON COLUMN payment_compensation_records.id IS '自增ID';
COMMENT ON COLUMN payment_compensation_records.order_no IS '订单号';
COMMENT ON COLUMN payment_compensation_records.primary_channel IS '主支付渠道';
COMMENT ON COLUMN payment_compensation_records.backup_if_code IS '备用支付渠道';
COMMENT ON COLUMN payment_compensation_records.amount IS '支付金额';
COMMENT ON COLUMN payment_compensation_records.create_time IS '创建时间';
COMMENT ON COLUMN payment_compensation_records.update_time IS '更新时间';

-- 支付订单表（从MySQL迁移）
CREATE TABLE IF NOT EXISTS t_pay_order (
  pay_order_id VARCHAR(30) PRIMARY KEY,
  mch_no VARCHAR(64) NOT NULL,
  isv_no VARCHAR(64),
  app_id VARCHAR(64) NOT NULL,
  mch_name VARCHAR(30) NOT NULL,
  mch_type SMALLINT NOT NULL,
  mch_order_no VARCHAR(64) NOT NULL,
  if_code VARCHAR(20),
  backup_if_code VARCHAR(20),
  way_code VARCHAR(20) NOT NULL,
  amount BIGINT NOT NULL,
  mch_fee_rate NUMERIC(20,6) NOT NULL,
  mch_fee_amount BIGINT NOT NULL,
  currency VARCHAR(3) NOT NULL DEFAULT 'cny',
  state SMALLINT NOT NULL DEFAULT 0,
  notify_state SMALLINT NOT NULL DEFAULT 0,
  client_ip VARCHAR(32),
  subject VARCHAR(64) NOT NULL,
  body VARCHAR(256) NOT NULL,
  channel_extra VARCHAR(512),
  channel_user VARCHAR(64),
  channel_order_no VARCHAR(64),
  refund_state SMALLINT NOT NULL DEFAULT 0,
  refund_times INTEGER NOT NULL DEFAULT 0,
  refund_amount BIGINT NOT NULL DEFAULT 0,
  division_mode SMALLINT DEFAULT 0,
  division_state SMALLINT DEFAULT 0,
  division_last_time TIMESTAMP,
  err_code VARCHAR(128),
  err_msg VARCHAR(256),
  ext_param VARCHAR(128),
  notify_url VARCHAR(128) NOT NULL DEFAULT '',
  return_url VARCHAR(128) DEFAULT '',
  expired_time TIMESTAMP,
  success_time TIMESTAMP,
  created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uk_mch_no_mch_order_no UNIQUE (mch_no, mch_order_no)
);
CREATE INDEX IF NOT EXISTS idx_t_pay_order_created_at ON t_pay_order(created_at);

COMMENT ON TABLE t_pay_order IS '支付订单表';
COMMENT ON COLUMN t_pay_order.pay_order_id IS '支付订单号';
COMMENT ON COLUMN t_pay_order.mch_no IS '商户号';
COMMENT ON COLUMN t_pay_order.isv_no IS '服务商号';
COMMENT ON COLUMN t_pay_order.app_id IS '应用ID';
COMMENT ON COLUMN t_pay_order.mch_name IS '商户名称';
COMMENT ON COLUMN t_pay_order.mch_type IS '类型: 1-普通商户, 2-特约商户(服务商模式)';
COMMENT ON COLUMN t_pay_order.mch_order_no IS '商户订单号';
COMMENT ON COLUMN t_pay_order.if_code IS '支付接口代码';
COMMENT ON COLUMN t_pay_order.backup_if_code IS '备用支付接口代码';
COMMENT ON COLUMN t_pay_order.way_code IS '支付方式代码';
COMMENT ON COLUMN t_pay_order.amount IS '支付金额,单位分';
COMMENT ON COLUMN t_pay_order.mch_fee_rate IS '商户手续费费率快照';
COMMENT ON COLUMN t_pay_order.mch_fee_amount IS '商户手续费,单位分';
COMMENT ON COLUMN t_pay_order.currency IS '三位货币代码,人民币:cny';
COMMENT ON COLUMN t_pay_order.state IS '支付状态: 0-订单生成, 1-支付中, 2-支付成功, 3-支付失败, 4-已撤销, 5-已退款, 6-订单关闭';
COMMENT ON COLUMN t_pay_order.notify_state IS '向下游回调状态, 0-未发送, 1-已发送';
COMMENT ON COLUMN t_pay_order.client_ip IS '客户端IP';
COMMENT ON COLUMN t_pay_order.subject IS '商品标题';
COMMENT ON COLUMN t_pay_order.body IS '商品描述信息';
COMMENT ON COLUMN t_pay_order.channel_extra IS '特定渠道发起额外参数';
COMMENT ON COLUMN t_pay_order.channel_user IS '渠道用户标识,如微信openId,支付宝账号';
COMMENT ON COLUMN t_pay_order.channel_order_no IS '渠道订单号';
COMMENT ON COLUMN t_pay_order.refund_state IS '退款状态: 0-未发生实际退款, 1-部分退款, 2-全额退款';
COMMENT ON COLUMN t_pay_order.refund_times IS '退款次数';
COMMENT ON COLUMN t_pay_order.refund_amount IS '退款总金额,单位分';
COMMENT ON COLUMN t_pay_order.division_mode IS '订单分账模式：0-该笔订单不允许分账, 1-支付成功按配置自动完成分账, 2-商户手动分账(解冻商户金额)';
COMMENT ON COLUMN t_pay_order.division_state IS '订单分账状态：0-未发生分账, 1-等待分账任务处理, 2-分账处理中, 3-分账任务已结束(不体现状态)';
COMMENT ON COLUMN t_pay_order.division_last_time IS '最新分账时间';
COMMENT ON COLUMN t_pay_order.err_code IS '渠道支付错误码';
COMMENT ON COLUMN t_pay_order.err_msg IS '渠道支付错误描述';
COMMENT ON COLUMN t_pay_order.ext_param IS '商户扩展参数';
COMMENT ON COLUMN t_pay_order.notify_url IS '异步通知地址';
COMMENT ON COLUMN t_pay_order.return_url IS '页面跳转地址';
COMMENT ON COLUMN t_pay_order.expired_time IS '订单失效时间';
COMMENT ON COLUMN t_pay_order.success_time IS '订单支付成功时间';
COMMENT ON COLUMN t_pay_order.created_at IS '创建时间';
COMMENT ON COLUMN t_pay_order.updated_at IS '更新时间';

-- 退款订单表（从MySQL迁移）
CREATE TABLE IF NOT EXISTS t_refund_order (
  refund_order_id VARCHAR(30) PRIMARY KEY,
  pay_order_id VARCHAR(30) NOT NULL,
  channel_pay_order_no VARCHAR(64),
  mch_no VARCHAR(64) NOT NULL,
  isv_no VARCHAR(64),
  app_id VARCHAR(64) NOT NULL,
  mch_name VARCHAR(30) NOT NULL,
  mch_type SMALLINT NOT NULL,
  mch_refund_no VARCHAR(64) NOT NULL,
  way_code VARCHAR(20) NOT NULL,
  if_code VARCHAR(20),
  pay_amount BIGINT NOT NULL,
  refund_amount BIGINT NOT NULL,
  currency VARCHAR(3) NOT NULL DEFAULT 'cny',
  state SMALLINT NOT NULL DEFAULT 0,
  client_ip VARCHAR(32),
  refund_reason VARCHAR(128),
  channel_order_no VARCHAR(64),
  err_code VARCHAR(128),
  err_msg VARCHAR(2048),
  channel_extra VARCHAR(512),
  notify_url VARCHAR(128),
  ext_param VARCHAR(128),
  success_time TIMESTAMP,
  expired_time TIMESTAMP,
  created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uk_mch_no_mch_refund_no UNIQUE (mch_no, mch_refund_no)
);
CREATE INDEX IF NOT EXISTS idx_t_refund_order_created_at ON t_refund_order(created_at);
CREATE INDEX IF NOT EXISTS idx_t_refund_order_pay_order_id ON t_refund_order(pay_order_id);

COMMENT ON TABLE t_refund_order IS '退款订单表';
COMMENT ON COLUMN t_refund_order.refund_order_id IS '退款订单号';
COMMENT ON COLUMN t_refund_order.pay_order_id IS '支付订单号';
COMMENT ON COLUMN t_refund_order.channel_pay_order_no IS '渠道支付单号';
COMMENT ON COLUMN t_refund_order.mch_no IS '商户号';
COMMENT ON COLUMN t_refund_order.isv_no IS '服务商号';
COMMENT ON COLUMN t_refund_order.app_id IS '应用ID';
COMMENT ON COLUMN t_refund_order.mch_name IS '商户名称';
COMMENT ON COLUMN t_refund_order.mch_type IS '类型:1-普通商户,2-特约商户(服务商模式)';
COMMENT ON COLUMN t_refund_order.mch_refund_no IS '商户退款单号';
COMMENT ON COLUMN t_refund_order.way_code IS '支付方式代码';
COMMENT ON COLUMN t_refund_order.if_code IS '支付接口代码';
COMMENT ON COLUMN t_refund_order.pay_amount IS '支付金额,单位分';
COMMENT ON COLUMN t_refund_order.refund_amount IS '退款金额,单位分';
COMMENT ON COLUMN t_refund_order.currency IS '三位货币代码,人民币:cny';
COMMENT ON COLUMN t_refund_order.state IS '退款状态:0-订单生成,1-退款中,2-退款成功,3-退款失败,4-退款任务关闭';
COMMENT ON COLUMN t_refund_order.client_ip IS '客户端IP';
COMMENT ON COLUMN t_refund_order.refund_reason IS '退款原因';
COMMENT ON COLUMN t_refund_order.channel_order_no IS '渠道订单号';
COMMENT ON COLUMN t_refund_order.err_code IS '渠道错误码';
COMMENT ON COLUMN t_refund_order.err_msg IS '渠道错误描述';
COMMENT ON COLUMN t_refund_order.channel_extra IS '特定渠道发起额外参数';
COMMENT ON COLUMN t_refund_order.notify_url IS '通知地址';
COMMENT ON COLUMN t_refund_order.ext_param IS '扩展参数';
COMMENT ON COLUMN t_refund_order.success_time IS '订单退款成功时间';
COMMENT ON COLUMN t_refund_order.expired_time IS '退款失效时间';
COMMENT ON COLUMN t_refund_order.created_at IS '创建时间';
COMMENT ON COLUMN t_refund_order.updated_at IS '更新时间';

-- 分账记录表
CREATE TABLE IF NOT EXISTS t_pay_order_division_record (
  record_id BIGSERIAL PRIMARY KEY,
  mch_no VARCHAR(64) NOT NULL,
  isv_no VARCHAR(64),
  app_id VARCHAR(64) NOT NULL,
  mch_name VARCHAR(30) NOT NULL,
  mch_type SMALLINT NOT NULL,
  if_code VARCHAR(20) NOT NULL,
  pay_order_id VARCHAR(30) NOT NULL,
  pay_order_channel_order_no VARCHAR(64),
  pay_order_amount BIGINT NOT NULL,
  pay_order_division_amount BIGINT NOT NULL,
  batch_order_id VARCHAR(30) NOT NULL,
  channel_batch_order_id VARCHAR(64),
  state SMALLINT NOT NULL,
  channel_resp_result TEXT,
  receiver_id BIGINT NOT NULL,
  receiver_group_id BIGINT,
  receiver_alias VARCHAR(64),
  acc_type SMALLINT NOT NULL,
  acc_no VARCHAR(50) NOT NULL,
  acc_name VARCHAR(30) NOT NULL DEFAULT '',
  relation_type VARCHAR(30) NOT NULL,
  relation_type_name VARCHAR(30) NOT NULL,
  division_profit NUMERIC(20,6) NOT NULL,
  cal_division_amount BIGINT NOT NULL,
  created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_t_pay_order_division_record_created_at ON t_pay_order_division_record(created_at);
CREATE INDEX IF NOT EXISTS idx_t_pay_order_division_record_pay_order_id ON t_pay_order_division_record(pay_order_id);
CREATE INDEX IF NOT EXISTS idx_t_pay_order_division_record_batch_order_id ON t_pay_order_division_record(batch_order_id);

COMMENT ON TABLE t_pay_order_division_record IS '分账记录表';
COMMENT ON COLUMN t_pay_order_division_record.record_id IS '分账记录ID';
COMMENT ON COLUMN t_pay_order_division_record.mch_no IS '商户号';
COMMENT ON COLUMN t_pay_order_division_record.isv_no IS '服务商号';
COMMENT ON COLUMN t_pay_order_division_record.app_id IS '应用ID';
COMMENT ON COLUMN t_pay_order_division_record.mch_name IS '商户名称';
COMMENT ON COLUMN t_pay_order_division_record.mch_type IS '类型: 1-普通商户, 2-特约商户(服务商模式)';
COMMENT ON COLUMN t_pay_order_division_record.if_code IS '支付接口代码';
COMMENT ON COLUMN t_pay_order_division_record.pay_order_id IS '系统支付订单号';
COMMENT ON COLUMN t_pay_order_division_record.pay_order_channel_order_no IS '支付订单渠道支付订单号';
COMMENT ON COLUMN t_pay_order_division_record.pay_order_amount IS '订单金额,单位分';
COMMENT ON COLUMN t_pay_order_division_record.pay_order_division_amount IS '订单实际分账金额, 单位：分（订单金额 - 商户手续费 - 已退款金额）';
COMMENT ON COLUMN t_pay_order_division_record.batch_order_id IS '系统分账批次号';
COMMENT ON COLUMN t_pay_order_division_record.channel_batch_order_id IS '上游分账批次号';
COMMENT ON COLUMN t_pay_order_division_record.state IS '状态: 0-待分账 1-分账成功（明确成功）, 2-分账失败（明确失败）, 3-分账已受理（上游受理）';
COMMENT ON COLUMN t_pay_order_division_record.channel_resp_result IS '上游返回数据包';
COMMENT ON COLUMN t_pay_order_division_record.receiver_id IS '账号快照》 分账接收者ID';
COMMENT ON COLUMN t_pay_order_division_record.receiver_group_id IS '账号快照》 组ID（便于商户接口使用）';
COMMENT ON COLUMN t_pay_order_division_record.receiver_alias IS '接收者账号别名';
COMMENT ON COLUMN t_pay_order_division_record.acc_type IS '账号快照》 分账接收账号类型: 0-个人 1-商户';
COMMENT ON COLUMN t_pay_order_division_record.acc_no IS '账号快照》 分账接收账号';
COMMENT ON COLUMN t_pay_order_division_record.acc_name IS '账号快照》 分账接收账号名称';
COMMENT ON COLUMN t_pay_order_division_record.relation_type IS '账号快照》 分账关系类型（参考微信）， 如： SERVICE_PROVIDER 服务商等';
COMMENT ON COLUMN t_pay_order_division_record.relation_type_name IS '账号快照》 当选择自定义时，需要录入该字段。 否则为对应的名称';
COMMENT ON COLUMN t_pay_order_division_record.division_profit IS '账号快照》 配置的实际分账比例';
COMMENT ON COLUMN t_pay_order_division_record.cal_division_amount IS '计算该接收方的分账金额,单位分';
COMMENT ON COLUMN t_pay_order_division_record.created_at IS '创建时间';
COMMENT ON COLUMN t_pay_order_division_record.updated_at IS '更新时间';

-- 支付订单补偿记录表
CREATE TABLE IF NOT EXISTS t_pay_order_compensation (
  compensation_id BIGSERIAL PRIMARY KEY,
  pay_order_id VARCHAR(30) NOT NULL,
  mch_no VARCHAR(64) NOT NULL,
  app_id VARCHAR(64) NOT NULL,
  original_if_code VARCHAR(20) NOT NULL,
  compensation_if_code VARCHAR(20) NOT NULL,
  amount BIGINT NOT NULL,
  state SMALLINT NOT NULL DEFAULT 0,
  result_info VARCHAR(256),
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT uk_t_pay_order_compensation_pay_order_id UNIQUE (pay_order_id)
);
CREATE INDEX IF NOT EXISTS idx_t_pay_order_compensation_created_at ON t_pay_order_compensation(created_at);

COMMENT ON TABLE t_pay_order_compensation IS '支付订单补偿记录';
COMMENT ON COLUMN t_pay_order_compensation.compensation_id IS '补偿记录ID';
COMMENT ON COLUMN t_pay_order_compensation.pay_order_id IS '支付订单号';
COMMENT ON COLUMN t_pay_order_compensation.mch_no IS '商户号';
COMMENT ON COLUMN t_pay_order_compensation.app_id IS '应用ID';
COMMENT ON COLUMN t_pay_order_compensation.original_if_code IS '原支付接口代码';
COMMENT ON COLUMN t_pay_order_compensation.compensation_if_code IS '补偿支付接口代码';
COMMENT ON COLUMN t_pay_order_compensation.amount IS '订单金额';
COMMENT ON COLUMN t_pay_order_compensation.state IS '状态: 0-处理中, 1-成功, 2-失败';
COMMENT ON COLUMN t_pay_order_compensation.result_info IS '补偿结果信息';
COMMENT ON COLUMN t_pay_order_compensation.created_at IS '创建时间';
COMMENT ON COLUMN t_pay_order_compensation.updated_at IS '更新时间';

-- 创建支付对账物化视图
DROP VIEW IF EXISTS payment_reconciliation;
CREATE MATERIALIZED VIEW payment_reconciliation AS
SELECT
    po.pay_order_id AS order_no,
    CAST(po.amount AS NUMERIC(20,6)) AS expected,
    pr.amount AS actual,
    CASE
        WHEN pr.amount IS NULL THEN 'MISSING_PAYMENT'
        WHEN CAST(po.amount AS NUMERIC(20,6)) != pr.amount THEN 'AMOUNT_MISMATCH'
        ELSE 'NONE'
        END AS discrepancy_type,
    CASE
        WHEN pr.amount IS NULL THEN CAST(po.amount AS NUMERIC(20,6))
        ELSE CAST(po.amount AS NUMERIC(20,6)) - pr.amount
        END AS discrepancy_amount,
    0 AS is_fixed,  -- Using 0 instead of FALSE to match your Java entity
    po.if_code AS channel,
    po.backup_if_code,
    CURRENT_TIMESTAMP AS create_time,
    CURRENT_TIMESTAMP AS update_time
FROM
    t_pay_order po
        LEFT JOIN
    payment_records pr ON po.pay_order_id = pr.order_no
WHERE
    po.state = 2;  -- 支付状态为成功的订单

-- 为物化视图创建主键
CREATE UNIQUE INDEX IF NOT EXISTS payment_reconciliation_pkey ON payment_reconciliation (order_no);

-- 创建支付渠道监控表
CREATE TABLE IF NOT EXISTS payment_channel_metrics (
  if_code VARCHAR(32) PRIMARY KEY,
  call_count BIGINT NOT NULL DEFAULT 0,
  success_count BIGINT NOT NULL DEFAULT 0,
  failure_count BIGINT NOT NULL DEFAULT 0,
  avg_response_time BIGINT NOT NULL DEFAULT 0,
  last_success_time BIGINT,
  last_failure_time BIGINT,
  create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE payment_channel_metrics IS '支付渠道监控表';
COMMENT ON COLUMN payment_channel_metrics.if_code IS '接口代码';
COMMENT ON COLUMN payment_channel_metrics.call_count IS '调用总次数';
COMMENT ON COLUMN payment_channel_metrics.success_count IS '成功次数';
COMMENT ON COLUMN payment_channel_metrics.failure_count IS '失败次数';
COMMENT ON COLUMN payment_channel_metrics.avg_response_time IS '平均响应时间（毫秒）';
COMMENT ON COLUMN payment_channel_metrics.last_success_time IS '最后一次成功时间';
COMMENT ON COLUMN payment_channel_metrics.last_failure_time IS '最后一次失败时间';
COMMENT ON COLUMN payment_channel_metrics.create_time IS '创建时间';
COMMENT ON COLUMN payment_channel_metrics.update_time IS '更新时间';

-- 创建函数自动更新时间戳
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.update_time = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 为表添加自动更新时间戳触发器
DO $$
BEGIN
    -- 检查并创建更新时间触发器
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_payment_records_update_timestamp') THEN
        CREATE TRIGGER trigger_payment_records_update_timestamp
        BEFORE UPDATE ON payment_records
        FOR EACH ROW
        EXECUTE FUNCTION update_timestamp();
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_payment_compensation_records_update_timestamp') THEN
        CREATE TRIGGER trigger_payment_compensation_records_update_timestamp
        BEFORE UPDATE ON payment_compensation_records
        FOR EACH ROW
        EXECUTE FUNCTION update_timestamp();
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_payment_channel_metrics_update_timestamp') THEN
        CREATE TRIGGER trigger_payment_channel_metrics_update_timestamp
        BEFORE UPDATE ON payment_channel_metrics
        FOR EACH ROW
        EXECUTE FUNCTION update_timestamp();
    END IF;
    
    -- 添加t_pay_order表的自动更新时间戳触发器
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_t_pay_order_update_timestamp') THEN
        CREATE TRIGGER trigger_t_pay_order_update_timestamp
        BEFORE UPDATE ON t_pay_order
        FOR EACH ROW
        EXECUTE FUNCTION update_timestamp();
    END IF;

    -- 添加t_pay_order_compensation表的自动更新时间戳触发器
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_t_pay_order_compensation_update_timestamp') THEN
        CREATE TRIGGER trigger_t_pay_order_compensation_update_timestamp
        BEFORE UPDATE ON t_pay_order_compensation
        FOR EACH ROW
        EXECUTE FUNCTION update_timestamp();
    END IF;
    
    -- 添加t_refund_order表的自动更新时间戳触发器
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_t_refund_order_update_timestamp') THEN
        CREATE TRIGGER trigger_t_refund_order_update_timestamp
        BEFORE UPDATE ON t_refund_order
        FOR EACH ROW
        EXECUTE FUNCTION update_timestamp();
    END IF;
    
    -- 添加t_pay_order_division_record表的自动更新时间戳触发器
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_t_pay_order_division_record_update_timestamp') THEN
        CREATE TRIGGER trigger_t_pay_order_division_record_update_timestamp
        BEFORE UPDATE ON t_pay_order_division_record
        FOR EACH ROW
        EXECUTE FUNCTION update_timestamp();
    END IF;
END $$;

-- 创建物化视图刷新函数
CREATE OR REPLACE FUNCTION refresh_payment_reconciliation()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY payment_reconciliation;
END;
$$ LANGUAGE plpgsql;

-- 注释物化视图和函数
COMMENT ON MATERIALIZED VIEW payment_reconciliation IS '支付对账物化视图';
COMMENT ON FUNCTION refresh_payment_reconciliation() IS '刷新支付对账物化视图';

-- 转账订单表
CREATE TABLE IF NOT EXISTS t_transfer_order (
  transfer_id VARCHAR(30) PRIMARY KEY,
  mch_no VARCHAR(64) NOT NULL,
  isv_no VARCHAR(64),
  app_id VARCHAR(64) NOT NULL,
  mch_name VARCHAR(30) NOT NULL,
  mch_type SMALLINT NOT NULL,
  mch_order_no VARCHAR(64) NOT NULL,
  if_code VARCHAR(20),
  entry_type VARCHAR(20),
  amount BIGINT NOT NULL,
  currency VARCHAR(3) NOT NULL DEFAULT 'cny',
  account_no VARCHAR(64),
  account_name VARCHAR(64),
  bank_name VARCHAR(64),
  transfer_desc VARCHAR(128),
  client_ip VARCHAR(32),
  state SMALLINT NOT NULL DEFAULT 0,
  channel_extra VARCHAR(512),
  channel_order_no VARCHAR(64),
  channel_res_data TEXT,
  err_code VARCHAR(128),
  err_msg VARCHAR(256),
  ext_param VARCHAR(128),
  notify_url VARCHAR(128),
  success_time TIMESTAMP,
  created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uk_mch_no_mch_order_no_transfer UNIQUE (mch_no, mch_order_no)
);
CREATE INDEX IF NOT EXISTS idx_t_transfer_order_created_at ON t_transfer_order(created_at);

COMMENT ON TABLE t_transfer_order IS '转账订单表';
COMMENT ON COLUMN t_transfer_order.transfer_id IS '转账订单号';
COMMENT ON COLUMN t_transfer_order.mch_no IS '商户号';
COMMENT ON COLUMN t_transfer_order.isv_no IS '服务商号';
COMMENT ON COLUMN t_transfer_order.app_id IS '应用ID';
COMMENT ON COLUMN t_transfer_order.mch_name IS '商户名称';
COMMENT ON COLUMN t_transfer_order.mch_type IS '类型: 1-普通商户, 2-特约商户(服务商模式)';
COMMENT ON COLUMN t_transfer_order.mch_order_no IS '商户订单号';
COMMENT ON COLUMN t_transfer_order.if_code IS '支付接口代码';
COMMENT ON COLUMN t_transfer_order.entry_type IS '入账方式： WX_CASH-微信零钱; ALIPAY_CASH-支付宝转账; BANK_CARD-银行卡';
COMMENT ON COLUMN t_transfer_order.amount IS '转账金额,单位分';
COMMENT ON COLUMN t_transfer_order.currency IS '三位货币代码,人民币:cny';
COMMENT ON COLUMN t_transfer_order.account_no IS '收款账号';
COMMENT ON COLUMN t_transfer_order.account_name IS '收款人姓名';
COMMENT ON COLUMN t_transfer_order.bank_name IS '收款人开户行名称';
COMMENT ON COLUMN t_transfer_order.transfer_desc IS '转账备注信息';
COMMENT ON COLUMN t_transfer_order.client_ip IS '客户端IP';
COMMENT ON COLUMN t_transfer_order.state IS '支付状态: 0-订单生成, 1-转账中, 2-转账成功, 3-转账失败, 4-订单关闭';
COMMENT ON COLUMN t_transfer_order.channel_extra IS '特定渠道发起额外参数';
COMMENT ON COLUMN t_transfer_order.channel_order_no IS '渠道订单号';
COMMENT ON COLUMN t_transfer_order.channel_res_data IS '渠道响应数据';
COMMENT ON COLUMN t_transfer_order.err_code IS '渠道支付错误码';
COMMENT ON COLUMN t_transfer_order.err_msg IS '渠道支付错误描述';
COMMENT ON COLUMN t_transfer_order.ext_param IS '商户扩展参数';
COMMENT ON COLUMN t_transfer_order.notify_url IS '异步通知地址';
COMMENT ON COLUMN t_transfer_order.success_time IS '转账成功时间';
COMMENT ON COLUMN t_transfer_order.created_at IS '创建时间';
COMMENT ON COLUMN t_transfer_order.updated_at IS '更新时间'; 