-- 添加列和索引（确保表已创建后再添加）

-- 支付记录表索引
ALTER TABLE payment_records 
    ADD COLUMN IF NOT EXISTS backup_if_code VARCHAR(32) DEFAULT NULL;
COMMENT ON COLUMN payment_records.backup_if_code IS '备用支付渠道';
CREATE INDEX IF NOT EXISTS idx_payment_records_channel ON payment_records(channel);
CREATE INDEX IF NOT EXISTS idx_payment_records_backup_if_code ON payment_records(backup_if_code);

-- 支付补偿记录表索引
ALTER TABLE payment_compensation_records 
    ADD COLUMN IF NOT EXISTS backup_if_code VARCHAR(32) DEFAULT NULL;
COMMENT ON COLUMN payment_compensation_records.backup_if_code IS '备用支付渠道';
CREATE INDEX IF NOT EXISTS idx_compensation_records_order_no ON payment_compensation_records(order_no);
CREATE INDEX IF NOT EXISTS idx_compensation_records_primary_channel ON payment_compensation_records(primary_channel);
CREATE INDEX IF NOT EXISTS idx_compensation_records_backup_if_code ON payment_compensation_records(backup_if_code);

-- 注意：PostgreSQL 物化视图支持索引创建
-- 假设payment_reconciliation是物化视图(materialized view)而不是普通视图
-- 如果是普通视图，则这些索引创建语句会失败
CREATE INDEX IF NOT EXISTS idx_payment_reconciliation_discrepancy_type ON payment_reconciliation(discrepancy_type);
CREATE INDEX IF NOT EXISTS idx_payment_reconciliation_is_fixed ON payment_reconciliation(is_fixed);

CREATE INDEX IF NOT EXISTS idx_t_pay_order_compensation_status ON t_pay_order_compensation (state);
CREATE INDEX IF NOT EXISTS idx_t_pay_order_compensation_create_time ON t_pay_order_compensation (created_at);


