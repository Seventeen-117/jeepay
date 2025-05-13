-- 添加索引（分开创建，确保表已存在）
-- MySQL不支持IF NOT EXISTS语法，处理方式为程序中捕获异常


-- 支付记录表索引（backup_if_code字段）
-- 注意：此字段可能之前不存在，已在程序中处理添加字段
ALTER TABLE payment_records
    ADD COLUMN backup_if_code VARCHAR(255) DEFAULT NULL COMMENT '备用支付代码';
CREATE INDEX idx_payment_records_backup_if_code ON payment_records(backup_if_code);

-- 支付补偿记录表索引
ALTER TABLE payment_compensation_records
    ADD COLUMN backup_if_code VARCHAR(255) DEFAULT NULL COMMENT '备用支付代码';
CREATE INDEX idx_payment_compensation_records_backup_if_code ON payment_compensation_records(backup_if_code);

-- 注意：payment_reconciliation 不是基础表(BASE TABLE)，而是视图，无法在视图上创建索引
-- 以下索引创建语句已被注释掉
-- CREATE INDEX idx_payment_reconciliation_discrepancy_type ON payment_reconciliation(discrepancy_type);
-- CREATE INDEX idx_payment_reconciliation_is_fixed ON payment_reconciliation(is_fixed);
