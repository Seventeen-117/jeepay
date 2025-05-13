-- 支付记录表
CREATE TABLE IF NOT EXISTS `payment_records` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `order_no` varchar(64) NOT NULL COMMENT '订单号',
  `amount` decimal(20,6) NOT NULL COMMENT '支付金额',
  `channel` varchar(32) NOT NULL COMMENT '支付渠道',
  `backup_if_code` varchar(32) DEFAULT NULL COMMENT '备用支付渠道',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_order_no` (`order_no`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='支付记录表';

-- 支付补偿记录表
CREATE TABLE IF NOT EXISTS `payment_compensation_records` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `order_no` varchar(64) NOT NULL COMMENT '订单号',
  `primary_channel` varchar(32) NOT NULL COMMENT '主支付渠道',
  `backup_if_code` varchar(32) NOT NULL COMMENT '备用支付渠道',
  `amount` decimal(20,6) NOT NULL COMMENT '支付金额',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_order_no` (`order_no`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='支付补偿记录表';

-- 支付对账表（物化视图）
CREATE TABLE IF NOT EXISTS `payment_reconciliation` (
  `order_no` varchar(64) NOT NULL COMMENT '订单号',
  `expected` decimal(20,6) NOT NULL COMMENT '预期金额（订单金额）',
  `actual` decimal(20,6) DEFAULT NULL COMMENT '实际金额（支付金额）',
  `discrepancy_type` varchar(32) NOT NULL DEFAULT 'NONE' COMMENT '差异类型',
  `discrepancy_amount` decimal(20,6) DEFAULT NULL COMMENT '差异金额',
  `is_fixed` tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否已修复',
  `channel` varchar(32) DEFAULT NULL COMMENT '支付渠道',
  `backup_if_code` varchar(32) DEFAULT NULL COMMENT '备用支付渠道',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`order_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='支付对账表';


-- 创建支付渠道监控表
CREATE TABLE IF NOT EXISTS `payment_channel_metrics` (
  `if_code` varchar(32) NOT NULL COMMENT '接口代码',
  `call_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '调用总次数',
  `success_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '成功次数',
  `failure_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '失败次数',
  `avg_response_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '平均响应时间（毫秒）',
  `last_success_time` bigint(20) DEFAULT NULL COMMENT '最后一次成功时间',
  `last_failure_time` bigint(20) DEFAULT NULL COMMENT '最后一次失败时间',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`if_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='支付渠道监控表'; 