-- ----------------------------
-- Table structure for t_pay_order_compensation
-- ----------------------------
DROP TABLE IF EXISTS `t_pay_order_compensation`;
CREATE TABLE `t_pay_order_compensation` (
  `compensation_id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '补偿记录ID',
  `pay_order_id` varchar(30) NOT NULL COMMENT '支付订单号',
  `mch_no` varchar(30) NOT NULL COMMENT '商户号',
  `app_id` varchar(64) NOT NULL COMMENT '应用ID',
  `original_if_code` varchar(20) NOT NULL COMMENT '原支付接口代码',
  `compensation_if_code` varchar(20) NOT NULL COMMENT '补偿支付接口代码',
  `amount` bigint(20) NOT NULL COMMENT '订单金额',
  `state` tinyint(6) NOT NULL DEFAULT '0' COMMENT '状态: 0-处理中, 1-成功, 2-失败',
  `result_info` varchar(256) DEFAULT NULL COMMENT '补偿结果信息',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` timestamp NULL DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`compensation_id`),
  UNIQUE KEY `idx_pay_order_id` (`pay_order_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1001 DEFAULT CHARSET=utf8mb4 COMMENT='支付订单补偿记录'; 