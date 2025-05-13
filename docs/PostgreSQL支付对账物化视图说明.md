# PostgreSQL支付对账物化视图实现说明

## 1. 概述

本文档描述了在Jeepay系统中将支付对账视图从MySQL普通视图切换到PostgreSQL物化视图的实现方案。物化视图提供了更好的查询性能和更灵活的刷新机制，非常适合实时对账场景。

## 2. 物化视图与普通视图对比

| 特性 | MySQL视图 | PostgreSQL物化视图 |
| --- | --- | --- |
| 数据存储 | 不存储数据，每次查询时重新计算 | 实际存储查询结果，类似表 |
| 查询性能 | 每次都执行完整查询，性能较差 | 直接查询存储的结果，性能优异 |
| 实时性 | 总是实时的 | 需要定期刷新以保持最新 |
| 索引支持 | 不支持在视图上创建索引 | 支持创建索引提升查询性能 |
| 适用场景 | 简单查询、少量数据 | 复杂查询、大量数据、频繁访问 |

## 3. 实现架构

实现架构采用了以下组件：

1. 动态数据源配置：根据配置自动选择MySQL或PostgreSQL
2. 数据库检测机制：自动识别当前使用的数据库类型
3. 适配层：针对不同数据库类型使用不同的SQL语法
4. 定时刷新机制：PostgreSQL物化视图定期刷新保证数据一致性

## 4. 配置说明

### 4.1 启用PostgreSQL物化视图

在`application.yml`中配置：

```yaml
spring:
  datasource:
    dynamic:
      primary: postgres  # 设置为postgres使用PostgreSQL
      datasource:
        postgres:
          url: jdbc:postgresql://127.0.0.1:5432/jiangyangpay?currentSchema=public
          username: postgres
          password: postgres
          driver-class-name: org.postgresql.Driver
```

### 4.2 刷新频率配置

默认刷新频率为5秒，可在`ReconciliationService`中修改：

```java
@Scheduled(fixedDelay = 5000) // 每5秒刷新一次，可根据需要调整
public void refreshReconciliationView() {
    // ...
}
```

## 5. 物化视图结构

支付对账物化视图包含以下关键字段：

- `order_no`: 订单号（主键）
- `expected`: 预期金额（订单金额）
- `actual`: 实际金额（支付金额）
- `discrepancy_type`: 差异类型（NONE、MISSING_PAYMENT、AMOUNT_MISMATCH）
- `discrepancy_amount`: 差异金额
- `is_fixed`: 是否已修复
- `channel`: 支付渠道
- `backup_channel`: 备用支付渠道

## 6. 性能优化

为提高物化视图性能，系统自动：

1. 创建了订单号索引：`idx_payment_reconciliation_order_no`
2. 使用CONCURRENTLY刷新方式减少锁定
3. 优化了刷新频率，平衡实时性和系统负载

## 7. 并发刷新说明

PostgreSQL物化视图支持两种刷新方式：

1. `REFRESH MATERIALIZED VIEW CONCURRENTLY payment_reconciliation`：并发刷新，不阻塞读取，但需要有唯一索引
2. `REFRESH MATERIALIZED VIEW payment_reconciliation`：阻塞刷新，刷新期间无法查询

系统默认使用并发刷新模式，已自动创建所需索引。

## 8. 故障排除

常见问题：

1. 刷新失败：检查索引是否正确创建
2. 数据不一致：可能是刷新间隔过长，考虑缩短刷新周期
3. 性能问题：检查物化视图索引，考虑优化查询条件

## 9. 扩展阅读

- [PostgreSQL官方文档 - 物化视图](https://www.postgresql.org/docs/current/rules-materializedviews.html)
- [MySQL与PostgreSQL视图对比](https://www.postgresql.org/about/featurematrix/) 