# PostgreSQL Reconciliation Service Migration

本文档介绍了将 PostgreSQL 支付对账服务从 JdbcTemplate 迁移到 MyBatis-Plus 的过程和使用方法。

## 迁移背景

Jeepay 支付系统使用了两种数据库：
- MySQL 作为主数据库，存储核心业务数据
- PostgreSQL 作为专用数据库，用于支付对账（利用其物化视图特性）

原始的 `PostgreSQLReconciliationService` 类使用了 JdbcTemplate 直接操作 PostgreSQL 数据库，而系统的其他部分广泛使用了 MyBatis-Plus ORM。为了统一技术栈和提高代码质量，我们将对账服务迁移到了 MyBatis-Plus。

## 迁移策略

我们采用了以下迁移策略：

1. **创建新的实现**：开发了新的 `PostgreSQLReconciliationServiceMybatis` 类，基于 MyBatis-Plus 实现
2. **保持兼容性**：保留原有的 JdbcTemplate 实现作为备选方案
3. **适配器模式**：提供 `PostgreSQLReconciliationServiceAdapter` 作为适配器，使现有代码可以无缝切换到新实现
4. **模型转换**：使用 `ReconciliationModelConverter` 工具类处理 JPA 和 MyBatis-Plus 模型之间的转换
5. **优雅降级**：如果新实现出现问题，自动回退到原有实现

## 主要组件

| 文件名 | 描述 |
|-------|------|
| PostgreSQLReconciliationService.java | 原始的基于 JdbcTemplate 的实现 |
| PostgreSQLReconciliationServiceMybatis.java | 新的基于 MyBatis-Plus 的实现 |
| PostgreSQLReconciliationServiceAdapter.java | 适配器类，提供无缝切换能力 |
| ReconciliationModelConverter.java | 模型转换工具类 |
| PostgreSQLReconciliationServiceExample.java | 新服务使用示例 |

## 数据模型差异

JPA 模型（原始）与 MyBatis-Plus 模型（新）之间存在一些差异：

| 特性 | JPA 模型 | MyBatis-Plus 模型 |
|-----|----------|----------------|
| 包路径 | com.jeequan.jeepay.pay.model | com.jeequan.jeepay.core.entity |
| 金额字段 | expectedAmount, actualAmount | expected, actual |
| 是否修复字段 | isFixed (Boolean) | isFixed (Integer) |
| 注解 | @Entity, @Table, @Column, @Id | @TableName, @TableId |

## 使用方法

### 使用适配器（推荐，平滑迁移）

```java
@Autowired
private PostgreSQLReconciliationServiceAdapter reconciliationService;

// 使用和原来一样的API
reconciliationService.refreshReconciliationView();
List<PaymentReconciliation> discrepancies = reconciliationService.findAllUnfixedDiscrepancies();
```

### 直接使用新实现（完全迁移后）

```java
@Autowired
private PostgreSQLReconciliationServiceMybatis reconciliationService;

// 刷新视图
reconciliationService.refreshReconciliationView();

// 获取未处理的差异记录
List<PaymentReconciliation> discrepancies = reconciliationService.findAllUnfixedDiscrepancies();

// 标记为已处理
reconciliationService.markAsFixed("order123456");

// 查询统计信息
Map<String, Object> stats = reconciliationService.getReconciliationStats();
```

### 切换实现方式

```java
@Autowired
private PostgreSQLReconciliationServiceAdapter adapter;

// 切换到新实现
adapter.setUseNewImplementation(true);

// 切换回旧实现
adapter.setUseNewImplementation(false);
```

## 迁移步骤

1. 添加新的 MyBatis-Plus 实现类 `PostgreSQLReconciliationServiceMybatis`
2. 添加模型转换工具类 `ReconciliationModelConverter`
3. 添加适配器类 `PostgreSQLReconciliationServiceAdapter`
4. 在现有代码中，将 `PostgreSQLReconciliationService` 替换为 `PostgreSQLReconciliationServiceAdapter`
5. 测试新实现的功能，确保与旧实现行为一致
6. 系统稳定后，可完全切换到新实现

## 注意事项

1. 物化视图的字段类型需与 MyBatis-Plus 实体类一致，尤其注意 `is_fixed` 字段应为 INTEGER 而非 BOOLEAN
2. 使用适配器时，会自动在模型之间进行转换，无需手动处理
3. 新实现保留了原有实现的所有容错和降级策略，包括：
   - 检查是否支持物化视图
   - 当物化视图不可用时回退到普通表
   - 处理各种SQL语法错误和数据库连接问题

## 示例代码

参考 `PostgreSQLReconciliationServiceExample` 类，其中演示了如何使用新的 MyBatis-Plus 实现执行常见的对账操作。 