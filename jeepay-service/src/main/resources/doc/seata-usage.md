# Seata 分布式事务使用指南

## 简介

本项目使用 Seata 实现分布式事务管理，支持两种模式：
1. AT 模式（Two-Phase Commit）：适用于强一致性场景
2. Saga 模式：适用于长事务场景，每个步骤都有对应的补偿操作

## 配置说明

Seata 配置文件位于以下位置：
- `file.conf`：Seata 基本配置
- `registry.conf`：注册中心配置
- `seata.yml`：Spring Boot 应用配置

### 数据库表

Seata 需要以下数据库表：
- AT 模式：undo_log
- Saga 模式：seata_state_machine_def, seata_state_machine_inst, seata_state_inst

表创建脚本位于 `jeepay-service/src/main/resources/db/seata_tables.sql`

## 使用方式

### AT 模式（两阶段提交）

使用 `@GlobalTransactional` 注解来标记全局事务：

```java
@GlobalTransactional(name = "业务名称", rollbackFor = Exception.class)
public void businessMethod() {
    // 业务逻辑，可以跨多个微服务
    // 如果任何一步出现异常，会自动回滚所有已提交的事务
}
```

示例：

```java
// 在 PaymentTransactionService 中
@GlobalTransactional(name = "pay-order-success-tx", rollbackFor = Exception.class)
public void completePayOrderWithTwoPhaseCommit(String payOrderId) {
    // 更新订单状态
    // 更新商户余额
}
```

### Saga 模式（状态机）

1. 定义 JSON 格式的状态机：

在 `jeepay-service/src/main/resources/statelang/` 目录下创建 JSON 文件，定义状态机。状态机包含各个状态节点以及它们的补偿节点。

2. 使用状态机引擎执行状态机：

```java
// 准备参数
Map<String, Object> startParams = new HashMap<>();
startParams.put("paramName", paramValue);

// 执行状态机
stateMachineEngine.startWithBusinessKey(stateMachineName, null, businessKey, startParams);
```

示例：

```java
// 在 PaymentTransactionService 中
public void refundPayOrderWithSaga(String payOrderId, Long refundAmount, String refundReason) {
    Map<String, Object> startParams = new HashMap<>();
    startParams.put("payOrderId", payOrderId);
    startParams.put("refundAmount", refundAmount);
    
    stateMachineEngine.startWithBusinessKey("payOrderRefundStateMachine", null, "refund:" + payOrderId, startParams);
}
```

## 注意事项

1. 确保每个参与分布式事务的数据库都创建了 Seata 所需的表
2. AT 模式下，所有操作的资源必须支持事务，如 MySQL、PostgreSQL
3. Saga 模式下，需要为每个服务操作编写对应的补偿操作
4. 启动项目前，确保 Seata Server 已正确配置并启动

## API 接口示例

测试 AT 模式：
```
POST /api/transaction/at/demo?payOrderId=P0001&newState=2
```

测试 Saga 模式：
```
POST /api/transaction/saga/demo?payOrderId=P0001&newState=2
```

支付完成（AT 模式）：
```
POST /api/transaction/pay/complete?payOrderId=P0001
```

订单退款（Saga 模式）：
```
POST /api/transaction/pay/refund?payOrderId=P0001&refundAmount=100&refundReason=客户退款
```

## 部署 Seata Server

1. 下载 Seata Server：https://github.com/seata/seata/releases
2. 配置 `conf/registry.conf` 和 `conf/file.conf`
3. 创建数据库表
4. 启动 Seata Server：
```bash
sh bin/seata-server.sh
```

## 故障排除

1. 事务无法回滚：检查是否正确配置了数据源代理
2. 状态机无法执行：检查 JSON 定义是否正确，服务方法是否存在
3. Seata Server 连接问题：检查网络和配置

## 参考文档

- [Seata 官方文档](https://seata.io/zh-cn/docs/overview/what-is-seata.html)
- [AT 模式](https://seata.io/zh-cn/docs/overview/what-is-seata.html#at-模式)
- [Saga 模式](https://seata.io/zh-cn/docs/overview/what-is-seata.html#saga-模式) 