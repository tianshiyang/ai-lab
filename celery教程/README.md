# Celery：从任务投递到可靠运行

这套教程从零解释 Celery 的运行模型，不要求先读 RabbitMQ 教程。代码只放在讲义的独立代码块中，不替你生成 `.py` 文件；你自己按代码块创建文件、启动 worker、投递任务。

教程按当前项目依赖的 Celery 5.6 编写。示例默认使用 RabbitMQ 作为 broker、Redis 作为 result backend，并从环境变量读取连接地址：

```bash
RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672//
REDIS_URL=redis://127.0.0.1:6379/0
```

## Celery 先解决什么问题

普通函数调用发生在当前进程里，调用者必须等它返回。Celery 把一次函数调用转换成消息：调用端把“任务名 + 参数 + 调用选项”发送给 broker，worker 从队列取走消息并执行任务，结果可选地写入 backend。

```mermaid
sequenceDiagram
    participant C as 调用端
    participant B as Broker
    participant W as Worker
    participant R as Result Backend

    C->>B: send_task：任务名 + 参数 + task_id
    B-->>C: 发布完成；调用端得到 AsyncResult
    W->>B: 消费任务消息
    W->>W: 根据任务名找到函数并执行
    W->>R: 写入 SUCCESS / FAILURE 与结果
    C->>R: 按 task_id 查询状态或结果
```

Celery 不是“把函数扔到线程里”，而是跨进程、甚至跨机器的消息驱动执行系统。broker 负责传递任务，worker 负责执行，backend 负责保存结果；三者职责不能混为一谈。

## 星级规则

| 标记 | 含义 | 学习目标 |
| --- | --- | --- |
| **⭐⭐⭐ 企业常用** | 日常 Celery 项目会直接写到 | 能解释机制并独立使用 |
| **⭐⭐ 条件常用** | 特定任务形态或架构中常见 | 知道何时选择、有哪些代价 |
| **⭐ 补充了解** | 老项目、运维或少数复杂编排会遇到 | 能看懂，使用时再查 |

最先掌握：`@app.task`、`delay`、`apply_async`、`AsyncResult`、`retry` / `autoretry_for`、任务幂等、`acks_late`、`task_routes`、Beat、`chain` / `group`。

## 学习路线

| 章节 | 核心问题 |
| --- | --- |
| [01 运行模型与第一个任务](01-运行模型与第一个任务/讲义.md) | app、task、broker、worker、backend 各自做什么？ |
| [02 任务调用与结果](02-任务调用与结果/讲义.md) | `delay`、`apply_async`、`AsyncResult` 分别是什么意思？ |
| [03 重试、确认与幂等](03-重试、确认与幂等/讲义.md) | 业务失败怎样重试？worker 崩溃怎样重投？为什么必须幂等？ |
| [04 队列与路由](04-队列与路由/讲义.md) | 任务怎样进入不同队列，worker 怎样只消费指定队列？ |
| [05 Beat 周期任务](05-Beat周期任务/讲义.md) | Beat 是执行者还是调度者？为什么只能运行一个 scheduler？ |
| [06 Canvas 工作流](06-Canvas工作流/讲义.md) | `signature`、`chain`、`group`、`chord` 怎样传递结果？ |
| [07 Worker 与生产配置](07-Worker与生产配置/讲义.md) | 并发池、时间限制、预取、监控和优雅关停怎样配？ |
| [08 故障语义与上线清单](08-故障语义与上线清单/讲义.md) | Celery 能保证什么？上线前如何检查重复、丢失和积压风险？ |

配套资料：[概念地图](概念地图.md) 用于回顾任务的一生；[配置速查](celery配置速查.md) 用于查配置名，不承担第一次教学。

## 学习时始终带着三问

1. 这次调用只是发布成功，还是任务已经执行成功？
2. 当前失败属于业务异常、worker 崩溃，还是 broker / backend 不可用？
3. 如果同一个任务执行两次，业务结果还能保持正确吗？

Celery 的可靠性不是某个配置开关带来的，而是消息确认、重试策略和业务幂等共同组成的。
