# Celery 5.6 配置速查

这份文件只做查表。第一次学习请按章节阅读。

## 应用与序列化

| 配置 | 优先级 | 含义 | 常见建议 |
| --- | --- | --- | --- |
| `broker_url` | ⭐⭐⭐ | 任务消息的 broker 地址 | 通过环境变量注入 |
| `result_backend` | ⭐⭐⭐ | 状态与结果存储地址 | 不需要结果时可不配置或对任务 `ignore_result=True` |
| `task_serializer` | ⭐⭐⭐ | 任务参数序列化格式 | 通常使用 `json` |
| `result_serializer` | ⭐⭐⭐ | 结果序列化格式 | 通常使用 `json` |
| `accept_content` | ⭐⭐⭐ | worker 接受的格式白名单 | 通常仅 `['json']`，不要开放不可信 pickle |
| `timezone` | ⭐⭐ | Beat 解释 crontab 的时区 | 明确配置，例如 `Asia/Shanghai` |
| `result_expires` | ⭐⭐ | backend 中结果的过期时间 | 按查询需求设置，避免结果无限堆积 |

## 可靠性与吞吐

| 配置 | 优先级 | 含义 | 关键代价 |
| --- | --- | --- | --- |
| `task_acks_late` | ⭐⭐⭐ | 任务执行结束后再确认消息 | worker 崩溃可能导致重复执行，任务必须幂等 |
| `task_reject_on_worker_lost` | ⭐⭐ | 子进程异常丢失时让消息重新入队 | 有重复和故障循环风险 |
| `worker_prefetch_multiplier` | ⭐⭐⭐ | 每个执行槽预取的消息倍数 | 长任务常设 1；短任务可提高吞吐 |
| `task_track_started` | ⭐⭐ | 执行时写入 STARTED 状态 | 增加 backend 写入量 |
| `task_time_limit` | ⭐⭐ | 硬时间限制 | 到期会终止执行进程，必须考虑资源一致性 |
| `task_soft_time_limit` | ⭐⭐ | 软时间限制 | prefork 等支持的池中可捕获并清理 |
| `worker_max_tasks_per_child` | ⭐⭐ | 子进程处理 N 个任务后重建 | 控制泄漏，带来进程重启开销 |
| `worker_max_memory_per_child` | ⭐⭐ | 子进程超过内存后重建 | 当前任务结束后才替换子进程 |

## 路由与调度

| 配置 | 优先级 | 含义 |
| --- | --- | --- |
| `task_default_queue` | ⭐⭐⭐ | 没有命中路由时的默认队列 |
| `task_routes` | ⭐⭐⭐ | 按任务名决定队列、routing key 等 |
| `task_queues` | ⭐⭐ | 显式声明队列、exchange 和 routing key |
| `beat_schedule` | ⭐⭐⭐ | 静态周期任务表 |

## 一个适合作为起点的配置

```python
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Asia/Shanghai",
    task_default_queue="default",
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    result_expires=3600,
)
```

它不是所有项目的答案。尤其是 `acks_late=True`，只有在任务已经实现幂等后才安全启用。

## 常用命令

```bash
# 启动 worker
celery -A celery_app worker -l INFO

# 只消费指定队列
celery -A celery_app worker -l INFO -Q default,media

# 启动 Beat
celery -A celery_app beat -l INFO

# 查看活跃、已保留、已注册任务
celery -A celery_app inspect active
celery -A celery_app inspect reserved
celery -A celery_app inspect registered

# 查看 worker 在线情况
celery -A celery_app status
```
