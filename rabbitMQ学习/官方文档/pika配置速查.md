# pika 速通：企业项目常用配置（基于 1.4.4）

> 格式约定：每个参数按 `名字：含义，作用` 词条式解释；是枚举就把所有取值列全。
> 代码只保留骨架，解释全在下面的词条里。

---

## 1. 必须会：五件套

### ① 连接

```python
conn = pika.BlockingConnection(pika.URLParameters(
    "amqp://user:pass@mq.example.com:5672/%2F?heartbeat=30&connection_attempts=5&retry_delay=3"
))
ch = conn.channel()
```

**词条：**

- `heartbeat`：心跳间隔（秒）。作用：连接空闲时双方定期互发心跳，防止被防火墙/云负载均衡当成死连接掐断。**枚举语义**：`0`=禁用心跳；`不传`=用 broker 默认值（RabbitMQ 是 60）；正整数=自定义间隔。生产必设 30 左右
- `connection_attempts`：连接尝试次数。作用：服务启动时 broker 可能还没就绪，多试几次再报错
- `retry_delay`：两次尝试的间隔（秒）
- URL 本体：`amqp://用户:密码@主机:端口/vhost`，vhost 为 `/` 时必须写 `%2F`；要 TLS 就把 `amqp` 换成 `amqps`

### ② 声明队列

```python
ch.queue_declare(
    queue="order_paid",
    durable=True,
    arguments={"x-queue-type": "quorum"},
)
```

**词条：**

- `durable`：**枚举**。`True`=队列定义写盘，broker 重启后队列还在；`False`=重启就没。生产恒为 True
- `x-queue-type`：队列类型，**3 种**：
  - `classic`：经典队列，单机存储，老项目常见
  - `quorum`：法定人数队列，Raft 多节点复制，消息恒为持久化，官方推荐，新项目用它
  - `stream`：流式队列，类似 Kafka 可重复回放，特殊场景才有
- 潜规则：声明是幂等的，但参数必须和已存在的队列完全一致，否则 broker 报 406 并关闭 channel

### ③ 发布

```python
ch.basic_publish(
    exchange="",
    routing_key="order_paid",
    body=json.dumps({"order_id": "A001"}, ensure_ascii=False),
    properties=pika.BasicProperties(
        delivery_mode=pika.DeliveryMode.Persistent,
        content_type="application/json",
    ),
)
```

**词条：**

- `exchange`：交换机名。空串 `""` = **默认交换机**，消息直送 routing_key 同名的队列，最简单也最常用；具名交换机见第 2 节
- `routing_key`：路由键。用默认交换机时就是目标队列名
- `delivery_mode`：消息是否持久化，**2 种**：
  - `1` = Transient：临时消息，不落盘，broker 重启就没（`pika.DeliveryMode.Transient`）
  - `2` = Persistent：持久化消息，写盘，重启还在（`pika.DeliveryMode.Persistent`）——生产用这个
- `content_type`：内容类型标记，约定写 `application/json`，消费者照此解析

### ④ 消费：限流 + 手动确认

```python
ch.basic_qos(prefetch_count=50)
ch.basic_consume(queue="order_paid", on_message_callback=on_message)  # 不传 auto_ack 即手动

# callback 内部，处理成功后：
ch.basic_ack(delivery_tag=method.delivery_tag)
```

**词条：**

- `prefetch_count`：消费者最多同时"预支"多少条未 ack 消息。**枚举语义**：`0`=不限（消息全量压过来，消费方可能被撑爆）；`1`=严格逐条（教程值，每条都巨慢才用）；`10~100`=企业吞吐场景的常见值
- `auto_ack`：**枚举**。`True`=fire-and-forget，broker 发出即删，消费者挂了消息就丢；`False`=必须消费者 ack 才删（默认，生产用这个）
- `delivery_tag`：本次投递的编号，随消息在 `method` 里送来，ack/nack 时原样带回去

### ⑤ 失败处理

```python
ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
```

**词条：**

- `requeue`：**枚举**。`True`=消息重回本队列并立刻重投（坏消息会无限死循环，别用）；`False`=消息转投死信交换机（队列没配死信则直接丢弃）——配 `x-dead-letter-exchange` 一起用

### 三条实战提醒（比配置更容易翻车）

1. **消费逻辑必须幂等**：ack 前崩了消息会重投，按 `message_id` 或业务主键去重。
2. **BlockingConnection 不是线程安全的**：多线程各开各的 connection（不是 channel）。
3. **消费端要有重连**：`start_consuming()` 断了要能重开。

---

## 2. 按需查：高频场景

### 订单超时自动取消（TTL + 死信 = 延迟队列）

```python
ch.queue_declare(
    queue="order_wait_pay",
    durable=True,
    arguments={
        "x-queue-type": "quorum",
        "x-message-ttl": 900_000,
        "x-dead-letter-exchange": "timeout_dlx",
    },
)
ch.queue_declare(queue="order_timeout", durable=True)
ch.exchange_declare(exchange="timeout_dlx", exchange_type="fanout", durable=True)
ch.queue_bind(queue="order_timeout", exchange="timeout_dlx", routing_key="")
```

```
下单 → order_wait_pay 躺15分钟 → 过期变死信 → order_timeout → 消费者查单：已支付跳过/未支付取消
```

**词条：**

- `x-message-ttl`：消息在队列里的存活毫秒数，到期变"死信"——配了死信交换机就转投，没配就丢弃
- `x-dead-letter-exchange`：死信交换机。消息在三种情况下自动转投它：**过期**、**被 nack/reject**、**队列超长被挤掉**
- `x-dead-letter-routing-key`：转投时改用的路由键，不配就沿用原 routing_key（下个场景会用到）
- `exchange_type`：交换机类型，**4 种**：
  - `direct`：routing_key 精确相等才投递
  - `fanout`：广播，绑定的队列全投，无视 key（死信/通知场景常用）
  - `topic`：通配符匹配，`*` 一个词、`#` 零或多个词（业务路由主力，下一章主角）
  - `headers`：按消息 headers 匹配，少用
- `queue_bind`：把队列绑到交换机上，绑了才会收到它的消息

> 坑：TTL 用队列级 `x-message-ttl`，别每条消息各写不同 `expiration`——队列只检查队头，队头不过期后面全堵着。

### 失败自动重试（30 秒后回流主队列）

```python
ch.queue_declare(
    queue="order_paid",
    durable=True,
    arguments={"x-dead-letter-exchange": "retry_dlx"},   # 消费失败(nack)的消息去这
)
ch.queue_declare(
    queue="order_paid.retry",
    durable=True,
    arguments={
        "x-message-ttl": 30_000,                     # 躺 30 秒
        "x-dead-letter-exchange": "",                # 过期后转投默认交换机
        "x-dead-letter-routing-key": "order_paid",   # 默认交换机按名路由 → 回到主队列
    },
)
ch.exchange_declare(exchange="retry_dlx", exchange_type="fanout", durable=True)
ch.queue_bind(queue="order_paid.retry", exchange="retry_dlx", routing_key="")
```

```
消费失败 → nack(requeue=False) → retry_dlx → retry 队列躺30秒 → 过期死信 → 回主队列再来
```

> 重试次数要有限：消费时数重试次数（死信头 x-death 里有记录），超限落库人工处理。

### 队列防膨胀

```python
ch.queue_declare(
    queue="user_behavior_log",
    durable=True,
    arguments={
        "x-max-length": 100_000,
        "x-overflow": "reject-publish",
    },
)
```

**词条：**

- `x-max-length`：队列最多攒多少条消息，超出按 overflow 策略处理
- `x-overflow`：**2 种**：`drop-head`=丢最老的腾位置（默认，悄悄丢）；`reject-publish`=拒收新消息（publisher 开了 confirm 才感知得到被拒）

### 生产端确认送达（订单/支付类必开）

```python
ch.confirm_delivery()
try:
    ch.basic_publish(exchange="", routing_key="order_paid", body=body,
                     properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
                     mandatory=True)
except pika.exceptions.UnroutableError:
    ...  # 记日志 / 落库兜底
```

**词条：**

- `confirm_delivery()`：channel 开一次，之后每次 publish 都等 broker 确认（收到并落盘）才返回
- `mandatory`：**枚举**。`True`=路由不到任何队列时抛 `UnroutableError`（默认）；`False`=路由不到就静默丢弃

---

## 3. 认识即可（低频，看懂别人代码就行）

| 词条 | 含义/作用 |
|---|---|
| `exclusive=True` 队列 | 本连接专属、断开自动删——RPC 临时回复队列用 |
| `auto_delete` 队列 | 最后一个消费者走后自动删 |
| `passive=True` | 只探测队列存在与否（运维脚本用） |
| `message_id` / `headers` | 消息属性里做幂等、传 trace_id 的常用位置 |
| `correlation_id` / `reply_to` | MQ 版 RPC 的请求-响应关联 |
| `expiration` | 单条消息的 TTL（字符串毫秒） |
| `x-max-priority` + `priority` | 开队列优先级 + 消息插队 |
| `x-expires` | 队列闲置 N 毫秒自动删除 |
| `x-single-active-consumer` | 同队列多消费者但一主多备 |
| `amqps://` | TLS 连接，URL 换个前缀 |

## 4. 基本不用（别记，用到再查）

- `tx_select/tx_commit/tx_rollback`：AMQP 事务，慢，被 `confirm_delivery` 取代
- `flow()`：已被 RabbitMQ 弃用
- `channel_max` / `frame_max`：协议协商参数，默认没人动
- `socket_timeout` / `stack_timeout` / `locale` / `client_properties` / `tcp_options` / `blocked_connection_timeout`：连接参数边角
- `user_id` / `app_id` / `type` / `cluster_id`：消息属性冷门字段
- `internal` 交换机、`alternate-exchange`：交换机高级特性
- `basic_reject`：功能上是 `basic_nack` 的子集
