# pika 配置速查：Connection / Channel / 消息

> 依据本项目实际安装的 **pika 1.4.4** 整理，参数名与默认值都从源码实测验证
> （`uv run python -c "import pika; print(pika.__version__)"`）。

## 0. 先记三层结构

配置挂在哪一层是 AMQP 协议决定的，不是 pika 随便分的：

| 层级 | 管什么 | 生效范围 | 入口 |
|---|---|---|---|
| Connection | TCP/协议层：心跳、帧大小、通道上限、重连、TLS | 整条连接 | `pika.ConnectionParameters` / `pika.URLParameters` |
| Channel | 消费节奏（prefetch）、发布确认、事务模式 | 本 channel（项目里共用一个 channel 时≈全局） | `channel.basic_qos()` 等 |
| 声明 | 队列/交换机的定义 | broker 端，可持久 | `queue_declare` / `exchange_declare`（经 channel 调用） |
| 消息 | 单条消息的属性 | 只有这一条消息 | `basic_publish(properties=...)` |

---

## 1. Connection 级：ConnectionParameters / URLParameters

两种等价写法：

```python
# 写法一：关键字参数
pika.BlockingConnection(pika.ConnectionParameters(
    host="localhost", port=5672, virtual_host="/",
    heartbeat=30, connection_attempts=3,
))

# 写法二：URL（本项目 .env 用的就是这种）
pika.BlockingConnection(pika.URLParameters("amqp://user:pass@localhost:5672/%2F?heartbeat=30"))
```

### 连接到哪

| 参数 | 默认值 | 说明 |
|---|---|---|
| `host` | `'localhost'` | broker 地址 |
| `port` | `5672` | 端口（TLS 是 5671） |
| `virtual_host` | `'/'` | vhost，逻辑隔离单元。URL 写法里 `/` 必须编码成 `%2F` |
| `credentials` | guest/guest | `pika.PlainCredentials(user, pwd)`；URL 里就是 `user:pass@` |

### 协议协商

| 参数 | 默认值 | 说明 |
|---|---|---|
| `channel_max` | `65535` | 单条连接最多同时开多少 channel，实际和 broker 取小 |
| `frame_max` | `131072` | 单帧最大字节数，和 broker 取小。大消息会自动拆帧，调大可提吞吐 |
| `heartbeat` | `None` | 心跳秒数。None=接受 broker 提议（RabbitMQ 默认 60）；0=禁用。长空闲连接建议显式设，防被中间设备掐断 |

### 重试与超时

| 参数 | 默认值 | 说明 |
|---|---|---|
| `connection_attempts` | `1` | 连接尝试次数，默认 1 次，失败即抛异常 |
| `retry_delay` | `2.0` | 两次尝试之间的间隔秒数 |
| `socket_timeout` | `10.0` | 底层 socket 单次读写超时秒数 |
| `stack_timeout` | `15.0` | 一个完整 AMQP 操作（穿完协议栈）的超时，必须 > socket_timeout |

### 其他

| 参数 | 默认值 | 说明 |
|---|---|---|
| `blocked_connection_timeout` | `None` | broker 因内存/磁盘告警阻塞发布时最多等多少秒；None=无限等 |
| `locale` | `'en_US'` | 错误消息语言 |
| `client_properties` | `None` | 附加客户端信息 dict，会显示在管理界面的连接详情里 |
| `tcp_options` | `None` | 传给 TCP socket 的选项字典（如 keepalive 相关） |
| `ssl_options` | `None` | `pika.SSLOptions(ssl_context, server_hostname)`；URL 用 `amqps://` 前缀最省事 |

### URLParameters 支持的查询参数

URL 本体（`amqp://user:pass@host:port/vhost`）之外的配置放 query string。1.4.4 实测支持：

`heartbeat`、`channel_max`、`frame_max`、`connection_attempts`、`retry_delay`、
`socket_timeout`、`stack_timeout`、`blocked_connection_timeout`、`locale`、
`client_properties`、`tcp_options`、`ssl_options`

例：`amqp://guest:guest@localhost:5672/%2F?heartbeat=30&connection_attempts=3&socket_timeout=15`

---

## 2. Channel 级：channel 上真正的"设置"只有这几个

channel 是"会话"，没有一堆持久配置；能在它上面**设置状态**的 API 全在这里：

### basic_qos —— 消费限流（最常用）

```python
channel.basic_qos(prefetch_size=0, prefetch_count=1, global_qos=False)
```

| 参数 | 说明 |
|---|---|
| `prefetch_count` | 本消费者最多同时持有多少条**未 ack** 消息；0=不限。ack 一条才给下一条 |
| `prefetch_size` | 按字节数限流。**RabbitMQ 不支持**，必须保持 0 |
| `global_qos` | False（默认）= 限制对 channel 上**每个**消费者单独生效；True = 所有消费者**共享**一个上限。（1.3 及以前这个参数叫 `global_`，1.4 改名） |

对 channel 调用一次，之后在这个 channel 上注册的消费者都遵守——想做成"项目级配置"，就在统一的 config 里创建 channel 后立刻调它。

### confirm_delivery —— 发布确认

```python
channel.confirm_delivery()
```

开启后 `basic_publish` 会等 broker 确认（已入队/已落盘）才返回；配 `mandatory=True` 时路由不到任何队列抛 `UnroutableError`，broker 拒收抛 `NackError`。生产端"确认消息真的发出去了"就靠它。

### tx_select / tx_commit / tx_rollback —— AMQP 事务

```python
channel.tx_select()                # 切到事务模式
channel.basic_publish(...)
channel.tx_commit()                # 或 channel.tx_rollback()
```

能把多条发布+ack 打包原子提交，但每次多一个同步往返，**很慢**，实际项目基本都用 confirm_delivery 替代。

### flow —— 已弃用

`channel.flow(active)` 暂停/恢复 broker 向本 channel 发送。RabbitMQ 已不支持，别用。

---

## 3. 声明级：queue_declare / exchange_declare（定义存在 broker 那边）

严格说这不是 channel 的配置，而是经 channel 声明的 **broker 端实体定义**。核心规则：
**声明是幂等的，但参数必须与已存在的定义完全一致，否则 406 报错并关 channel**——
所以生产/消费两边要共用同一份声明代码（如本项目的 work_config.py）。

### queue_declare

| 参数 | 默认 | 说明 |
|---|---|---|
| `queue` | 必填 | 队列名；传 `''` 则由 broker 生成随机名（在返回的 DeclareOk 里拿） |
| `durable` | False | 定义在 broker 重启后保留（消息还要 delivery_mode=2 才真的不丢） |
| `exclusive` | False | 只允许本连接使用，连接断开队列自动删除 |
| `auto_delete` | False | 最后一个消费者取消订阅后自动删除 |
| `passive` | False | 只探测不创建：存在→返回 DeclareOk（含积压消息数、消费者数）；不存在→404 |
| `arguments` | None | x-arguments，见下表 |

### 常用 x-arguments

| key | 作用 |
|---|---|
| `x-queue-type` | `quorum`（Raft 复制，官方推荐）/ `classic` / `stream` |
| `x-message-ttl` | 消息入队多少毫秒后过期丢弃 |
| `x-expires` | 队列多少毫秒没有任何消费者就整个删掉 |
| `x-max-length` | 队列最多攒多少条消息，超了按 overflow 策略处理 |
| `x-max-length-bytes` | 同上，按字节数 |
| `x-overflow` | 超限策略：`drop-head`（默认，丢最老的）/ `reject-publish`（拒收新的） |
| `x-dead-letter-exchange` | 消息被丢弃/过期/拒收时转发到这个交换机（死信路由） |
| `x-dead-letter-routing-key` | 死信转发时改用的 routing key |
| `x-single-active-consumer` | true 时多个消费者挂同一条队列，但同一时刻只有一个在消费（主备消费） |
| `x-max-priority` | 开启消息优先级（配消息的 priority 属性生效） |

### exchange_declare

| 参数 | 默认 | 说明 |
|---|---|---|
| `exchange` | 必填 | 交换机名 |
| `exchange_type` | `direct` | `direct` / `fanout` / `topic` / `headers` |
| `durable` | False | 同队列 |
| `auto_delete` | False | 被使用过且所有绑定解除后自动删除 |
| `internal` | False | 内部交换机，客户端不能直接 publish，只能被其他交换机绑定 |
| `passive` | False | 只探测 |
| `arguments` | None | 如 `{"alternate-exchange": "ae"}`：路由不到任何队列时改投这里 |

---

## 4. 消息级：BasicProperties（basic_publish 的 properties）

```python
pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
```

| 字段 | 说明 |
|---|---|
| `delivery_mode` | 2=Persistent 持久化，1=Transient 临时。**消息不丢的标配**（quorum 队列恒为持久化） |
| `content_type` | 如 `application/json`，约定俗成的内容类型 |
| `content_encoding` | 如 `utf-8` |
| `headers` | 自由 dict，放业务自定义头 |
| `priority` | 优先级，需队列开了 `x-max-priority` 才生效 |
| `expiration` | 本条消息的 TTL（**字符串**毫秒），优先于队列的 `x-message-ttl` |
| `message_id` | 消息 ID，做幂等消费常用 |
| `timestamp` | 创建时间（datetime 对象） |
| `correlation_id` / `reply_to` | RPC 场景：关联请求与响应、指定回复队列名 |
| `type` | 消息类型名 |
| `user_id` | 发布者身份；设置了就必须与连接用户一致，否则 broker 拒绝 |
| `app_id` | 发布方应用标识 |
| `cluster_id` | 已废弃，别用 |

---

## 5. 常见组合速记

| 目标 | 组合 |
|---|---|
| 消息绝不丢 | durable 队列 + `delivery_mode=2` + 消费端手动 ack + 发布端 `confirm_delivery()` |
| 消费端不被压垮 | `basic_qos(prefetch_count=10)` 按处理能力调 |
| 慢任务多 worker 公平分摊 | 多消费者 + `prefetch_count=1` |
| 任务超时自动进死信 | `x-message-ttl` + `x-dead-letter-exchange` + 死信队列 |
| 队列别无限膨胀 | `x-max-length` + `x-overflow=reject-publish` |
