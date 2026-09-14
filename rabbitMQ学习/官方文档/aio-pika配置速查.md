# aio-pika 速通：企业项目常用配置（基于 10.0.1）

> 格式约定：每个参数按 `名字：含义，作用` 词条式解释；是枚举就把所有取值列全。
> 代码只保留骨架，解释全在下面的词条里。
> aio-pika 底下是 aiormq，走同一套 AMQP——**队列参数（x-\*）、交换机类型、ack 语义和 pika 完全一致**，语义词条不重复，见《pika配置速查》。这份只讲 aio-pika 的 API 形态 + 异步新增的坑。

---

## 1. 必须会：五件套

### ① 连接

```python
connection = await aio_pika.connect("amqp://user:pass@mq.example.com:5672/%2F?heartbeat=30")
channel = await connection.channel()
```

**词条：**

- `connect(url)`：建连接返回 Connection，**协程**——aio-pika 里所有操作都要 await，忘了就是造了个没跑的协程，静默无效
- `heartbeat`：心跳间隔（秒），写 URL query。**枚举语义**：`0`=禁用；不传=broker 默认 60；正整数=自定义。生产设 30 左右，防防火墙/云 LB 掐空闲连接。URL query 还认 `timeout`（连接超时秒数）和 `name`（连接名，管理台里显示谁是谁）
- URL 本体：`amqp://用户:密码@主机:端口/vhost`，vhost 为 `/` 时写 `%2F`；TLS 换 `amqps://`
- `async with connection:`：连接是上下文管理器，出块自动 close（topology.py 的 `connect()` 就是这么包的）
- **生命周期铁律**：连接/信道只能在事件循环里建——模块顶层 `import` 发生在 `asyncio.run` 之前，没有循环可 await。跨模块共享就把"建连+声明拓扑"包成 `asynccontextmanager`，用 `async with` 控制存活范围
- 启动期重试：aio-pika 没有 pika 的 `connection_attempts`/`retry_delay`，首连失败直接抛异常——要么自己写 for + sleep 循环，要么交给部署编排；**运行中**断线重连用第 3 节的 `connect_robust`

### ② 声明

```python
exchange = await channel.declare_exchange("order.events", aio_pika.ExchangeType.TOPIC, durable=True)
queue = await channel.declare_queue("order_paid", durable=True, arguments={"x-message-ttl": 30_000})
await queue.bind(exchange, routing_key="notify.*.sms")
```

**词条：**

- `declare_exchange(名字, type=, durable=)`：`aio_pika.ExchangeType` 标准的 **4 种**：`DIRECT` 精确匹配 / `FANOUT` 广播 / `TOPIC` 通配符（`*` 一个词、`#` 零或多个词）/ `HEADERS` 按消息头匹配（少用）；枚举里另有 3 个插件类型 `X_DELAYED_MESSAGE`/`X_CONSISTENT_HASH`/`X_MODULUS_HASH`，broker 装了对应插件才有意义。不传 type 默认 DIRECT
- `declare_queue(名字, durable=, arguments=)`：`durable` **2 种**，`True`=队列定义写盘重启还在（生产恒 True）/ `False`=重启就没。`arguments` 就是 pika 那个字典：`x-message-ttl`、`x-dead-letter-exchange`、`x-max-priority`、`x-queue-type` 全塞这里，取值语义见 pika 速查。名字传 `None` = broker 起名的匿名队列（RPC 回复队列用）
- `queue.bind(exchange, routing_key=)`：绑定动作挂在队列对象上，不在 channel 上
- 潜规则同 pika：声明幂等，但参数必须和已存在队列**完全一致**，否则 406 `PRECONDITION_FAILED` 并关闭信道——练习二·下半场要撞的坑
- `channel.get_exchange(名字)` / `channel.get_queue(名字)`：只拿引用不声明——拓扑别处已建好、我只发/只收时用（producer.py 用的 get_exchange）

### ③ 发布

```python
await exchange.publish(
    aio_pika.Message(
        body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        content_type="application/json",
    ),
    routing_key="notify.verify.sms",
)
```

**词条：**

- `Message`：消息体和属性一体构造，`body` 是 **bytes**，str 要自己 encode
- `delivery_mode`：**2 种**：`NOT_PERSISTENT`(1)=临时不落盘 / `PERSISTENT`(2)=写盘重启还在——生产用后者
- `content_type` / `content_encoding`：约定写 `application/json` / `utf-8`，消费者照此解析
- **publisher confirms 默认开**：`connection.channel()` 默认 `publisher_confirms=True`，每次 publish 等 broker 确认收到才返回——等于 pika 的 `confirm_delivery()` 常开。要 pika 那种裸吞吐才显式传 False
- `mandatory`：aio-pika **默认 True**（pika 默认 False，方向相反）。但光有它不报错——路由不到时 broker 发回 basic.return，默认被吞掉。要真抛异常：开信道时 `connection.channel(on_return_raises=True)`，publish 处抛 `DeliveryError`（只在 confirms 开着时可用）
- 默认交换机：`channel.default_exchange.publish(msg, routing_key="队列名")`——名字空串那个，按名直投

### ④ 消费：限流 + 手动确认

```python
await channel.set_qos(prefetch_count=50)

async for message in queue.iterator():
    ...  # 全部处理完成
    await message.ack()
```

**词条：**

- `set_qos(prefetch_count=)`：消费者最多同时"预支"多少条未 ack。**枚举语义**：`0`=不限（消息全量压过来，可能撑爆）；`1`=严格逐条（教程值）；`10~100`=企业吞吐常见值。作用在这条信道上，所以多通道要各开各的信道互不干扰（consumer.py 一通道一信道的原因）
- `queue.iterator()`：消费循环写法，`async for` 一条条出；等价写法是回调式 `await queue.consume(async回调)`，长驻服务两样都行
- `message.ack()`：手动确认，**协程，必须 await**。iterator 不传 `no_ack=True` 就是手动确认模式
- delivery_tag 封装在 message 对象里，ack/nack 不用自己带号

### ⑤ 失败处理

```python
await message.nack(requeue=False)      # 或 await message.reject(requeue=False)
```

**词条：**

- `nack(requeue=)`：**默认 `requeue=True`**——空参 nack 是"立刻重投回本队列"，坏消息会无限死循环；要送死信必须显式 `requeue=False`（配 `x-dead-letter-exchange`）
- `reject(requeue=)`：单条版，语义同 nack，但**默认 `requeue=False`**——两个方法默认值相反，用哪个都行，别被默认值阴了
- `async with message.process():`：出块自动 ack、块内抛异常自动 nack 的糖——生产代码常用，练手动 ack 时机时别用（练习二的要求就是练这个）

### 三条实战提醒（比配置更容易翻车）

1. **一切皆协程**：`ack` / `publish` / `declare_*` 忘了 await 不报错（顶多一条 RuntimeWarning），动作根本没发生——练习二的 ack 少 await 就是这条。
2. **事件循环里不能塞阻塞调用**：一个进程一个循环，`time.sleep`、同步 DB 驱动、`requests` 会把**全部**消费者一起冻住——sleep 用 `asyncio.sleep`，库用 async 版。
3. **连接建在循环里、活在循环里**：模块顶层建不了（import 早于 asyncio.run），循环结束连接作废。跨模块共享用 asynccontextmanager 包"建连+声明"。

---

## 2. pika → aio-pika 对照表

| pika | aio-pika | 备注 |
|---|---|---|
| `BlockingConnection(URLParameters(url))` | `await aio_pika.connect(url)` | |
| `conn.channel()` | `await connection.channel()` | confirms 默认开 |
| `queue_declare(queue=, durable=, arguments=)` | `await channel.declare_queue(...)` | |
| `exchange_declare(exchange=, exchange_type=, durable=)` | `await channel.declare_exchange(名, type=, durable=)` | type 换枚举 |
| `queue_bind(queue=, exchange=, routing_key=)` | `await queue.bind(exchange, routing_key=)` | 挂在队列上 |
| `basic_publish(exchange=, routing_key=, body=, properties=)` | `await exchange.publish(Message(...), routing_key=)` | 体和属性合进 Message |
| `basic_qos(prefetch_count=)` | `await channel.set_qos(prefetch_count=)` | |
| `basic_consume(queue=, on_message_callback=)` | `queue.iterator()` 或 `await queue.consume(cb)` | 回调也是 async |
| `basic_ack(delivery_tag=)` | `await message.ack()` | tag 藏进对象 |
| `basic_nack(delivery_tag=, requeue=)` | `await message.nack(requeue=)` | 默认 requeue=True |
| `confirm_delivery()` | 不用调 | channel() 默认已开 |
| — | `connect_robust(url)` | 断线自动重连，pika 没有 |

---

## 3. 按需查：高频场景

### 断线自动重连（aio-pika 独门福利）

```python
connection = await aio_pika.connect_robust(RABBITMQ_URL)
channel = await connection.channel()          # 拿到的是 RobustChannel
queue = await channel.declare_queue("order_paid", durable=True)
await channel.set_qos(prefetch_count=50)
await queue.consume(on_message)
```

- `connect_robust()`：断线后自动重连，并把这个连接上做过的**声明、绑定、set_qos、consume 全部重放**——pika 速查"三条提醒"里的"消费端要有重连"它替你干了
- 它救连接不救消息：断线瞬间 unacked 的消息 broker 会重投，幂等照样得自己有（练习二那套）

### 延迟/重试（TTL + 死信）

队列参数与 pika 逐字相同，字典塞进 `arguments`：

```python
retry_queue = await channel.declare_queue(
    "order_paid.retry", durable=True,
    arguments={
        "x-message-ttl": 30_000,                    # 躺 30 秒
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "order_paid",  # 过期后经默认交换机回主队列
    },
)
```

词条（TTL / DLX / dl-routing-key / 队头阻塞）见 pika 速查第 2 节，一个字不用改。练习三的重试、练习五的延迟投递全靠这套。

### 优先级插队（练习二·下半场）

```python
queue = await channel.declare_queue("sms", durable=True, arguments={"x-max-priority": 10})
await exchange.publish(aio_pika.Message(body=body, priority=9), routing_key="notify.verify.sms")
```

- 两件事缺一不可：队列开 `x-max-priority`，消息带 `priority`；只做一件没效果
- 坑两条：已存在队列改参数重声明 → 406（①的潜规则）；prefetch 一大优先级肉眼失效（下半场验收 2 要解释的）

### 队列防膨胀

```python
queue = await channel.declare_queue("user_behavior_log", durable=True, arguments={
    "x-max-length": 100_000,
    "x-overflow": "reject-publish",
})
```

词条同 pika：`x-max-length` 最多攒多少条；`x-overflow` **2 种**：`drop-head`=丢最老的（默认，悄悄丢）/ `reject-publish`=拒新的（要 confirms 才感知得到被拒）

### 只查数不消费（练习五 status.py 要用）

```python
queue = await channel.declare_queue("sms", passive=True)
print(queue.declaration_result.message_count)   # Ready 条数
```

- `passive=True`：只探测不创建；队列**不存在**时抛 404 并关信道（信道废了得重开）
- `declaration_result.message_count`：只含 Ready，**不含 unacked**——unacked 看管理台或管理 HTTP API

### 生产端确认送达（订单/支付类）

aio-pika 默认已开（③的 publisher confirms），比 pika 省一步。要感知"路由不到任何队列"，开 `on_return_raises`：

```python
channel = await connection.channel(on_return_raises=True)
try:
    await channel.default_exchange.publish(msg, routing_key="order_paid")
except aio_pika.exceptions.DeliveryError:
    ...  # 记日志 / 落库兜底
```

---

## 4. 认识即可（低频，看懂别人代码就行）

| 词条 | 含义/作用 |
|---|---|
| `await queue.get(no_ack=False)` | 主动拉一条；默认 `fail=True` 队列空抛 `QueueEmpty`，传 `fail=False` 变成返回 None。轮询/批处理脚本用 |
| `await queue.purge()` | 清空队列不删队列（测试清场常用） |
| `await queue.delete()` | 连队列本体一起删（改参数前的那步） |
| `Message(message_id=, headers=)` | 幂等键、传 trace_id 的惯用位置 |
| `Message(expiration=毫秒)` | 单条 TTL，也收 timedelta；坑：队列只查队头，别指望每条不同 TTL 排队过期 |
| `correlation_id` / `reply_to` | MQ 版 RPC 的请求-响应关联 |
| `declare_queue(None, exclusive=True)` | 匿名+本连接专属+断开自动删——RPC 临时回复队列 |
| `URL 加 ?name=xxx` | 给连接起名，管理台 Connections 列表直接认出是谁 |
| `aio_pika.pool.Pool` | 连接池/信道池，高并发发布才需要 |
| `x-queue-type` 三种 | classic / quorum（官方推荐新项目）/ stream——见 pika 速查① |

## 5. 基本不用（别记，用到再查）

- `publisher_confirms=False`：关确认换吞吐，只在批量灌数据时考虑
- `set_qos(prefetch_size=)`：按字节限流，RabbitMQ 没实现，设了白设
- `channel_number=`：手动指定信道编号
- `ExchangeType.X_DELAYED_MESSAGE` 等三个插件类型：broker 装插件才有意义，需要时再说
- 直接用 aiormq：aio-pika 的底层，封装掉的细节一般不用碰
