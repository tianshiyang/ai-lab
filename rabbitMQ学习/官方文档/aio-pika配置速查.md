# aio-pika 常用 API 笔记（版本 10.0.1）

一份完整代码 + 逐行讲参数，讲完在最后补充几件事。AMQP 那套语义（TTL、死信、优先级、ack）和 pika 完全一样，这份只讲 aio-pika 的用法和异步特有的坑。

## 完整代码

连上 RabbitMQ，声明一个交换机一个队列，发一条消息，再自己消费掉。常用 API 全在这十几行里：

```python
import asyncio
import json

import aio_pika

RABBITMQ_URL = "amqp://user:pass@127.0.0.1:5672/%2F?heartbeat=30"


async def main():
    connection = await aio_pika.connect(RABBITMQ_URL)          # ①
    async with connection:                                      # ②
        channel = await connection.channel()                    # ③

        await channel.set_qos(prefetch_count=10)                # ④

        exchange = await channel.declare_exchange(              # ⑤
            "notify.events",
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        queue = await channel.declare_queue(                    # ⑥
            "notify.sms",
            durable=True,
            arguments={"x-message-ttl": 60000},
        )
        await queue.bind(exchange, routing_key="notify.*.sms")  # ⑦

        message = aio_pika.Message(                             # ⑧
            body=json.dumps({"order_id": "A001"}).encode("utf-8"),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
            priority=5,
        )
        await exchange.publish(message, routing_key="notify.order.sms")  # ⑨

        async for received in queue.iterator():                 # ⑩
            print("收到:", json.loads(received.body))
            await received.ack()                                # ⑪


asyncio.run(main())
```

## 逐行讲

### ① aio_pika.connect(url)

连接的配置基本都写在 URL 里：

```
amqp://user:pass@127.0.0.1:5672/%2F?heartbeat=30&timeout=10
```

- 前半段 `amqp://用户:密码@主机:端口/vhost`：vhost 是默认的 `/` 时必须写成 `%2F`（URL 转义），直接写 `/` 会连错。要走 TLS 就把 amqp 换成 amqps。
- `heartbeat`：心跳间隔，秒。不写用服务端默认的 60；写 0 是关掉。生产上防火墙、云负载均衡会把长时间没流量的连接当死链掐掉，一般设 30 左右。
- `timeout`：建连接这个动作本身的超时秒数，不写是 60。
- URL 里还可以加 `name=xxx` 给连接起名，管理台 Connections 页里能直接认出哪个连接是谁，排障时好用。

注意它是协程函数，await 不能丢。忘了 await 不会报错，只会拿到一个"从没跑过"的协程对象，下一行用的时候才炸——aio-pika 所有方法都是协程，这个坑全篇适用，后面不再重复说。

### ② async with connection:

连接能当上下文管理器用，出了块自动 close。不这么写就得自己记着 `await connection.close()`，异常路径上很容易漏。

有句铁律顺便记这：连接和信道只能在事件循环里创建。模块顶层写 `connection = await aio_pika.connect(...)` 是语法错误——顶层不能 await，而 import 发生在 asyncio.run 之前，那会儿循环还不存在。想全项目共用一份连接，惯用做法是用 asynccontextmanager 把"建连 + 声明拓扑"包起来，谁用谁 `async with`（项目里 topology.py 的 connect() 就是这么干的）。

### ③ connection.channel(...)

开信道。三个参数都有默认值，平时不碰：

- `publisher_confirms`：默认 True。开了之后每次 publish 都要等 broker 回一句"收到了"才返回，等于 pika 里 confirm_delivery() 常开。这个默认值比 pika 激进，消息想丢都难；要极限吞吐才显式传 False。
- `on_return_raises`：默认 False。跟⑨的 mandatory 配套，见⑨。
- `channel_number`：信道编号，自动分配，不用管。

### ④ channel.set_qos(...)

消费限流。

- `prefetch_count`：这个消费者最多同时拿几条"还没 ack"的消息。0 = 不限，消息全量压过来，消费方可能被撑爆；1 = 严格一条一条来；10~100 是吞吐场景的常见值。它作用在整条信道上，所以一个进程消费多个队列、又想各限各的，就得每个队列单独开信道（consumer.py 一通道一信道的由来）。
- `prefetch_size`：按字节数限流。RabbitMQ 没实现这个，设了白设。

### ⑤ channel.declare_exchange(名字, type=, ...)

- 第一个参数：交换机名字。
- `type`：类型，能填的值——
  - `ExchangeType.DIRECT`：routing_key 完全相等才投。
  - `ExchangeType.FANOUT`：广播，绑了的队列全投，不看 key。
  - `ExchangeType.TOPIC`：通配符，`*` 匹配一个词，`#` 匹配零个或多个词。业务路由基本都用它。
  - `ExchangeType.HEADERS`：按消息 headers 匹配，少见。

  枚举里还有 X_DELAYED_MESSAGE、X_CONSISTENT_HASH、X_MODULUS_HASH 三个，是 broker 插件提供的类型，装了插件才有意义，先当不存在。不传 type 时默认 DIRECT。
- `durable`：True 写盘，broker 重启后交换机还在。生产恒 True。
- `auto_delete`：绑定的队列都没了自动删。少用。
- `passive`：只探测不创建，见文末补充。
- `internal`：只给 broker 内部转发用，客户端不能直接往里 publish。基本不用。

声明的规矩：同一个名字重复声明不报错，但参数必须和已存在的完全一致，不一致直接 406 PRECONDITION_FAILED，还会把信道关掉。

### ⑥ channel.declare_queue(名字, ...)

- 第一个参数：队列名。传 None 让 broker 起名，拿到一个匿名队列。
- `durable`：True 队列写盘，重启还在。生产恒 True。
- `exclusive`：True 表示队列只属于当前连接，连接一断队列自动删。RPC 的临时回复队列这么用，业务队列别开。
- `auto_delete`：最后一个消费者断开后自动删。
- `passive`：只探测不创建，见文末补充。
- `arguments`：RabbitMQ 的队列参数全在这个字典里，常用的键（值和语义跟 pika 那份速查完全一样）：
  - `x-message-ttl`：毫秒。消息在队列里躺够这么久就算过期。过期去哪，看下一条。
  - `x-dead-letter-exchange`：死信交换机。消息过期、被 nack(requeue=False)、队列超长被挤掉，三种情况都会转投到它。
  - `x-dead-letter-routing-key`：转投时改用的路由键。不写就沿用消息原来的 key——练习三的重试设计靠的就是这条。
  - `x-max-priority`：开优先级，一般写 10。消息要插队得队列和消息两头都设置（见⑧的 priority）。
  - `x-max-length` / `x-overflow`：队列最多攒多少条；超了怎么办——drop-head 丢最老的（默认），reject-publish 拒收新的。
  - `x-queue-type`：classic 经典单机 / quorum 多副本（官方推荐新项目）/ stream 类 Kafka。
  - `x-expires`：毫秒，队列闲置多久自动删。
  - `x-single-active-consumer`：同队列挂多个消费者但同一时刻只让一个收，一主多备。

### ⑦ queue.bind(exchange, routing_key=...)

把队列挂到交换机上。routing_key 是绑定键，topic 交换机下可以带 `*` 和 `#`，比如 `notify.*.sms` 就是"业务随便，通道是 sms 的都要"。消息投给谁，看的是消息的 routing_key 和这个绑定键匹不匹配。

### ⑧ aio_pika.Message(...)

消息 = 内容 + 属性，一个构造函数搞定。参数按常用程度排：

- `body`：消息体，必须是 bytes。传 str 在构造这一步就报错，自己先 encode。
- `delivery_mode`：两个值。NOT_PERSISTENT(1) 不落盘，broker 重启就没；PERSISTENT(2) 落盘。生产用 2。要真生效还得配 durable 队列。
- `content_type` / `content_encoding`：约定写 application/json / utf-8，告诉消费者怎么解析。
- `priority`：0 到队列 x-max-priority 之间，值大的先投。队列没开 x-max-priority 时这个字段没效果。
- `message_id`：消息唯一标识，做幂等、日志串联的惯用位置。
- `headers`：字典，装自定义头，传 trace_id 之类的元数据。
- `expiration`：单条消息的 TTL，毫秒（也收 timedelta）。坑：队列检查过期只看队头，前面压着一条没到期的，后面的全堵着——所以延迟场景都是队列级 TTL 分档建队列，不给单条消息设。
- `timestamp` / `correlation_id` / `reply_to`：RPC 那套，普通业务用不上。

### ⑨ exchange.publish(message, routing_key, ...)

- `routing_key`：这条消息往哪投，和⑦的绑定键匹配。
- `mandatory`：默认 True（pika 默认 False，方向相反，注意）。路由不到任何队列时 broker 会把消息退回，但默认情况下这个"退回"被 aio-pika 静默吞掉，你什么都感觉不到。想让它抛 DeliveryError，开信道时要传 `channel(on_return_raises=True)`，而且只在 publisher_confirms 开着时有效。
- `timeout`：等 broker 确认的超时秒数。

发到默认交换机（名字是空串那个，按队列名直投）：`channel.default_exchange.publish(msg, routing_key="notify.sms")`。

### ⑩ queue.iterator() / queue.consume(callback)

两种消费写法，效果等价：

```python
# 写法一：迭代器
async for message in queue.iterator():
    ...

# 写法二：回调
await queue.consume(on_message)   # async def on_message(message): ...
```

- `no_ack`：默认 False，手动确认模式。传 True 是自动确认，broker 发出去就删，消费者挂了消息就没了，生产别用。

### ⑪ message.ack() / nack() / reject()

处理完告诉 broker 可以删了。三个都是协程：

- `ack(multiple=False)`：确认。multiple=True 连同之前没确认的一起确认，批处理才用。
- `nack(requeue=True)`：注意默认是 requeue=True，意思是"没处理好，塞回队列立刻重投"。坏消息会无限循环，所以想让消息去死信队列，必须显式写 `nack(requeue=False)`。
- `reject(requeue=False)`：跟 nack 的区别只有两点——一次只能拒一条，而且默认就是 requeue=False。两个默认值刚好相反，用哪个都把 requeue 写明白，别靠默认值。

还有个糖：

```python
async with message.process():
    ...   # 块内正常走完自动 ack，抛异常自动 nack
```

生产代码常用，但练习二练的就是手动控制 ack 的时机，先别用。

## 补充说明

**断线自动重连。** 消费端最朴素的担忧：网络抖一下连接断了怎么办。aio-pika 有现成答案：

```python
connection = await aio_pika.connect_robust(RABBITMQ_URL)
```

拿到的连接断了会自动重连，而且你在这条连接上做过的声明、绑定、set_qos、consume 会全部重放一遍。pika 时代这些重连逻辑要自己写。但它只救连接不救消息——断线瞬间没 ack 的消息 broker 会重投，幂等还是得自己有（练习二干的就是这个）。

**只查数、不消费。** 声明队列时传 `passive=True`：队列存在就拿到引用，不存在报 404，同时信道被关掉，得重开一条。拿到的对象上有 `declaration_result.message_count`，就是队列里的消息数——注意这只是 Ready 的数，unacked 拿不到，得看管理台。练习五的 status.py 靠这个。

**主动拉一条。** `await queue.get(no_ack=False, timeout=5)`：队列空了抛 QueueEmpty，传 fail=False 就不抛、返回 None。轮询式脚本用。清空队列但不删队列是 `await queue.purge()`，测试清场常用。

**三个最容易翻车的点**，都不是参数问题，是异步模型本身：

1. 忘 await。所有方法都是协程，忘 await 不报错、静默无效——练习二那个 ack 少 await 就是活例子。
2. 循环里塞阻塞调用。一个进程一个事件循环，time.sleep、同步驱动查库、requests 会把所有消费者一起冻住。sleep 用 asyncio.sleep，库用 async 版。
3. 启动时 broker 没就绪。aio-pika 没有 pika 的 connection_attempts / retry_delay，首连失败直接抛异常，重试要么自己写循环，要么交给部署编排。

**写过 pika 的照这个表换：**

| pika | aio-pika |
|---|---|
| BlockingConnection(URLParameters(url)) | await aio_pika.connect(url) |
| conn.channel() | await connection.channel() |
| ch.queue_declare(queue=, durable=, arguments=) | await channel.declare_queue(...) |
| ch.exchange_declare(exchange=, exchange_type=, durable=) | await channel.declare_exchange(...) |
| ch.queue_bind(queue=, exchange=, routing_key=) | await queue.bind(exchange, routing_key=) |
| ch.basic_publish(exchange=, routing_key=, body=, properties=) | await exchange.publish(Message(...), routing_key=) |
| ch.basic_qos(prefetch_count=) | await channel.set_qos(prefetch_count=) |
| ch.basic_consume(queue=, on_message_callback=) | queue.iterator() 或 await queue.consume(cb) |
| ch.basic_ack(delivery_tag=) | await message.ack() |
| ch.basic_nack(delivery_tag=, requeue=) | await message.nack(requeue=) |
| ch.confirm_delivery() | 不用，channel() 默认开 |
