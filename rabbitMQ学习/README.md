# Python RabbitMQ：企业异步任务、可靠投递与失败处理

这份教程只讲企业项目真正会遇到的 RabbitMQ 场景：下单后异步通知、多个系统订阅同一事件、消息可靠投递、消费幂等、延迟重试、死信归档和运维排查。

不讲生产环境不会直接使用的“Hello World”队列，也不把 RabbitMQ 当数据库或同步 RPC 来用。

贯穿全文的案例是：订单服务提交订单后发布 `order.created` 事件；订单后处理 worker 消费它，处理失败时延迟 5 秒重试，连续失败后进入失败队列等待人工或补偿任务处理。

## 1. 先确定交付目标：至少一次，而不是“恰好一次”

在分布式系统里，网络中断可能发生在任何一次确认前后。因此企业消息系统通常把目标定为 **至少一次投递**：生产者可能重发，消费者可能收到同一条消息多次。

| 责任 | 必须做的事 | 解决的问题 |
| --- | --- | --- |
| 生产者 | 持久化消息、持久化队列、Publisher Confirm | 确认 broker 已接收消息。 |
| 消费者 | 手动确认（manual ack） | 只有业务处理成功后才允许 broker 删除消息。 |
| 业务代码 | 使用 `event_id` 做幂等 | 重复消息不重复扣库存、不重复发券。 |
| 数据库与消息之间 | Transactional Outbox | 数据库已提交但进程崩溃时，事件不会丢失。 |
| 失败处理 | 延迟重试 + 失败队列 | 临时故障不打爆下游，永久失败可追踪、可补偿。 |

RabbitMQ 的 Publisher Confirm 只说明 broker 已经接管消息，消费者 ack 只说明消费者已经处理消息；两者相互独立。它们共同带来至少一次语义，而不是端到端的“只执行一次”。因此幂等不是可选优化，而是业务正确性的一部分。

## 2. 准备本地环境与依赖

开发环境可以启动带管理页面的 RabbitMQ：

```powershell
docker run -d --name ai-lab-rabbitmq `
  -p 5672:5672 -p 15672:15672 `
  -e RABBITMQ_DEFAULT_USER=app `
  -e RABBITMQ_DEFAULT_PASS=dev-password `
  rabbitmq:4-management
```

管理页面是 [http://127.0.0.1:15672](http://127.0.0.1:15672)，上面的账号为 `app` / `dev-password`。生产环境应使用独立用户、vhost、TLS 与机密管理，不能继续使用默认 `guest` 账号。

项目已在 `pyproject.toml` 中加入 `pika`。同步依赖后，在 `.env` 中配置：

```dotenv
RABBITMQ_URL=amqp://app:dev-password@127.0.0.1:5672/%2F
```

`%2F` 表示默认 vhost `/`。如果用户名、密码或 vhost 含有 `@`、`/` 等 URL 特殊字符，必须做 URL 编码。

后续示例共用 [config.py](./config.py)：

```python
from rabbitMQ学习.config import open_connection

with open_connection() as connection:
    channel = connection.channel()
```

连接是昂贵资源。Web 请求不应该每发一条消息就新建连接；实际服务会在进程内维护连接池或专用发布器。本文的短生命周期连接让示例更容易读，worker 则在整个消费期间保持连接。

## 3. 先声明拓扑，再写业务代码

一条消息的去向由“交换机类型 + routing key + binding”决定，而不是由生产者直接指定队列。案例采用下面的拓扑：

```text
订单服务
  │  order.created
  ▼
order.events（topic exchange）
  │
  ▼
order.created.q ──失败且 requeue=False──► order.retry（direct exchange）
  ▲                                          │ retry.5s
  │                                          ▼
  └──────── TTL 5 秒后死信返回 ───── order.created.retry.5s

达到最大重试次数或永久业务错误
  │
  ▼
order.failure（direct exchange） → order.created.failure.q
```

拓扑名称与 routing key 集中在 [topology.py](./topology.py)，生产者和消费者都只引用这些常量。这样改队列名、重试时间或路由规则时，不会出现两边各改一半的事故。

```python
from rabbitMQ学习.topology import declare_order_topology

with open_connection() as connection:
    channel = connection.channel()
    declare_order_topology(channel)
```

代码中使用了 durable exchange、durable queue、持久化消息以及 quorum queue。它们一起提高 broker 重启和节点故障后的数据存活概率；但“持久化”不能替代 Publisher Confirm。

`x-message-ttl`、死信交换机等队列参数在 RabbitMQ 中通常不可直接修改。示例将它们写在代码里是为了让本地拓扑可复现；生产环境建议把这些参数放在 RabbitMQ policy 或基础设施配置中，并走变更流程。已有队列的属性发生不兼容变化时，应新建版本化队列、迁移消费者后再下线旧队列，不能直接用同名声明覆盖。

还要特别注意：quorum queue 的死信转发默认仍是 at-most-once，broker 在死信转发期间故障时可能丢失消息。对本例这种“重试后仍必须处理”的链路，应在部署时为两个**源**队列启用 at-least-once dead lettering；该模式需要 quorum queue、`reject-publish` overflow、合适的长度上限和已启用的 `stream_queue` feature flag。以下命令中的 `100000` 只是容量示例，应按消息大小和可接受积压量调整：

```powershell
rabbitmqctl set_policy --apply-to queues order-reliable-dlx `
  "^(order\.created\.q|order\.created\.retry\.5s)$" `
  '{"dead-letter-strategy":"at-least-once","overflow":"reject-publish","max-length":100000}'
```

该策略使 broker 在目标队列确认接收前保留源消息，代价是更多内存和 CPU；目标不可用时源队列会积压，因而必须为队列深度和消息年龄设置告警。

### `topic` exchange：一条领域事件可以被多个系统各消费一次

案例的 `order.events` 采用 topic exchange。订单服务发布一次 `order.created`，搜索、通知、积分等服务各自建立自己的队列并绑定同一个 routing key：

```python
channel.queue_declare(queue="order.notify.q", durable=True)
channel.queue_bind(
    queue="order.notify.q",
    exchange="order.events",
    routing_key="order.created",
)

channel.queue_declare(queue="order.points.q", durable=True)
channel.queue_bind(
    queue="order.points.q",
    exchange="order.events",
    routing_key="order.created",
)
```

这两个队列都会各得到一份消息。不要把通知和积分 worker 都挂在同一个队列上：同一队列中的多个消费者是**分摊任务**，一条消息只会分配给其中一个消费者；不同业务订阅必须使用不同队列。

## 4. 生产者：订单提交后发布带身份的持久化事件

[publisher.py](./publisher.py) 中的 `publish_order_created()` 已包含：

- `event_id`：全局唯一的幂等键；消费者应以它去重。
- `schema_version`：演进消息字段时的兼容依据。
- `correlation_id=order_id`：日志、链路和故障排查时关联订单。
- `delivery_mode=Persistent`：消息应持久化。
- `confirm_delivery()`：等待 broker confirm。
- `mandatory=True`：路由不到任何队列时，pika 会抛出 `UnroutableError`，不能默默丢掉。

发布代码的调用顺序是先建立连接、声明拓扑、开启 confirm，最后发送：

```python
from decimal import Decimal

from rabbitMQ学习.publisher import publish_order_created

event_id = publish_order_created(
    order_id="202608250001",
    user_id=10086,
    total_amount=Decimal("99.00"),
)
print(event_id)
```

发送成功返回 `event_id`。若 broker 返回 nack、连接中断，或 mandatory 消息没有可路由的队列，`basic_publish()` 会抛异常；调用方必须记录告警并交给重试策略处理，不能简单地吞掉异常后仍告诉用户“下单事件已发送”。

### 订单写库与发消息之间：使用 Transactional Outbox

下面这种写法仍然有丢消息窗口：订单数据库事务已经提交，但进程在 `publish_order_created()` 前崩溃。

```python
# 不要在生产中只写成这样
create_order_in_database(...)
publish_order_created(...)
```

企业系统用 Outbox 表消除这个窗口。订单与待发送事件在**同一个数据库事务**里提交；后台 outbox publisher 轮询未发送事件，调用上面的发布函数，收到 broker confirm 后才标记为 `published`。

```python
def create_order(command: CreateOrderCommand) -> str:
    order_id = new_order_id()
    event_id = new_event_id()

    with database.transaction():
        database.insert_order(order_id, command.user_id, command.total_amount)
        database.insert_outbox(
            event_id=event_id,
            event_name="order.created",
            aggregate_id=order_id,
            payload={
                "order_id": order_id,
                "user_id": command.user_id,
                "total_amount": str(command.total_amount),
            },
            status="pending",
        )

    return order_id


def publish_pending_outbox_events() -> None:
    for event in database.lock_pending_outbox_events(limit=100):
        publish_order_created(
            order_id=event.payload["order_id"],
            user_id=event.payload["user_id"],
            total_amount=Decimal(event.payload["total_amount"]),
            event_id=event.event_id,
        )
        database.mark_outbox_published(event.id)
```

上例的 `database`、`CreateOrderCommand` 与 `new_order_id()` 是你的 ORM/数据库实现占位符。真实表至少应保存 `event_id`、事件名、payload、状态、创建时间、发送时间和错误信息。

注意：发布成功、但标记 Outbox 已发送前进程崩溃时，任务会再次发布。因此 Outbox 保证的是“不漏发”，代价是“可能重复发”；这正是消费者必须幂等的原因。

## 5. 消费者：完成业务后再 ack

[consumer.py](./consumer.py) 把消费流程固化为下面的顺序：

1. `basic_qos(prefetch_count=10)` 限制一个 worker 同时未确认的消息不超过 10 条。
2. 收到消息后校验 JSON 和 `event_id`。
3. 调用 `handle(event)` 完成业务与幂等记录。
4. 成功才 `basic_ack()`；进程在 ack 前崩溃时，消息会再次投递。
5. 不可重试错误发布到失败队列后 ack；临时错误 `basic_nack(requeue=False)`，由死信拓扑送入延迟重试队列。

启动 worker 时，把真实的业务处理函数传进去：

```python
from rabbitMQ学习.consumer import consume_order_created


def handle_order_created(event: dict) -> None:
    """在一个数据库事务中做幂等判断和所有业务写入。"""
    with database.transaction():
        inserted = database.insert_processed_event_if_absent(
            event_id=event["event_id"],
            consumer_name="order-notify-worker",
        )
        if not inserted:
            return

        # 例如创建站内信待发送记录，而不是直接调用不可靠的外部 HTTP 服务。
        database.insert_notification_outbox(
            order_id=event["order_id"],
            user_id=event["user_id"],
        )


consume_order_created(handle_order_created)
```

`processed_event` 表应给 `(consumer_name, event_id)` 建唯一索引。不能写成“先查是否存在，再插入”：多实例并发时两个 worker 都可能查到不存在。应直接尝试插入唯一记录，以数据库的唯一约束决定谁第一次处理。

`handle_order_created()` 返回后，示例才会 ack。因此业务函数要么在事务中成功提交，要么抛出异常；不要在内部捕获异常然后假装成功。

### `prefetch_count` 不是越大越好

`prefetch_count=10` 代表一个 worker 同时最多拿十条未确认消息。它避免单个慢 worker 把大量任务预取在本地，其他 worker 却闲置。

| 消费任务 | 建议起点 | 原因 |
| --- | --- | --- |
| 调第三方支付、短信、邮件 | 1～10 | 下游慢、限流严格，优先控制并发。 |
| CPU 或数据库轻量任务 | 10～100 | 可逐步提高吞吐，但要观察数据库连接与延迟。 |
| 顺序敏感的单实体任务 | 1，并按业务键分片 | RabbitMQ 不替你保证跨消费者的全局业务顺序。 |

根据消费耗时、下游容量和监控数据逐步调大；不要先设成几千。消费者 ack 超时、未确认消息持续上涨，都是 worker 堵塞或失败的信号。

## 6. 延迟重试：不要 `requeue=True` 立即回队列

某个依赖短暂超时后，下面的写法会让消息立刻回到原队列，再被立刻取出，形成空转并压垮 broker、日志和下游：

```python
# 不推荐：短暂故障时会产生紧凑重试循环
channel.basic_nack(method.delivery_tag, requeue=True)
```

本教程的重试逻辑已经在 [topology.py](./topology.py) 和 [consumer.py](./consumer.py) 中配套实现：

1. 临时异常时，消费者执行 `basic_nack(..., requeue=False)`。
2. 主队列把消息死信到 `order.retry`。
3. `order.created.retry.5s` 留住消息 5 秒。
4. TTL 到期后，重试队列将消息死信回 `order.events` 的 `order.created`。
5. 主消费者再次拿到消息；`x-death` 中的 rejected 计数已增加。
6. 已重试 3 次仍失败时，消费者先把消息发布到最终失败队列并等到 publisher confirm，再确认原消息。

`MAX_RETRY_COUNT = 3` 表示首次处理外最多再重试三次，即最多执行四次。这个数字要按业务错误类型设定：网络超时可能值得重试，字段不合法、商品不存在等永久错误应该抛 `PermanentBusinessError`，直接进入失败队列。

示例使用固定 5 秒延迟，便于理解。实际项目常用 5 秒、30 秒、5 分钟等多级重试队列，或使用 RabbitMQ 的 delayed-message 插件；无论选择哪种方式，都要有最大次数、失败归档和告警，不能无限循环。

## 7. 最终失败队列不是垃圾桶

`order.created.failure.q` 接收两类消息：

- 不可重试的业务错误，例如格式错误、引用的订单不存在。
- 达到最大重试次数后仍失败的临时错误。

失败队列消息的 `message_id`、`correlation_id` 与 `x-death` 头信息保留了原始上下文。运维或补偿程序应按以下流程处理：

1. 用 `event_id`、`order_id` 查询日志和业务数据，确认失败原因。
2. 修复数据、配置或外部依赖。
3. 通过受控的补偿脚本重新发布一条**新的**领域事件，或执行专门的补偿业务。
4. 记录处理人、原因与结果，再确认或归档失败消息。

不要在管理页面无选择地把失败队列全部 requeue。故障仍存在时会再度放大流量；人为重放也会产生重复消息，所以业务幂等仍必须保留。

## 8. 消息契约：先向后兼容，再部署消费者

消息是服务之间的公开接口。建议把事件名、版本和字段规则写进契约文档，并遵守：

- 只新增可选字段，不直接删除或重命名已有字段。
- 金额用字符串（如 `"99.00"`）传递，避免浮点误差。
- 时间用带时区的 ISO 8601 文本（示例中的 `occurred_at`）。
- 每条事件必须有稳定的 `event_id`；不应把 broker delivery tag 当业务 ID。
- 发布方先发兼容新旧消费者的版本，所有消费者升级后再移除旧字段。

发布方和消费者都应记录至少 `event_id`、事件名、`schema_version`、`order_id`、retry 次数、处理耗时和异常类型。日志不要直接打印手机号、地址、支付凭据等敏感 payload。

## 9. FastAPI 集成：接口只写库与 Outbox

`pika.BlockingConnection` 是同步 I/O。不要在 FastAPI 的 `async def` 接口里直接调用它，否则等待 broker confirm 时会阻塞事件循环。

面向 HTTP 请求，推荐的边界是：接口在事务中写订单与 Outbox，立即返回订单号；独立后台 worker 调用 `publish_pending_outbox_events()`。这样 HTTP 服务不依赖 RabbitMQ 的瞬时可用性，也避免请求超时后用户重复提交。

```python
from fastapi import FastAPI, status

app = FastAPI()


@app.post("/orders", status_code=status.HTTP_201_CREATED)
async def create_order_endpoint(command: CreateOrderCommand) -> dict[str, str]:
    # create_order 内部只提交订单和 outbox；不在这里直接使用 pika。
    order_id = create_order(command)
    return {"order_id": order_id}
```

Outbox publisher 与 RabbitMQ consumer 应作为独立进程部署、独立扩缩容。它们和 Web API 混在一个 Uvicorn 进程里会让发布/消费的生命周期、重启策略和资源上限互相影响。

## 10. 上线前检查清单

| 项目 | 检查项 |
| --- | --- |
| 连接 | 生产使用专用 user、vhost、TLS、权限最小化；连接与 channel 有重连和健康检查。 |
| 拓扑 | exchange、queue、binding 均 durable；关键队列使用 quorum queue；队列参数变更有迁移方案。 |
| 生产 | 消息 persistent、开启 Publisher Confirm、`mandatory=True`；业务写库通过 Outbox 发事件。 |
| 消费 | `auto_ack=False`；成功后 ack；有限 prefetch；数据库唯一约束实现幂等。 |
| 重试 | 有退避、上限与失败队列；永久错误不重试；失败队列有告警和处理责任人。 |
| 监控 | 队列 Ready/Unacked 数、消费者数量、发布 confirm 失败、重试率、失败队列深度和消息年龄。 |
| 容量 | 依据消息体大小、积压峰值、消费者耗时压测；不要把大文件直接塞进消息体，传对象存储 URL 与校验信息。 |

## 推荐练习顺序

1. 启动本地 broker，运行拓扑声明，在管理页面确认三个 exchange 和三个队列。
2. 调用 `publish_order_created()` 发一条订单事件，确认它进入 `order.created.q`。
3. 实现一个只打印事件的 `handle_order_created()`，再启动 `consume_order_created()`，观察成功 ack 后队列清空。
4. 让处理函数第一次抛普通异常，观察消息经过重试队列并在约 5 秒后重新投递。
5. 让处理函数持续失败，观察最多四次尝试后消息进入 `order.created.failure.q`。
6. 最后把打印逻辑替换成“唯一索引去重 + 业务写库”的事务，再加入 Outbox publisher。

可靠消息不是靠单个 API 达成的：生产者确认、消费者确认、幂等、Outbox 与可运营的失败处理缺一不可。

## 官方参考

- [Consumer Acknowledgements and Publisher Confirms](https://www.rabbitmq.com/docs/confirms)
- [Dead Letter Exchanges](https://www.rabbitmq.com/docs/dlx)
- [Time-To-Live and Expiration](https://www.rabbitmq.com/docs/ttl)
- [RabbitMQ Reliability Guide](https://www.rabbitmq.com/docs/reliability)
