# RabbitMQ 开发教程

使用 Python 连接服务器上的 RabbitMQ，完成消息发送、消费、路由和失败处理。所有示例在 PyCharm 中直接运行，**Parameters 留空**。

## 目录

1. [先明确程序和服务器的关系](#1-先明确程序和服务器的关系)
2. [配置 PyCharm 并连接服务器](#2-配置-pycharm-并连接服务器)
3. [创建队列并发送一条文本消息](#3-创建队列并发送一条文本消息)
4. [启动消费者接收消息](#4-启动消费者接收消息)
5. [交换机和绑定怎样决定消息去向](#5-交换机和绑定怎样决定消息去向)
6. [运行订单消息示例](#6-运行订单消息示例)
7. [让两个业务各收到一份消息](#7-让两个业务各收到一份消息)
8. [确认机制与重新投递](#8-确认机制与重新投递)
9. [处理失败与死信](#9-处理失败与死信)
10. [代码结构与 API 说明](#10-代码结构与-api-说明)
11. [接入真实业务需要补充什么](#11-接入真实业务需要补充什么)
12. [排查连接和运行问题](#12-排查连接和运行问题)
13. [运行设置速查](#13-运行设置速查)

按顺序运行第 2～9 节。每节先操作、观察结果，再对照代码。文中的代码片段用于解释已有脚本，不需要复制成新的文件。

## 1. 先明确程序和服务器的关系

RabbitMQ 是一个独立运行的消息服务器。Python 程序通过客户端库 Pika 与它通信。

这份项目里有两类程序：

- **生产者 Producer**：发布消息，发布后可以退出。
- **消费者 Consumer**：接收消息，执行业务，通常持续运行。

RabbitMQ 本身负责消息接收、路由和存储；邮件发送、积分更新等业务由消费者执行。

```mermaid
sequenceDiagram
    participant P as 本机生产者
    participant R as 服务器上的RabbitMQ
    participant C as 本机消费者
    P->>R: 发布消息
    R->>R: 路由到队列并保存
    Note over R: 消费者未启动时，消息等待
    C->>R: 启动后订阅队列
    R->>C: 投递消息
    C->>C: 执行业务
    C->>R: 确认处理完成
```

| 名称 | 定义 | 用途 |
| --- | --- | --- |
| 消息 Message | 程序之间传递的数据 | 例如订单号、用户 ID 和事件时间 |
| 队列 Queue | RabbitMQ 中保存待处理消息的资源 | 消费者暂时不在线时保留消息 |
| 交换机 Exchange | RabbitMQ 中决定消息应进入哪些队列的资源 | 按配置路由消息 |
| Broker | 消息服务器 | 本教程中指 RabbitMQ 服务 |
| 异步处理 | 当前流程不等待后续业务全部完成 | 例如订单接口不等待通知发送完成 |

一条消息进入队列，只表示任务已交给消息系统，不表示业务已经完成。队列容量也不是无限的，任务持续积压时需要排查消费者。

## 2. 配置 PyCharm 并连接服务器

### 2.1 确认连接配置

| 配置项 | 本项目使用的值 |
| --- | --- |
| 管理页面 | [http://62.234.16.180:15672/](http://62.234.16.180:15672/) |
| Python 连接地址 | `62.234.16.180:5672` |
| 用户名 | `tianshiyang` |
| 密码 | 保存在根目录 `.env` |
| Vhost | `/` |
| 资源名称前缀 | `ai_lab.learn` |

**15672 用于浏览器管理页面，5672 用于 Python 的 AMQP 连接。** 网页能登录，不能证明 5672 也可访问。[官方端口说明](https://www.rabbitmq.com/docs/networking)

根目录 `.env` 中的连接配置格式为：

```dotenv
RABBITMQ_URL=amqp://tianshiyang:YOUR_PASSWORD@62.234.16.180:5672/%2F
RABBITMQ_PREFIX=ai_lab.learn
```

本机已有实际连接配置。手动配置时，把 YOUR_PASSWORD 替换为自己的密码，保留其他服务的环境变量。

- **AMQP**：客户端和 RabbitMQ 使用的消息通信协议；Pika 在这里使用 AMQP 0-9-1。
- **Vhost，虚拟主机**：RabbitMQ 内部独立的资源和权限空间。这里用默认空间 `/`。
- **%2F**：URL 中对斜杠的编码，表示 vhost 名 `/`。
- **资源前缀**：本项目用来区分资源的名称前半部分，不是权限隔离机制。

如果密码包含 `@`、`/`、`#` 等 URL 特殊字符，需要对密码部分做 URL 编码。`.env` 不提交到 Git；`.env.example` 用于记录配置项，不放真实密码。

### 2.2 选择项目解释器

用 PyCharm 打开整个项目：

```text
C:\Users\Lenovo\Desktop\python项目\ai-lab
```

Python Interpreter 选择：

```text
C:\Users\Lenovo\Desktop\python项目\ai-lab\.venv\Scripts\python.exe
```

项目需要 Python 3.14 或更高版本。依赖已安装；换电脑或重建环境时，在 PyCharm 的 Terminal 中切到项目根目录，执行：

```powershell
uv sync --locked
```

| 工具或文件 | 作用 |
| --- | --- |
| pika | Python 客户端，负责连接、发布、订阅和确认 |
| python-dotenv | 从 .env 读取配置 |
| uv | 安装依赖并管理项目 Python 环境 |
| .venv | 项目独立的 Python 环境 |
| pyproject.toml | 声明项目需要哪些包 |
| uv.lock | 记录锁定的依赖版本 |

安装检查命令：

```powershell
uv run python -c "import pika; import dotenv; print(pika.__version__); print('dotenv OK')"
```

已安装版本为 pika 1.4.4、python-dotenv 1.2.3。本机不需要另外安装 RabbitMQ 服务。

### 2.3 运行连接检查

在 PyCharm 选择 **RabbitMQ-连接检查**，或打开 [check_connection.py](check_connection.py) 右键运行。

预期输出：

```text
AMQP 连接成功，通道编号：1
已通过 AMQP 登录；创建、读写队列的权限在后续练习中验证。
```

```mermaid
sequenceDiagram
    participant I as PyCharm
    participant P as check_connection.py
    participant R as RabbitMQ
    I->>P: 使用项目解释器运行
    P->>P: 读取.env
    P->>R: 建立连接并登录vhost
    R-->>P: 登录成功
    P->>R: 建立通道
    R-->>P: 通道可用
    P->>R: 关闭连接
    P-->>I: 显示检查结果
```

**连接 Connection** 是程序与服务器的通信连接。**通道 Channel** 是连接内的逻辑操作通路；队列声明、发送和接收都通过通道完成。同一连接可以有多个通道，这里先使用一个。

检查失败就先看第 12 节，连接成功再继续。这个脚本不创建队列，不收发消息；它验证了登录和通道建立，不代表所有资源权限都已验证。

### 2.4 PyCharm 的运行约定

项目 `.run` 中提供六个运行配置，均不需要启动参数：

| 配置 | 文件 |
| --- | --- |
| RabbitMQ-连接检查 | check_connection.py |
| RabbitMQ-01-发送文本 | hello_send.py |
| RabbitMQ-02-接收文本 | hello_receive.py |
| RabbitMQ-03-创建订单资源 | topology.py |
| RabbitMQ-04-发送订单 | publisher.py |
| RabbitMQ-05-消费订单 | consumer.py |

自动创建的配置如有问题，检查 **Run → Edit Configurations**：

- Working directory 为项目根目录 `ai-lab`。
- 勾选 Add content roots to PYTHONPATH，保证能导入 `rabbitMQ学习`。
- Parameters 留空。
- 同一个消费者需要启动两份时，启用 Allow multiple instances。提供的配置已允许多实例。

[PyCharm 运行配置说明](https://www.jetbrains.com/help/pycharm/run-debug-configuration-python.html)

运行选项在各脚本顶部修改，保存后点运行。正在运行的进程不会自动采用源码修改；需要替换它时，先在对应 Run 窗口停止，再运行。

## 3. 创建队列并发送一条文本消息

### 3.1 操作

先不要运行消费者。打开 [hello_send.py](hello_send.py)，顶部设置为：

```python
message_text = "你好，RabbitMQ！"
```

运行后应看到：

```text
已发送：你好，RabbitMQ！
目标队列：ai_lab.learn.hello.q
```

这一次运行完成了：连接服务器、声明队列、发布一条消息、关闭连接。

```mermaid
sequenceDiagram
    participant P as hello_send.py
    participant R as RabbitMQ
    P->>R: 创建连接和通道
    P->>R: queue_declare声明hello队列
    R-->>P: 队列已存在且参数一致，或创建成功
    P->>R: confirm_delivery开启发布确认
    P->>R: basic_publish发布文本
    R->>R: 默认交换机将消息路由到hello队列
    R-->>P: 确认发布
    P->>R: 关闭连接
```

### 3.2 观察结果

打开管理页面，选择 vhost `/`，进入 **Queues / Queues and Streams**，找到：

```text
ai_lab.learn.hello.q
```

如果原先没有消息，也没有消费者，等待页面刷新后应看到：

| Ready | Unacked | Total |
| --- | --- | --- |
| 1 | 0 | 1 |

- **Ready**：等待投递的消息数量。
- **Unacked**：已投递但还未确认完成的数量。
- **Total**：两者之和。

改成 `message_text = "第二条消息"` 再运行。没有消费者时，Ready 应再增加 1。

### 3.3 队列是怎样创建的

对应代码在默认前缀下相当于：

```python
channel.queue_declare(
    queue="ai_lab.learn.hello.q",
    durable=True,
    arguments={"x-queue-type": "classic"},
)
```

| 参数 | 作用 |
| --- | --- |
| queue | 队列名称，服务器通过这个名字标识队列 |
| durable=True | 持久化队列定义，使其能在正常重启后恢复 |
| x-queue-type=classic | 明确使用经典队列类型 |

**声明 declare**：不存在则创建；已存在则核对参数。重复声明不是清空或覆盖，参数不一致可能报 PRECONDITION_FAILED。

### 3.4 消息是怎样发送的

```python
channel.confirm_delivery()
channel.basic_publish(
    exchange="",
    routing_key="ai_lab.learn.hello.q",
    body=message_text.encode("utf-8"),
    properties=pika.BasicProperties(delivery_mode=2),
    mandatory=True,
)
```

- `exchange=""`：使用 RabbitMQ 自带的默认交换机。
- `routing_key`：此处填写队列名，默认交换机会将消息路由到同名队列。
- `body`：消息正文，encode 把文字变为 UTF-8 字节。
- `delivery_mode=2`：将消息标记为持久化；它与队列的 durable 是两个独立设置。
- `confirm_delivery()`：开启发布确认。
- `mandatory=True`：没有任何队列匹配时要求退回；本例确认模式下 Pika 会报告异常。

默认交换机为什么可以按队列名路由，第 5 节再展开。现在先确认：发送程序退出后，消息仍保存在队列中。

## 4. 启动消费者接收消息

### 4.1 操作

运行 [hello_receive.py](hello_receive.py)，应打印前面发送的文字：

```text
收到：你好，RabbitMQ！
收到：第二条消息
```

程序继续等待属于正常状态。保持它运行，再运行 hello_send.py 发送一条文字，消费者应继续接收。

两个程序分别使用自己的 Run 窗口。发送程序执行完会退出，消费者持续运行；要停止消费者，点它的红色停止按钮。

```mermaid
sequenceDiagram
    participant C as hello_receive.py
    participant R as RabbitMQ
    C->>R: 声明同名同参数的队列
    C->>R: basic_consume注册订阅
    Note over C: start_consuming进入接收循环
    R->>C: 投递一条消息
    C->>C: 执行on_message回调并打印正文
    C->>R: basic_ack确认完成
    R->>R: 移除已确认消息
```

页面刷新后，成功消费的消息不再计入队列 Total。

### 4.2 注册消费者与接收循环

```python
channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    queue="ai_lab.learn.hello.q",
    on_message_callback=on_message,
    auto_ack=False,
)
channel.start_consuming()
```

| API / 参数 | 作用 |
| --- | --- |
| basic_qos(prefetch_count=1) | 限制这个消费者尚未确认的消息数量为 1 |
| basic_consume | 注册队列订阅和收到消息后的处理函数 |
| on_message_callback=on_message | 将函数交给 Pika，收到消息时调用 |
| auto_ack=False | 不自动确认，由代码在处理后确认 |
| start_consuming | 持续处理连接事件和收到的消息 |

`basic_consume` 不创建队列，所以脚本前面另有 queue_declare。两个文本脚本都声明同名同参数队列，因此先启动哪一个都可以。

### 4.3 回调函数接收什么

```python
def on_message(ch, method, properties, body):
    print(f"收到：{body.decode('utf-8', errors='replace')}", flush=True)
    ch.basic_ack(delivery_tag=method.delivery_tag)
```

**回调函数 Callback**：由 Pika 在收到消息时调用的函数。不是你手动调用，也不意味着自动新增一个线程。

| 参数 | 内容 |
| --- | --- |
| ch | 收到消息的通道 |
| method | 投递信息，例如 delivery_tag 和 redelivered |
| properties | 附加属性，例如消息 ID、编码 |
| body | 正文字节 |

**Ack** 是消费者的处理确认。delivery_tag 是当前通道内本次投递的编号，必须使用收到消息的原通道确认。它不是订单号，也不是永久消息 ID。

这里的业务只是打印；真实业务需要完成后再 ack。第 8 节会观察确认前停止程序时发生什么。

## 5. 交换机和绑定怎样决定消息去向

### 5.1 先区分三个对象

| 对象 | 负责什么 | 是否保存待消费消息 |
| --- | --- | --- |
| 交换机 Exchange | 接收发布请求，根据路由规则选择目标队列 | 不负责保存待消费消息 |
| 队列 Queue | 保存消息，供消费者接收 | 是 |
| 绑定 Binding | 记录某个交换机向某个队列转发消息的规则 | 否，它是一条配置关系 |

**绑定不是一个处理消息的程序，也不是消息必须经过的独立节点。** 它是 RabbitMQ 保存的配置。

例如这一条绑定：

| 交换机 | 绑定键 | 目标队列 |
| --- | --- | --- |
| ai_lab.learn.order.events | order.paid | ai_lab.learn.order.email.q |

意思是：消息发布到这个交换机，并符合 order.paid 规则时，将消息路由到这个队列。

### 5.2 建立绑定与发布消息是两种操作

建立绑定：

```python
channel.queue_bind(
    exchange="ai_lab.learn.order.events",
    queue="ai_lab.learn.order.email.q",
    routing_key="order.paid",
)
```

这个调用只登记规则，**不发布消息**。交换机和队列需要先创建。

发布消息：

```python
channel.basic_publish(
    exchange="ai_lab.learn.order.events",
    routing_key="order.paid",
    body=json.dumps(event, ensure_ascii=False).encode("utf-8"),
    properties=pika.BasicProperties(delivery_mode=2),
    mandatory=True,
)
```

其中 event 是订单字典。这个调用发布一条消息，但**不创建绑定**。

两个 API 都有 routing_key 参数，含义分别是：

| 所在 API | routing_key 表示 |
| --- | --- |
| queue_bind | 这个队列接收消息时的匹配条件，也称绑定键 |
| basic_publish | 本次发布消息携带的路由键 |

### 5.3 RabbitMQ 按什么顺序处理

本项目的订单交换机类型是 topic，绑定键为完整的 order.paid。这个绑定只匹配同样的路由键。

```mermaid
sequenceDiagram
    participant T as topology.py
    participant R as RabbitMQ
    participant P as publisher.py
    participant C as consumer.py
    T->>R: 创建交换机和邮件队列
    T->>R: 绑定order.paid到邮件队列
    Note over R: 资源与规则就绪，此时尚未发消息
    P->>R: 发布到order.events，路由键order.paid
    R->>R: 查找这个交换机的绑定
    R->>R: 匹配order.paid，写入邮件队列
    Note over R: 没有消费者时，消息保留在队列
    C->>R: 订阅邮件队列
    R->>C: 投递消息
```

逐项核对：

1. 先根据 exchange 找到目标交换机。
2. 再按该交换机的类型，用消息的 routing_key 匹配已有绑定。
3. 每个匹配的目标队列接收消息。
4. 消费者从自己订阅的队列接收消息。

消费者订阅的是**队列**，不是交换机。生产者发布的是**交换机**，不需要逐一列出消费者。

### 5.4 改一个值，结果有什么不同

假设这个交换机只有上面一条绑定：

| 发布到的交换机 | 消息路由键 | 结果 |
| --- | --- | --- |
| ai_lab.learn.order.events | order.paid | 进入邮件队列 |
| ai_lab.learn.order.events | order.cancelled | 无匹配队列 |
| 另一个存在的交换机 | order.paid | 由另一个交换机自己的绑定决定 |

无匹配队列时，交换机不会存着消息等待以后补绑定。本例启用了 mandatory 和发布确认，Pika 会报告 UnroutableError。目标交换机根本不存在则是另一类错误，通常会关闭通道并报告 NOT_FOUND。

队列名包含 email 不会自动产生任何订阅；JSON 里有 `event_name="order.paid"` 也不能代替 basic_publish 的 routing_key。路由与正文是两套信息。

### 5.5 四种交换机类型

| 类型 | 匹配规则 |
| --- | --- |
| direct | 消息路由键必须与绑定键完全相同 |
| topic | 按点分隔的路由键匹配，绑定键支持通配符 |
| fanout | 忽略 routing_key，转发给所有绑定队列 |
| headers | 按消息头属性匹配 |

topic 示例：

| 绑定键 | 匹配 | 不匹配 |
| --- | --- | --- |
| order.paid | order.paid | order.cancelled |
| order.* | order.paid、order.cancelled | order、order.email.sent |
| order.# | order、order.paid、order.email.sent | user.registered |

`*` 匹配恰好一段，`#` 匹配零段或多段，一段指点号分隔的部分。本教程不需要修改交换机类型；已存在的交换机不能通过同名声明直接改变类型。[官方 topic 教程](https://www.rabbitmq.com/tutorials/tutorial-five-python)

### 5.6 默认交换机

默认交换机的名字是空字符串 `""`。创建队列时，RabbitMQ 自动建立一条到默认交换机的绑定，绑定键就是队列名。

所以文本示例可以写：

```python
exchange = ""
routing_key = "ai_lab.learn.hello.q"
```

它仍经过交换机，只是不需要你再手动 queue_bind。

## 6. 运行订单消息示例

### 6.1 创建订单相关资源

打开 [topology.py](topology.py)，保持：

```python
enable_points_queue = False
```

运行后，管理页面应出现：

| 类型 | 名称 | 用途 |
| --- | --- | --- |
| topic 交换机 | ai_lab.learn.order.events | 接收订单事件 |
| direct 交换机 | ai_lab.learn.order.failure | 接收邮件队列转出的死信 |
| 队列 | ai_lab.learn.order.email.q | 等待邮件处理 |
| 队列 | ai_lab.learn.order.email.failed.q | 保存正常转发到此的失败消息 |

打开 **Exchanges → ai_lab.learn.order.events → Bindings**，确认 order.paid 对应邮件队列。

topology.py 按顺序声明交换机、失败队列、邮件队列及其绑定。声明可重复执行，但参数要相同。死信配置第 9 节解释，先保持代码不动。

### 6.2 发布一条订单

打开 [publisher.py](publisher.py)，设置：

```python
demo_order_id = "202609060001"
simulate_failure = False
message_count = 1
```

运行后输出包含：

```text
已发送 order.paid：order_id=202609060001，event_id=...
```

未启动订单消费者时，邮件队列 Ready 增加 1。

实际正文是 JSON，字段如下：

| 字段 | 作用 |
| --- | --- |
| event_id | 事件 ID，本例每次生成一个新值 |
| event_name | order.paid，表明事件类型 |
| occurred_at | 使用 UTC 记录时间 |
| order_id | 订单号 |
| user_id | 用户 ID |
| total_amount | 金额字符串 99.00 |
| simulate_failure | 控制邮件处理是否模拟失败 |

JSON 是通用的数据文本格式。这里先将 Python 字典转成 JSON，再编码为字节发送。RabbitMQ 不校验订单字段；校验由消费端执行。

同一个订单号发布两次会得到两个 event_id，本例没有去重，会处理两次。

### 6.3 消费订单

打开 [consumer.py](consumer.py)，设置：

```python
queue_kind = "email"
delay_seconds = 0
stop_after_one = False
```

运行后应输出：

```text
收到消息：redelivered=False，body=...
模拟发送支付通知成功：order_id=202609060001
已发送 ack。
```

若消息曾投递过，redelivered 也可能是 True。它只是重新投递的标记，不证明业务之前已经执行完成。

```mermaid
sequenceDiagram
    participant P as publisher.py
    participant R as RabbitMQ
    participant C as consumer.py
    participant S as order_service.py
    P->>P: build_order_event生成数据
    P->>R: 发布订单事件
    R->>R: 路由到邮件队列
    R-->>P: 发布确认
    C->>R: 订阅邮件队列
    R->>C: 投递订单消息
    C->>S: parse_order_event解析和校验
    S-->>C: 返回字典
    C->>S: handle_order_event模拟业务
    S-->>C: 正常返回
    C->>R: ack
```

这里没有实际发送邮件或更新积分，只打印模拟结果。保持消费者运行，更换 publisher.py 的 demo_order_id 后再次运行，可以继续观察消息消费。

## 7. 让两个业务各收到一份消息

### 7.1 新增积分订阅

先停止正在运行的订单消费者。topology.py 改为：

```python
enable_points_queue = True
```

运行一次，增加 `ai_lab.learn.order.points.q`，也绑定到订单交换机的 order.paid。

同一交换机的绑定现在是：

| 绑定键 | 目标队列 |
| --- | --- |
| order.paid | ai_lab.learn.order.email.q |
| order.paid | ai_lab.learn.order.points.q |

### 7.2 先发消息，再看两个队列

publisher.py 设置：

```python
demo_order_id = "both-001"
simulate_failure = False
message_count = 1
```

运行一次。没有消费者时，两个队列 Ready 应各增加 1。**一次发布可以进入多个队列，每个队列保存自己的一份。**

```mermaid
sequenceDiagram
    participant P as 生产者
    participant E as 订单交换机
    participant M as 邮件队列
    participant S as 积分队列
    P->>E: 发布一次order.paid
    E->>E: 找到两个匹配的目标队列
    E->>M: 路由一份消息
    E->>S: 路由一份消息
    Note over M,S: 两个队列独立保存、消费和确认
```

### 7.3 分别启动两个消费者

1. consumer.py 中设 `queue_kind = "email"`、`delay_seconds = 0`，运行。
2. 保持该实例运行，将源码中的 queue_kind 改成 `"points"`，保存。
3. 再运行一个实例。第一个进程已经读取 email，第二个进程读取 points。
4. 邮件实例打印模拟通知，积分实例打印模拟增加积分。

若 PyCharm 提示停止已有进程，确认配置启用了 Allow multiple instances。

这两个队列不共享消费状态。邮件处理失败，不会让积分队列里的消息也自动失败。

新绑定不补发历史消息。enable_points_queue 改回 False 也不会删除已创建的积分队列；积分消费者停止后继续发布，它会积压消息。积分示例没有死信配置，无效消息被拒绝时会丢弃。[官方发布订阅教程](https://www.rabbitmq.com/tutorials/tutorial-three-python)

## 8. 确认机制与重新投递

### 8.1 两种确认分别确认什么

| 确认 | 发送方 → 接收方 | 表示什么 |
| --- | --- | --- |
| Publisher Confirm | RabbitMQ → 生产者 | 服务器对发布结果的确认 |
| Consumer Ack | 消费者 → RabbitMQ | 消费者确认这次处理完成 |

生产者收到 confirm 不意味着业务已经完成。消费者 ack 与发布确认是独立机制，不能互相替代。[官方确认机制](https://www.rabbitmq.com/docs/confirms)

```mermaid
sequenceDiagram
    participant P as 生产者
    participant R as RabbitMQ
    participant C as 消费者
    P->>R: 发布消息
    R-->>P: Publisher Confirm
    R->>C: 投递消息
    C->>C: 执行业务
    C->>R: Consumer Ack
    R->>R: 移除已确认消息
```

图只说明确认关系。消费者已运行时，投递消息与生产者收到 confirm 的先后可能不同。

### 8.2 观察 Unacked

停止已有邮件消费者，将 consumer.py 改成：

```python
queue_kind = "email"
delay_seconds = 20
stop_after_one = False
```

运行消费者，再用 publisher.py 发一条正常消息。消费者显示“模拟处理 20 秒”时，到邮件队列页面观察：

| 时刻 | 预期状态 |
| --- | --- |
| 尚未投递 | Ready |
| 已投递、模拟处理期间 | Unacked 增加 1 |
| 处理完成并 ack | Unacked 回落 |

页面统计有刷新延迟。短任务可能来不及观察，所以这里设置模拟耗时。

### 8.3 确认前停止进程

再发一条消息。消费者开始等待 20 秒时，在它的 Run 窗口点停止，之后把 delay_seconds 改回 0，重新运行。

服务器检测到连接关闭后，会将未确认消息重新入队。有其他消费者时，它可能立刻被接收；没有其他消费者时，Ready 会回升。

重新启动的消费者通常会收到 redelivered=True 的同一条消息。强制结束进程时不一定打印退出提示；网络异常也可能要等心跳检测后才重新投递。

业务已经完成、ack 却未成功传到服务器时，同样可能重新投递，所以真实业务需要防重复处理。

### 8.4 同一个队列启动两个消费者

consumer.py 设置 email、delay_seconds=2，运行两个实例。publisher.py 设置 message_count=6、simulate_failure=False，运行一次。

两个消费者会分担队列内的消息，不保证严格各三条。

**Prefetch** 是未确认消息上限。本例 prefetch_count=1，让一个消费者完成手里这条后再接下一条。它不是线程数。

对比第 7 节：

- 两个队列：各自收到同一事件的一份。
- 一个队列、两个消费者：共同处理这个队列中的消息；正常分发时一条交给其中一个，异常重新投递仍可能重复。

完成后恢复 message_count=1、delay_seconds=0。

## 9. 处理失败与死信

### 9.1 制造一次业务失败

保持一个普通邮件消费者运行。publisher.py 设置：

```python
demo_order_id = "fail-001"
simulate_failure = True
message_count = 1
```

运行后发布应成功，消费者会报告模拟失败。发送成功与业务失败可以同时发生，因为它们是不同阶段。

```mermaid
sequenceDiagram
    participant P as publisher.py
    participant R as RabbitMQ
    participant C as consumer.py
    participant S as order_service.py
    P->>R: 发布带模拟失败字段的事件
    R-->>P: 发布确认
    R->>C: 投递邮件队列中的消息
    C->>S: 调用业务处理
    S-->>C: 抛出ValueError
    C->>R: nack，requeue=False
    R->>R: 按死信配置转发到失败交换机
    R->>R: 按email.failed绑定路由到失败队列
```

### 9.2 Nack 和 requeue

消费者失败分支执行：

```python
ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
```

| 设置 | 含义 |
| --- | --- |
| basic_nack | 否定确认，拒绝这次投递 |
| requeue=True | 回到原队列，可能很快再次投递 |
| requeue=False | 不回原队列；按死信配置转交，没有配置则丢弃 |

不是任何 Python 异常都会自动触发 nack。本例只在捕获到对应数据或模拟业务错误时调用它，连接异常会继续抛出。

### 9.3 死信规则在哪里配置

**死信 Dead Letter** 是因拒绝、过期等原因从原队列转出的消息。**死信交换机 DLX** 接收这些消息；**死信队列 DLQ** 是绑定到它、用于保存这些消息的队列。

邮件队列设置了：

```python
arguments = {
    "x-queue-type": "classic",
    "x-dead-letter-exchange": "ai_lab.learn.order.failure",
    "x-dead-letter-routing-key": "email.failed",
}
```

失败队列另外绑定：

```python
channel.queue_bind(
    queue="ai_lab.learn.order.email.failed.q",
    exchange="ai_lab.learn.order.failure",
    routing_key="email.failed",
)
```

第一段说明原队列把死信交给哪个交换机、使用哪个路由键。第二段说明该交换机把 email.failed 送往哪个队列。

这是普通交换机和队列加上配置后的用途；名称中有 failed 并不会自动启用失败处理。

当前经典队列的默认死信转发，在目标不可用等情况下可能丢失。nack 不是失败队列的可靠收件确认。[官方死信说明](https://www.rabbitmq.com/docs/dlx)

### 9.4 查看失败消息

正常转发后，邮件队列数量回落，`ai_lab.learn.order.email.failed.q` 的 Ready 增加。

1. 在页面打开失败队列，展开 Get messages。
2. 数量填 1。
3. Ack mode 选择带 **requeue true** 的选项，查看后放回。
4. 点击 Get Message(s)，检查正文中的订单号和 simulate_failure。
5. Headers 中通常能看到 x-death，包括来源队列、原因和次数；本例原因通常为 rejected。

Get messages 会实际取消息，并非完全无副作用的查询。放回也可能影响投递标记或顺序，别使用删除模式进行普通查看。

### 9.5 恢复正常发送

将 publisher.py 的 simulate_failure 改回 False，更换订单号，再运行。

失败队列中原来的消息不会自动重试，修改 Python 变量也不会改变已发消息的正文。这个示例未实现自动回放。重新处理失败消息需要先修复原因，再决定如何补偿。

## 10. 代码结构与 API 说明

### 10.1 文件职责

```mermaid
flowchart TD
    P["publisher.py<br/>组装与发布事件"] --> F["config.py<br/>配置与连接"]
    C["consumer.py<br/>订阅与确认"] --> F
    T["topology.py<br/>资源声明"] --> F
    C --> S["order_service.py<br/>校验与业务处理"]
    F --> R["RabbitMQ"]
```

| 文件 | 职责 |
| --- | --- |
| [config.py](config.py) | 从 .env 读取配置，创建连接 |
| [check_connection.py](check_connection.py) | 检查登录和通道建立 |
| [hello_send.py](hello_send.py) | 文本发布示例 |
| [hello_receive.py](hello_receive.py) | 文本接收示例 |
| [topology.py](topology.py) | 声明交换机、队列和绑定 |
| [publisher.py](publisher.py) | 组装订单事件、发布并等待确认 |
| [consumer.py](consumer.py) | 注册订阅、调用业务、确认或拒绝 |
| [order_service.py](order_service.py) | 解析校验订单、模拟业务 |

业务模块不持有 RabbitMQ 通道，也不调用 ack。consumer.py 依据业务返回结果决定确认方式。资源名称直接写在调用处，不需要查另一份资源常量表。

源码里的：

```python
queue = f"{resource_prefix}.order.email.failed.q"
```

默认对应 `ai_lab.learn.order.email.failed.q`。resource_prefix 只负责名称前半部分，来自 .env。

### 10.2 连接 API

```python
parameters = pika.URLParameters(url)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
```

| 调用 | 作用 |
| --- | --- |
| URLParameters(url) | 解析 URL，返回参数对象，还没有连接服务器 |
| BlockingConnection(parameters) | 建立连接并登录，成功后返回连接对象 |
| connection.channel() | 在连接内建立通道 |
| connection.close() | 关闭连接及其中的通道 |
| with open_connection() | 离开代码块时自动关闭连接 |

本项目参数：

| 参数 | 值 | 用途 |
| --- | --- | --- |
| heartbeat | 60 秒 | 与服务器协商心跳，帮助发现断连 |
| socket_timeout | 5 秒 | 限制底层连接建立等待 |
| stack_timeout | 10 秒 | 限制整个连接建立过程 |
| connection_attempts | 1 | 单次启动只尝试建立一次连接 |
| blocked_connection_timeout | 15 秒 | 限制服务器因资源告警阻塞连接时的等待 |

这些不是所有操作的统一超时，也不代表自动重连。消费者没有消息时会持续等待。

BlockingConnection 的调用可能阻塞当前线程；不要把同一连接随意跨线程共享。模拟耗时使用 connection.sleep，让 Pika 仍能处理心跳等事件。[Pika BlockingConnection 文档](https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html)

### 10.3 声明、绑定、发布 API

| API | 关键参数与含义 |
| --- | --- |
| exchange_declare | exchange 是名字，exchange_type 是路由类型，durable 控制定义持久化 |
| queue_declare | queue 是队列名，arguments 是类型、死信等附加配置 |
| queue_bind | exchange 与 queue 指定绑定两端，routing_key 指定匹配条件 |
| confirm_delivery | 在当前通道开启发布确认；更换通道需重新开启 |
| basic_publish | 向交换机发布正文、路由键和属性 |
| BasicProperties | 保存附加信息，如内容格式、编码、持久化标记、消息 ID |

发布时需区分：

- content_type、content_encoding 是描述性属性，不会替你序列化或验证正文。
- message_id 由应用设置，RabbitMQ 不因为 ID 相同就自动去重。
- mandatory 关心是否有匹配队列，不要求消费者此刻在线。
- 本例确认模式下，根据是否抛出异常判断发布完成，不使用 basic_publish 的返回值做布尔判断。
- 断线时发布结果可能不确定：服务器可能已经接收，但确认没传回来。

### 10.4 消费 API

| API | 作用及注意 |
| --- | --- |
| basic_qos | 限制未确认消息数量 |
| basic_consume | 注册订阅，返回消费者标识；不创建队列 |
| start_consuming | 进入消费事件循环 |
| basic_ack | 确认投递，默认仅确认指定的一条 |
| basic_nack | 拒绝投递，requeue 决定是否放回原队列 |
| stop_consuming | 停止消费循环；本例处理一条后退出时使用 |

on_message_callback 要传函数 `on_message`，不要写 `on_message()` 提前调用。

method.delivery_tag 用于当前通道确认，properties.message_id 用于应用追踪事件，二者不能互换。

### 10.5 阅读代码需要的 Python 语法

| 写法 | 含义 |
| --- | --- |
| enable_points_queue = True | 普通变量赋值，开启积分资源声明 |
| def main() -> None | 定义入口函数，类型提示说明不返回业务结果 |
| if __name__ == "__main__" | 直接运行文件才执行入口；import 时不启动消费者 |
| from ... import ... | 引用其他模块里的函数或变量 |
| def f(*, fail=False) | 星号后参数必须按名称传入，例如 f(fail=True) |
| : str、-> dict | 类型提示，不会自动完成运行时字段校验 |
| cast(str, url) | 给类型检查器的提示，不会把 None 转成有效连接字符串 |
| event.get("order_id") | 从字典读取字段，缺失返回 None |
| raise ValueError(...) | 抛出错误，交给调用方处理 |
| try / except / else | 尝试执行；匹配异常走 except；无异常走 else |
| f"订单：{order_id}" | 将变量内容写入字符串 |
| range(message_count) | 按指定次数循环，序号从 0 开始 |
| flush=True | 及时输出到 Run 窗口 |

consumer.py 内部定义回调，是为了使用本次连接及 delay、once 设置。字段校验和业务处理在独立函数中，避免把所有逻辑写在回调里。

## 11. 接入真实业务需要补充什么

当前示例具备发布确认、手动消费确认和邮件死信路由，但业务处理仍是打印。接入真实业务时，需要处理数据库写入、重复消息和服务故障。

### 11.1 幂等处理

**幂等**：同一业务重复执行，不重复产生结果。例如同一笔支付事件处理两次，积分只增加一次。

消息可能重新投递，因此不能只依赖“通常只收到一次”。常见设计是在数据库中记录业务处理标识，用唯一约束防止重复，并让去重记录与业务变更在同一个事务中提交。

**事务**：一组数据库操作一起成功或一起回滚。若先记录“已处理”，业务修改却失败，下次就会错误地跳过。

外部邮件或支付调用不能直接纳入本地数据库事务，还要结合外部系统的幂等能力或可靠任务记录。

### 11.2 Outbox

“订单已经写库，程序却在发布前退出”会导致事件遗漏。

**Outbox** 是数据库中的待发送事件表：订单和待发送事件在同一事务中保存，由后台程序发布，收到 confirm 后标记完成。

```mermaid
sequenceDiagram
    participant A as 订单接口
    participant D as 数据库
    participant W as 发布程序
    participant R as RabbitMQ
    participant C as 消费者
    A->>D: 同一事务写订单和待发事件
    D-->>A: 提交成功
    W->>D: 查询待发事件
    W->>R: 发布
    R-->>W: confirm
    W->>D: 标记已发送
    R->>C: 投递
    C->>D: 去重及业务写入，同一事务
    D-->>C: 提交成功
    C->>R: ack
```

这张图是接入数据库后的设计，不是当前代码已实现的功能。发布成功但标记失败仍可能重复发送，Outbox 不能代替消费者幂等。

### 11.3 有限重试

| 错误 | 处理思路 |
| --- | --- |
| JSON 格式错误、缺必填字段 | 记录原因并进入失败处理，原样重试通常无效 |
| 外部服务短暂超时 | 限制调用时间，按间隔和次数重试 |
| RabbitMQ 断连 | 恢复连接、通道和订阅，处理不确定的发布结果 |
| 已完成业务但 ack 失败 | 允许重投，通过幂等避免重复结果 |

**TTL** 是存活时间，例如 x-message-ttl=5000 表示队列中消息的存活时间上限为 5000 毫秒。结合死信可以实现等待后重试，但不是精准定时器。

不要无限立即 requeue=True，持续失败的消息可能高频反复投递。当前代码没有自动重试或重连，这些需要单独实现或配合进程管理。

### 11.4 运行与部署

- 消费者作为独立进程持续运行，不在 HTTP 请求中调用 start_consuming。
- BlockingConnection 的等待不要直接放进 FastAPI 的 async def 事件循环。
- 日志记录事件 ID、订单号、队列、结果、耗时和错误类型，不输出凭据。
- 监控 Ready、Unacked、Consumers、失败队列数量及服务器内存、磁盘状态。
- 停机时尽量停止接新任务并完成处理中任务；未确认消息可能重新投递。

**仲裁队列 Quorum queue** 使用多数副本协作保存数据。单机创建它不等于获得多机保护，队列类型要与实际节点数和恢复要求一起设计。本例明确使用 classic。

生产环境还需配置加密传输、访问权限和容量。参考 [RabbitMQ 可靠性指南](https://www.rabbitmq.com/docs/reliability) 与 [生产部署指南](https://www.rabbitmq.com/docs/production-checklist)。

## 12. 排查连接和运行问题

### 12.1 按顺序定位

```mermaid
flowchart TD
    A["运行失败"] --> B{"能导入pika吗"}
    B -->|否| C["检查项目解释器和依赖"]
    B -->|是| D{"AMQP连接成功吗"}
    D -->|否| E["检查5672、账号和vhost"]
    D -->|是| F{"资源声明成功吗"}
    F -->|否| G["检查资源参数与权限"]
    F -->|是| H["检查路由绑定与消费日志"]
```

| 现象 | 优先检查 |
| --- | --- |
| No module named pika | PyCharm 是否使用项目 .venv；依赖是否安装 |
| No module named rabbitMQ学习 | 项目根目录、Add content roots to PYTHONPATH |
| ProbableAuthenticationError | 用户名、密码 |
| ProbableAccessDeniedError | vhost 名称、用户对该空间的权限 |
| ACCESS_REFUSED | 操作所需的资源权限 |
| NOT_FOUND | 是否先运行 topology.py，是否看错 vhost |
| PRECONDITION_FAILED / inequivalent arg | 同名队列或交换机的参数不一致 |
| UnroutableError | 当前交换机下没有匹配的队列绑定 |
| Ready 持续增加 | 消费者是否在线，处理能力是否不足 |
| Unacked 长时间不降 | 是否正在模拟延迟，业务是否卡住或未确认 |
| 发布后 Ready 一直是 0 | 消费者可能已经立即取走；也要核对队列与 Unacked |
| 修改 .env 后仍用其他地址 | Run 配置或系统环境变量可能覆盖文件，修改后需重启 |
| 中文乱码 | 运行配置设置 PYTHONIOENCODING=utf-8 |

### 12.2 5672 超时怎么查

在本机 PyCharm Terminal 的 PowerShell 中执行：

```powershell
Test-NetConnection 62.234.16.180 -Port 5672
```

主要看 TcpTestSucceeded。True 表示端口可连接，不代表账号已认证；下一步仍需运行 check_connection.py。

如果 False：

1. 云服务器安全组允许本机公网出口 IP 访问 TCP 5672。
2. 检查服务器系统防火墙。
3. 确认服务监听，以及 Docker 部署是否发布了宿主机端口。

以下命令需要登录服务器执行，不是在本机执行：

```bash
sudo ss -lntp | grep -E ':(5672|15672)\b'
sudo ufw status
```

UFW 如果是 inactive，不需要为了本次排查启用它；如果启用且缺少规则，可以按实际公网 IP 添加：

```bash
sudo ufw allow from YOUR_PUBLIC_IP to any port 5672 proto tcp
```

YOUR_PUBLIC_IP 要替换为本机公网出口 IP。云安全组单个 IPv4 来源通常写为该 IP 加 /32，不是本机 192.168.x.x 地址。

Docker 部署还需查看：

```bash
docker ps --format "table {{.Names}}\t{{.Ports}}"
```

应有宿主机到容器的端口映射，例如 0.0.0.0:5672->5672/tcp。仅显示 5672/tcp 通常不代表已发布到宿主机。

Compose 对应配置为：

```yaml
ports:
  - "5672:5672"
  - "15672:15672"
```

修改原部署前先确认数据卷，不要直接删除容器重装。服务在容器内监听 5672，不能证明外部访问已经打通。

### 12.3 权限和资源冲突

在管理页面 **Admin → Users → tianshiyang** 检查对 vhost `/` 的权限：

| 权限 | 用途 |
| --- | --- |
| Configure | 声明、管理资源 |
| Write | 写入消息等操作 |
| Read | 读取消息等操作 |

绑定和死信声明也会涉及资源权限检查，登录管理页面成功并不表示这些操作一定有权限。

遇到同名资源参数冲突，可以在 .env 换一个前缀：

```dotenv
RABBITMQ_PREFIX=ai_lab.learn2
```

重启脚本并重新声明，页面查找也改成这个前缀。它会创建另一组资源，不会迁移已有消息。

Purge 是清空待处理消息，Delete 是删除队列，不是刷新按钮。

## 13. 运行设置速查

修改文件顶部选项，保存，再运行对应脚本。Parameters 始终留空。

| 目标 | 文件 | 设置 |
| --- | --- | --- |
| 检查连接 | check_connection.py | 无需修改 |
| 发送文字 | hello_send.py | message_text = "你好" |
| 接收文字 | hello_receive.py | 无需修改 |
| 创建订单资源 | topology.py | enable_points_queue = False |
| 发布正常订单 | publisher.py | demo_order_id = "demo-001"，simulate_failure = False，message_count = 1 |
| 消费邮件队列 | consumer.py | queue_kind = "email"，delay_seconds = 0，stop_after_one = False |
| 一条后退出 | consumer.py | stop_after_one = True；空队列时仍等待 |
| 模拟慢处理 | consumer.py | delay_seconds = 20 |
| 模拟失败 | publisher.py | simulate_failure = True |
| 批量发送 | publisher.py | message_count = 6 |
| 创建积分订阅 | topology.py | enable_points_queue = True |
| 消费积分队列 | consumer.py | queue_kind = "points" |

```mermaid
sequenceDiagram
    participant U as 你
    participant P as PyCharm
    participant R as RabbitMQ管理页面
    U->>P: 修改设置并保存
    U->>P: 运行对应脚本
    P-->>U: 查看运行输出
    U->>R: 检查队列与绑定
    U->>P: 完成后恢复默认设置
```

完成标准：

- [ ] 连接检查成功。
- [ ] 没有消费者时，发布使 Ready 增加。
- [ ] 启动消费者后能收到消息，ack 后队列数量回落。
- [ ] 能区分 queue_bind 与 basic_publish，一个登记规则，一个发布消息。
- [ ] 订单事件可以正常发布和处理。
- [ ] 两个匹配队列各得到一份消息。
- [ ] 确认前停止消费者后，消息可以重新投递。
- [ ] 模拟失败时，失败队列正常收到转发消息。
- [ ] 能区分同队列多消费者与多队列订阅。

本教程描述的远程结果需要在实际连接成功后验证。本地模拟检查只能检查代码分支，不能证明服务器上的收发、死信和重投已经通过。
