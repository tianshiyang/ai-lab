# RabbitMQ 从零开始：连接服务器，发出并收到第一条消息

用 Python 连接服务器上的 RabbitMQ，练习发消息、收消息和处理失败。命令默认在 **Windows PowerShell 的项目根目录**执行，服务器命令会单独标明。

你现在的环境：

| 项目 | 配置 |
| --- | --- |
| RabbitMQ 所在位置 | 服务器 `62.234.16.180` |
| 管理页面 | [打开 RabbitMQ 管理页面](http://62.234.16.180:15672/) |
| 用户名 | `tianshiyang` |
| 密码 | 使用你已有的密码，本机已写入根目录 `.env` |
| Python 连接端口 | `5672` |
| 虚拟主机 | `/` |
| 练习资源名称前缀 | `ai_lab.learn` |

**2026-09-06 实测：管理页面可访问，账号可登录，服务器版本为 3.12.1；服务器内部监听了 5672，但本机连接服务器的 5672 超时。** 因此先完成第 2 节的端口检查，再运行收发练习。下面的预期输出是端口打通后应看到的结果，不代表当前已经完成远程收发。

本项目已安装 `pika 1.4.4` 和 `python-dotenv 1.2.3`。不用在自己的 Windows 上再装 RabbitMQ，也不用启动本地 Docker。

## 阅读顺序

1. [RabbitMQ 是干什么的](#1-rabbitmq-是干什么的)
2. [准备环境并连接服务器](#2-准备环境并连接服务器)
3. [发出第一条消息](#3-发出第一条消息)
4. [收到第一条消息](#4-收到第一条消息)
5. [认识交换机和路由](#5-认识交换机和路由)
6. [运行订单通知示例](#6-运行订单通知示例)
7. [观察消息处理中和重新投递](#7-观察消息处理中和重新投递)
8. [处理失败时消息去哪了](#8-处理失败时消息去哪了)
9. [多个消费者和多个队列](#9-多个消费者和多个队列)
10. [读懂连接配置和常用参数](#10-读懂连接配置和常用参数)
11. [后面再学的几个概念](#11-后面再学的几个概念)
12. [常见问题](#12-常见问题)
13. [命令速查与练习完成标准](#13-命令速查与练习完成标准)

## 1. RabbitMQ 是干什么的

RabbitMQ 用来接收、保存和转发消息。例如订单保存后，把发邮件的任务放进队列，由邮件程序稍后处理：

```text
下单程序 → 保存订单 → 把“需要发邮件”这件事交给 RabbitMQ → 返回结果
                                      ↓
                              邮件程序取出任务并发送
```

它属于**消息中间件**，也就是帮程序传递消息的软件。邮件、积分等业务仍由你写的消费者执行。

| 术语 | 是什么 | 有什么用 |
| --- | --- | --- |
| 消息 Message | 程序之间传递的一份数据 | 告诉另一个程序发生了什么，或者需要做什么 |
| 生产者 Producer | 发消息的程序 | 例如订单程序发出“订单已支付” |
| 队列 Queue | RabbitMQ 里存放待处理消息的地方 | 接收程序没启动时，先把消息留在这里 |
| 消费者 Consumer | 接收并处理消息的程序 | 例如接到订单消息后发通知 |
| Broker | 消息服务器，这里就是 RabbitMQ | 统一管理连接、交换机和队列 |
| 异步处理 | 发起工作后，不在当前流程里等它全部完成 | 下单页面不用一直等邮件发送 |
| 解耦 | 两个程序少依赖对方的运行细节 | 发消息的程序不必知道邮件程序部署在哪里 |
| 削峰 | 把突然涌入的任务先排队，再按处理能力执行 | 减少下游短时间内被大量请求压住的情况 |

队列能缓冲任务，但容量有限。任务进入队列也不等于业务完成；如果邮件程序一直没启动，邮件就一直不会发出。

## 2. 准备环境并连接服务器

### 2.1 进入项目根目录

打开 PowerShell，执行：

```powershell
cd "C:\Users\Lenovo\Desktop\python项目\ai-lab"
```

这里应该能看到 `pyproject.toml`、`uv.lock` 和 `rabbitMQ学习` 文件夹。

后面每新开一个终端，都先运行这条 `cd` 命令。

### 2.2 依赖包有什么用

| 名称 | 是什么 | 这里用来做什么 |
| --- | --- | --- |
| `pika` | Python 的 RabbitMQ 客户端库 | 从 Python 连接服务器、发送消息、接收消息 |
| `python-dotenv` | 读取 `.env` 文件的库 | 把连接配置读进程序 |
| `uv` | Python 项目和依赖管理工具 | 安装项目需要的包，并用项目环境运行 Python |
| `.venv` | 当前项目的 Python 虚拟环境目录 | 将本项目的包与其他项目隔开 |
| `pyproject.toml` | 项目配置文件 | 声明依赖哪些包、要求哪个 Python 版本 |
| `uv.lock` | 依赖版本记录文件 | 让不同电脑尽量安装同一组版本 |

这些包已经安装。以后换电脑或重新拉取项目，在根目录运行：

```powershell
uv sync --locked
```

验证安装：

```powershell
uv run python -c "import pika; import dotenv; print('pika:', pika.__version__); print('dotenv: OK')"
```

当前应显示：

```text
pika: 1.4.4
dotenv: OK
```

本项目要求 Python 3.14 或更高版本，这是项目自身的要求。安装一个名叫 `rabbitmq` 的 Python 包并不能替代服务器；这里需要的客户端就是 `pika`。

### 2.3 确认本地配置

根目录的 `.env` 已经配置了你的实际连接信息。如果以后手动配置，在里面加入下面这一行，将 `YOUR_PASSWORD` 换成你的密码：

```dotenv
RABBITMQ_URL=amqp://tianshiyang:YOUR_PASSWORD@62.234.16.180:5672/%2F
```

不要覆盖原有的 Redis 配置，只修改或添加 `RABBITMQ_URL`。

拆开看：

```text
amqp://用户名:密码@服务器地址:端口/虚拟主机
```

| 部分 | 含义 |
| --- | --- |
| `amqp://` | 使用 AMQP 消息协议；协议就是双方约定的通信规则 |
| `tianshiyang` | 连接用户名 |
| `YOUR_PASSWORD` | 密码占位符，运行前要替换 |
| `62.234.16.180` | RabbitMQ 服务器地址 |
| `5672` | Python 使用的 AMQP 端口 |
| `%2F` | 斜杠 `/` 的 URL 编码，表示默认虚拟主机 |

**虚拟主机 Vhost** 是 RabbitMQ 内部划分出来的独立空间。不同空间可以有同名队列，也可以设置不同权限。本教程都使用 `/`。

密码如果包含 `@`、`:`、`/`、`#` 等字符，要先做 URL 编码，否则可能被当作地址的一部分。你目前的密码不涉及这些字符。

`.env` 已被 Git 忽略；可提交的 `.env.example` 只有密码占位符。当前 `amqp://` 和管理页面的 `http://` 都没有加密；这里按现有学习环境连接，正式使用时应配置 TLS，并限制访问来源。

### 2.4 分清 15672 和 5672

这两个端口做的事不同：

```text
浏览器 ── HTTP / 15672 ──→ RabbitMQ 管理页面
Python ── AMQP / 5672 ──→ RabbitMQ 消息服务
```

所以“网页能登录”不能证明“Python 能连接”。不要把管理页面的完整网址直接填进 Python 的连接参数。[RabbitMQ 官方端口说明](https://www.rabbitmq.com/docs/networking)

在本机 PowerShell 运行：

```powershell
Test-NetConnection 62.234.16.180 -Port 5672
```

主要看：

```text
TcpTestSucceeded : True
```

如果是 `False`，先解决端口访问。Ping 不通也不一定说明服务不可用，关键看这次 TCP 检查。

### 2.5 你当前的 5672 超时，按这个顺序查

**第一步：检查云服务器安全组。**

登录云服务器控制台，找到这台服务器的安全组或防火墙入站规则：

- 协议：TCP。
- 端口：5672。
- 来源：你的本机公网出口 IP，单个 IPv4 地址后加 `/32`。
- 动作：允许。

“安全组”是云平台放在服务器外面的一层网络过滤规则。你用手机热点或换网络后，出口 IP 可能变化，需要更新来源地址。这里填的是公网出口 IP，不是本机的 `192.168.x.x`。

**第二步：在服务器检查系统防火墙。**

下面是 Ubuntu 上的命令，需要通过 SSH 登录服务器执行，不能在本机 PowerShell 直接执行：

```bash
sudo ufw status
```

如果显示 `Status: inactive`，说明 UFW 没启用，不要为了这次练习特意打开它。如果是 active，而且没有允许本机访问 5672，再按自己的实际公网 IP 添加规则：

```bash
# 把 YOUR_PUBLIC_IP 换成你的实际公网出口 IP。
sudo ufw allow from YOUR_PUBLIC_IP to any port 5672 proto tcp
```

如果服务器使用其他防火墙，检查对应规则，不要直接关闭整个防火墙。

**第三步：确认监听或 Docker 映射。**

```bash
sudo ss -lntp | grep -E ':(5672|15672)\b'
```

如果 RabbitMQ 是用 Docker 部署的，还要查看：

```bash
docker ps --format "table {{.Names}}\t{{.Ports}}"
```

端口列表中应有宿主机到容器 5672 的映射，例如 `0.0.0.0:5672->5672/tcp`；只有 `5672/tcp` 通常表示容器声明了端口，没有发布到宿主机。Docker Compose 配置通常需要包含：

```yaml
ports:
  - "5672:5672"
  - "15672:15672"
```

如果缺少映射，修改原来的部署配置后再按原部署方式更新，先确认数据卷配置；不要直接删除当前容器重装。通过管理 API 看到的“监听 5672”可能是容器内监听，并不能证明宿主机映射正确。

**第四步：回到本机重新检测。**

```powershell
Test-NetConnection 62.234.16.180 -Port 5672
uv run python -m rabbitMQ学习.check_connection
```

成功时：

```text
AMQP 连接成功，通道编号：1
已通过 AMQP 登录；创建、读写队列的权限在后续练习中验证。
```

这个脚本只登录并建立通道，不会创建或删除队列。若端口通了但提示权限错误，在管理页面打开 **Admin → Users → tianshiyang**，检查用户对 `/` 的权限：

| 权限 | 作用 |
| --- | --- |
| Configure | 创建、声明或删除队列和交换机 |
| Write | 向交换机写入消息等操作 |
| Read | 从队列读取消息等操作 |

学习空间需要相应读写权限。绑定和死信配置也会检查资源权限；能登录管理页面并不等于拥有这些权限。

## 3. 发出第一条消息

### 3.1 运行发送脚本

先不要启动消费者，在项目根目录执行：

```powershell
uv run python -m rabbitMQ学习.hello_send
```

预期输出：

```text
已发送：你好，RabbitMQ！
目标队列：ai_lab.learn.hello.q
```

`python -m rabbitMQ学习.hello_send` 的意思是“按模块运行这个目录里的 hello_send.py”。所有命令统一用这种写法，避免直接运行单个文件时找不到包。

### 3.2 到管理页面看消息

1. 打开管理页面并登录。
2. 如果页面有 Vhost 筛选，选 `/`。
3. 打开 **Queues** 或 **Queues and Streams**，不同版本名称可能略有差异。
4. 搜索 `ai_lab.learn.hello.q`。
5. 等待页面统计刷新。

如果队列原本为空，且没有消费者，应看到：

| Ready | Unacked | Total |
| --- | --- | --- |
| 1 | 0 | 1 |

这三个数字的意思：

- **Ready**：还在队列中，等着交给消费者。
- **Unacked**：已经交给消费者，但还没收到处理确认。
- **Total**：Ready 和 Unacked 的总数。

再发送两条：

```powershell
uv run python -m rabbitMQ学习.hello_send "第二条消息"
uv run python -m rabbitMQ学习.hello_send "第三条消息"
```

没有消费者时，Ready 应继续增加。即使发送脚本已经结束，消息仍然留在服务器的队列里。

### 3.3 对照源码看发送过程

打开 [hello_send.py](hello_send.py)。主要逻辑是：

```python
with open_connection() as connection:
    channel = connection.channel()
    channel.queue_declare(
        queue=HELLO_QUEUE,
        durable=True,
        arguments={"x-queue-type": "classic"},
    )
    channel.confirm_delivery()
    channel.basic_publish(
        exchange="",
        routing_key=HELLO_QUEUE,
        body=args.message.encode("utf-8"),
        properties=pika.BasicProperties(delivery_mode=2),
        mandatory=True,
    )
```

| 代码 | 是什么、有什么用 |
| --- | --- |
| `open_connection()` | 建立本机与服务器之间的连接 |
| `connection.channel()` | 在连接上开一个通道，后续通过它操作队列和消息 |
| `queue_declare` | 声明队列：不存在就创建，存在就检查参数是否一致 |
| `durable=True` | 保存队列定义，使它能在正常重启后恢复 |
| `x-queue-type: classic` | 明确使用经典队列，适合当前单机基础练习 |
| `confirm_delivery()` | 开启发送方确认，等待 RabbitMQ 对发布结果作出确认 |
| `exchange=""` | 使用 RabbitMQ 自带的默认交换机 |
| `routing_key=HELLO_QUEUE` | 默认交换机按队列名找到目标队列 |
| `body` | 消息正文，这里是编码为 UTF-8 字节的文本 |
| `delivery_mode=2` | 把这条消息标记为持久化消息 |
| `mandatory=True` | 无法路由到任何队列时要求退回消息；配合确认模式让 Pika 报错 |
| `with ...` | 离开代码块时关闭连接 |

**通道 Channel** 是一个连接内部的逻辑工作通路。同一条网络连接可以开多个通道，减少反复建立网络连接的开销。现在只开一个就够了。

队列持久化和消息持久化是两个设置：前者保存“队列”，后者保存“消息”。两者配合确认机制使用，但不能因此认为磁盘损坏、机器丢失等情况下也绝不会丢数据。

## 4. 收到第一条消息

### 4.1 打开第二个终端

在终端 B 中运行：

```powershell
cd "C:\Users\Lenovo\Desktop\python项目\ai-lab"
uv run python -m rabbitMQ学习.hello_receive
```

它会打印之前排队的消息，例如：

```text
等待 ai_lab.learn.hello.q 的消息，按 Ctrl+C 退出。
收到：你好，RabbitMQ！
收到：第二条消息
收到：第三条消息
```

程序一直停在那里等新消息，这是正常运行状态。

在终端 A 再发送：

```powershell
uv run python -m rabbitMQ学习.hello_send "我已经会发消息了"
```

终端 B 应立即收到。页面刷新后，队列 Ready 和 Unacked 都回到 0。

### 4.2 为什么接收程序没有退出

打开 [hello_receive.py](hello_receive.py)，看这部分：

```python
def on_message(ch, method, properties, body):
    print(f"收到：{body.decode('utf-8', errors='replace')}", flush=True)
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(
    queue=HELLO_QUEUE,
    on_message_callback=on_message,
    auto_ack=False,
)
channel.start_consuming()
```

**回调函数 Callback** 就是提前交给库的一个函数，等事情发生时由库调用。这里每收到一条消息，Pika 就调用一次 `on_message`。

回调的四个参数：

| 参数 | 含义 |
| --- | --- |
| `ch` | 收到消息的通道，确认时还用它 |
| `method` | 这次投递的信息，包括确认编号、是否重新投递 |
| `properties` | 消息的附加信息，例如消息 ID、内容类型 |
| `body` | 消息正文，类型为 bytes |

**Ack** 是消费者给 RabbitMQ 的处理确认：“这条消息我处理好了，可以从队列移除。”

- `auto_ack=False`：由我们在代码中手动确认。
- `basic_ack(...)`：发送确认。
- `delivery_tag`：本次投递在当前通道内的编号，用来指出确认哪一条。
- `start_consuming()`：持续等待并分发收到的消息。

`delivery_tag` 不是订单号，也不是消息的永久 ID；要在收到消息的同一个通道上确认它。

完成这节后，在终端 B 按 **Ctrl+C** 停止文本消费者，再进入订单练习。

## 5. 认识交换机和路由

前面的 `exchange=""` 使用了默认交换机。接下来自己创建交换机，决定订单消息交给哪些队列。

| 术语 | 是什么 | 有什么用 |
| --- | --- | --- |
| 交换机 Exchange | 接收生产者消息并决定送往哪些队列的组件 | 根据规则分发消息，本身不是存储消息的队列 |
| 路由键 Routing key | 发消息时附带的字符串 | 标记消息类别，例如 `order.paid` |
| 绑定 Binding | 交换机与队列之间的一条转发规则 | 告诉交换机哪些消息应该进入这个队列 |
| 拓扑 Topology | 交换机、队列和绑定组成的连接关系 | 描述消息经过哪里、最终进入哪个队列 |

订单练习的连接关系：

```text
publisher.py
    │ routing_key = order.paid
    ▼
ai_lab.learn.order.events（topic 交换机）
    │ 绑定规则 = order.paid
    ▼
ai_lab.learn.order.email.q
    │
    ▼
consumer.py → 模拟发送通知 → ack
```

常见交换机类型：

| 类型 | 转发规则 | 例子 |
| --- | --- | --- |
| direct | 路由键与绑定键完全相同才匹配 | `email.failed` 只匹配 `email.failed` |
| fanout | 忽略路由键，发送给所有绑定队列 | 同一条公告交给每个订阅者的队列 |
| topic | 按点分段匹配，支持通配符 | `order.*` 匹配 `order.paid` |
| headers | 根据消息头的键值匹配 | 按业务属性组合分发，初学暂时用不到 |

topic 的两个符号：

- `*` 匹配恰好一个单词：`order.*` 匹配 `order.paid`，不匹配 `order.email.sent`。
- `#` 匹配零个或多个单词：`order.#` 可以匹配 `order`、`order.paid`、`order.email.sent`。

这里的“单词”指点号分隔的部分。[官方 topic 示例](https://www.rabbitmq.com/tutorials/tutorial-five-python)

## 6. 运行订单通知示例

这里只模拟订单支付后的消息传递，不会实际扣款、发邮件或修改积分。所有业务结果都通过打印展示。

### 6.1 创建交换机、队列和绑定

在终端 A 运行：

```powershell
uv run python -m rabbitMQ学习.topology
```

去管理页面确认这些资源存在：

| 页面 | 名称 | 作用 |
| --- | --- | --- |
| Exchanges | `ai_lab.learn.order.events` | 接收订单消息 |
| Exchanges | `ai_lab.learn.order.failure` | 接收处理失败后的死信 |
| Queues | `ai_lab.learn.order.email.q` | 等待发送通知的消息 |
| Queues | `ai_lab.learn.order.email.failed.q` | 留待检查的失败消息 |

可以重复运行声明脚本，同名、同参数不会创建两份队列。如果同名队列参数不同，会报错；不要把声明理解成“覆盖修改”。

点击 `ai_lab.learn.order.events`，在 **Bindings** 里应能看到邮件队列及绑定键 `order.paid`。

### 6.2 发送一条订单消息

此时先不启动订单消费者：

```powershell
uv run python -m rabbitMQ学习.publisher --order-id 202609060001
```

预期输出包含：

```text
已发送 order.paid：order_id=202609060001，event_id=...
```

邮件队列的 Ready 应增加 1，失败队列数量不变。

发送的数据形状如下，实际 ID 和时间由代码生成：

```json
{
  "event_id": "每次生成的唯一消息ID",
  "event_name": "order.paid",
  "occurred_at": "2026-09-06T04:00:00+00:00",
  "order_id": "202609060001",
  "user_id": 10086,
  "total_amount": "99.00",
  "simulate_failure": false
}
```

**JSON** 是一种通用的数据文本格式，Python、Java 等语言都容易读取。RabbitMQ 只传递字节，不会替你检查这些字段是否正确。

| 字段 | 作用 |
| --- | --- |
| `event_id` | 区分不同消息，方便查日志；后续可以用于去重 |
| `event_name` | 说明发生了什么事，这里是订单已支付 |
| `occurred_at` | 记录消息事件时间，代码使用 UTC 时间 |
| `order_id` | 告诉接收方处理哪个订单 |
| `user_id` | 标识用户 |
| `total_amount` | 金额使用字符串，避免浮点表示误差 |
| `simulate_failure` | 练习用开关，让邮件处理故意失败 |

同一个订单号连续运行两次发送脚本，会生成两个不同的 event_id。这份示例没有做业务去重，会处理两次。

### 6.3 启动订单消费者

在终端 B 运行：

```powershell
cd "C:\Users\Lenovo\Desktop\python项目\ai-lab"
uv run python -m rabbitMQ学习.consumer
```

预期看到：

```text
收到消息：redelivered=False，body=...
模拟发送支付通知成功：order_id=202609060001
已发送 ack。
```

如果这条消息之前被交给过其他消费者，`redelivered` 也可能是 True。它表示重新投递，不代表一定发生过重复业务处理。

再回终端 A 发送另一条：

```powershell
uv run python -m rabbitMQ学习.publisher --order-id 202609060002
```

终端 B 应继续处理。管理页面中，成功处理后邮件队列的 Total 回落。

### 6.4 区分发送确认和处理确认

```text
生产者 ──发布──→ RabbitMQ ──投递──→ 消费者
       ←confirm──         ←ack────
```

- **Publisher Confirm**：RabbitMQ 告诉生产者发布结果。它不证明邮件已经发出。
- **Consumer Ack**：消费者告诉 RabbitMQ 这次处理完成，可以移除消息。

两个确认解决的是两个阶段的问题。示例先执行业务，再 ack；如果先 ack 再执行业务，业务中途失败时就无法依赖队列重新投递。[官方确认机制说明](https://www.rabbitmq.com/docs/confirms)

### 6.5 一条订单消息的时序图

从上往下看，每条箭头表示一次调用或消息传递。RabbitMQ 一列包含交换机、邮件队列和失败队列；发布前需要先运行 topology 创建它们。

```mermaid
sequenceDiagram
    autonumber
    participant P as 生产者 publisher.py
    participant R as RabbitMQ 服务器
    participant C as 消费者 consumer.py

    P->>R: confirm_delivery() 开启发送确认
    R-->>P: 确认模式已开启
    P->>R: basic_publish(order.paid, JSON消息)
    R->>R: 交换机按绑定规则路由到邮件队列
    R-->>P: Publisher Confirm 确认接收
    Note over P,R: 发送成功不等于业务已完成
    C->>R: basic_consume(auto_ack=False) 注册订阅
    Note over C: start_consuming() 持续等待消息
    R->>C: 投递消息，触发 on_message
    Note over R,C: 消息处于 Unacked，等待消费者确认
    C->>C: 解析 JSON，检查字段，模拟发送通知
    alt 处理成功
        C->>R: basic_ack(delivery_tag)
        R->>R: 从邮件队列移除已确认消息
    else 数据错误或模拟失败
        C->>R: basic_nack(delivery_tag, requeue=False)
        R->>R: 按死信配置转发到失败交换机及失败队列
        Note over R: 默认死信转发不是可靠到达保证
    else 确认前断开连接
        Note over C,R: 没有收到 ack 或 nack
        R->>R: 检测到连接关闭，将未确认消息重新入队
        R->>C: 有可用消费者时重新投递
    end
```

图中按“先发消息、后启动消费者”的练习顺序排列。消费者如果已经运行，投递和生产者收到 confirm 的先后可能不同；两种确认彼此独立。

对照终端看：生产者打印“已发送”对应发布确认成功；消费者打印“收到消息”对应回调开始；打印“已发送 ack”对应业务完成后发出确认。失败队列的实际数量要到管理页面查看。

## 7. 观察消息处理中和重新投递

### 7.1 故意把处理变慢

先在终端 B 用 Ctrl+C 停掉原消费者。确认没有其他邮件消费者运行，再启动：

```powershell
uv run python -m rabbitMQ学习.consumer --delay 20
```

终端 A 发一条：

```powershell
uv run python -m rabbitMQ学习.publisher --order-id slow-001
```

消费者打印“模拟处理 20 秒”时，去邮件队列页面看：

| 状态 | 预期变化 |
| --- | --- |
| 消息还未投递 | Ready 增加 |
| 正在处理这 20 秒 | Unacked 为 1 |
| 处理完成并 ack | Unacked 回到 0 |

统计刷新有延迟，短任务可能根本来不及在页面显示 Unacked，所以这一步特意增加等待时间。

**Prefetch** 是预取上限，用来限制每个消费者手中尚未确认的消息数量。代码设置 `prefetch_count=1`，这一条没处理完，就不会继续给它塞下一条。它不是“开启一个线程”，也不是整体队列容量限制。

### 7.2 在 ack 前停止消费者

保持 `--delay 20` 运行，再发一条：

```powershell
uv run python -m rabbitMQ学习.publisher --order-id retry-observe-001
```

当终端 B 显示正在模拟处理时，在 20 秒结束前按 Ctrl+C。

随后：

1. 消费者关闭连接。
2. RabbitMQ 将未确认消息重新放回可投递状态；网络异常断线时可能需要等待服务端检测到断连。
3. 没有其他消费者时，页面 Ready 应回升。
4. 重启普通消费者：

```powershell
uv run python -m rabbitMQ学习.consumer
```

应再次收到刚才的订单，通常显示 `redelivered=True`。

这就是手动确认的用途：程序拿到消息后没来得及处理完，消息还有机会交给它或另一个消费者。若业务已完成、ack 却没成功送达，也会发生重新投递，因此真实业务仍然需要防重复处理。

## 8. 处理失败时消息去哪了

### 8.1 先认识死信

**死信 Dead Letter** 是因拒绝、过期等原因从原队列转出的消息，不是说消息内容“死亡”了。

**死信交换机 DLX** 是负责接收这类消息的普通交换机；**死信队列 DLQ** 是绑定到它、用于接收这些消息的普通队列。名字里有 `failed` 并不会自动赋予队列特殊功能，真正起作用的是配置。

邮件队列设置了：

```python
arguments = {
    "x-queue-type": "classic",
    "x-dead-letter-exchange": ORDER_FAILURE_EXCHANGE,
    "x-dead-letter-routing-key": ORDER_FAILURE_ROUTING_KEY,
}
```

含义是：这条队列的消息成为死信时，交给失败交换机，并用 `email.failed` 作为新的路由键。

### 8.2 发一条故意失败的消息

让普通邮件消费者保持运行，在终端 A 执行：

```powershell
uv run python -m rabbitMQ学习.publisher --order-id fail-001 --fail
```

消费者会打印模拟失败，并执行：

```python
ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
```

术语和参数：

| 名称 | 含义 |
| --- | --- |
| Nack | 否定确认，告诉 RabbitMQ 这次没有成功处理 |
| `requeue=True` | 放回原队列，可以很快再次投递 |
| `requeue=False` | 不回原队列；有死信配置则走死信流程，没有则丢弃 |

这次的消息路径是：

```text
邮件队列 → 消费者处理失败 → nack(requeue=False)
                            ↓
                   order.failure 交换机
                            ↓
                   order.email.failed.q
```

在当前服务正常、绑定正确的情况下，邮件队列数量回落，失败队列 Ready 增加 1。

**本教程用经典队列演示死信流程。默认死信转发在目标不可用等情况下可能丢失，不能把“调用 nack”视为失败队列已可靠收到的确认。** 生产场景需要另行设计可靠转移和监控。[官方死信说明](https://www.rabbitmq.com/docs/dlx)

### 8.3 查看失败消息

1. 点击 `ai_lab.learn.order.email.failed.q`。
2. 展开 **Get messages**。
3. 数量填 1。
4. Ack mode 选择带有 **requeue true** 的选项，例如 **Nack message requeue true**。
5. 点击 **Get Message(s)**。
6. 查看 Payload 中的订单号和 `simulate_failure`。

管理页面的 Get messages 会真的取消息。选 requeue true 是为了查看后放回；不要选带有删除语义的模式来“随便看看”。即使放回，也可能影响投递标记或顺序。

消息 Headers 中通常还能看到 **x-death**：RabbitMQ 记录的死信历史，包括来源队列、原因和次数。这次原因通常是 `rejected`。

### 8.4 失败后怎么继续

再正常发一条，验证消费者没有因为前一条失败而停止：

```powershell
uv run python -m rabbitMQ学习.publisher --order-id normal-after-fail
```

失败队列里的那条不会自动恢复。本教程没有自动重试，也没有自动回放程序。这里的失败开关一直为 true，原样重新发送仍然会失败。

真实业务通常先排查原因、修复，再决定哪些消息可以重新处理。不要直接清空失败队列代替修复。

## 9. 多个消费者和多个队列

### 9.1 两个消费者监听同一个队列：分工

分别在终端 B、C 启动：

```powershell
uv run python -m rabbitMQ学习.consumer --delay 2
```

终端 A 连续发送 6 条：

```powershell
1..6 | ForEach-Object {
    uv run python -m rabbitMQ学习.publisher --order-id "batch-$_"
}
```

观察 B、C 都会拿到一部分消息，但不要期待严格一人三条。

```text
                 ┌→ 邮件消费者 B
一个邮件队列 ────┤
                 └→ 邮件消费者 C
```

正常分发时，一条消息交给其中一个消费者处理；异常重新投递仍可能造成重复。多个消费者适合让**同一种工作**有更多人分担。

### 9.2 邮件和积分都要收到：各建一个队列

先停掉 B、C 的邮件消费者，创建积分队列：

```powershell
uv run python -m rabbitMQ学习.topology --with-points
```

现在多了 `ai_lab.learn.order.points.q`，也绑定 `order.paid`：

```text
                           ┌→ order.email.q  → 邮件消费者
order.events 交换机 ───────┤
                           └→ order.points.q → 积分消费者
```

两个队列都准备好后，发送一条新消息：

```powershell
uv run python -m rabbitMQ学习.publisher --order-id both-001
```

如果没有其他消费者，页面上两个队列的 Ready 都应增加 1。

终端 B：

```powershell
uv run python -m rabbitMQ学习.consumer --queue email
```

终端 C：

```powershell
uv run python -m rabbitMQ学习.consumer --queue points
```

B 打印模拟发送通知，C 打印模拟增加积分；两边都能看到 `both-001`。

注意：

- 新绑定只影响此后发布的消息，不会补发历史消息。
- 积分队列创建后会一直保留。关闭积分消费者后继续发布，积分消息会积压，这是正常现象。
- 这一节只给邮件队列配置了死信规则，积分队列用于观察订阅。积分消费者若收到无效数据并拒绝，消息会丢弃；正式接入应为它补上独立失败处理。

## 10. 读懂连接配置和常用参数

连接配置见 [config.py](config.py)，下面按实际调用顺序解释 API。

`load_dotenv(ROOT / ".env")` 从明确的项目根目录读取配置。默认不会覆盖已经存在的系统环境变量，所以若你以前在终端设置过另一条 `RABBITMQ_URL`，它会优先于文件。

本例连接参数：

| 参数 | 当前值 | 作用 |
| --- | --- | --- |
| heartbeat | 60 秒 | 双方定期通信，帮助发现失效连接；实际值由双方协商 |
| socket_timeout | 5 秒 | 限制建立底层网络连接的等待时间 |
| stack_timeout | 10 秒 | 限制整个连接建立过程的等待时间 |
| connection_attempts | 1 | 建立连接失败时，本次不反复重试 |
| blocked_connection_timeout | 15 秒 | 服务器因资源告警阻塞连接时，避免相关等待无限持续 |

**心跳 Heartbeat** 是连接双方定期发送的小信号，用来判断对方还在不在。

这里的超时设置不等于“所有业务操作都最多等待 15 秒”。例如消费者本来就需要一直等新消息；`--once` 也是“处理一条后退出”，队列为空时仍会等待。

**阻塞式 Blocking** 表示调用通常要等待结果再继续。Pika 的 BlockingConnection 适合这里的独立练习脚本。连接不应直接在多个线程间随意共享；长业务处理也不能一直堵住心跳。本例用 `connection.sleep()` 模拟耗时，让 Pika 仍有机会处理连接事件。[Pika BlockingConnection 文档](https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html)

### 10.1 创建连接和通道

```python
parameters = pika.URLParameters(url)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
```

- `URLParameters(url)`：解析连接字符串，返回参数对象；这一步还没有发起网络连接。
- `BlockingConnection(parameters)`：实际连接服务器并登录，成功返回连接对象，失败抛出异常。
- `connection.channel()`：创建通道，返回 `BlockingChannel`。声明队列、发布、订阅和确认都通过它完成。
- `connection.close()`：关闭连接以及其中的通道。示例用 `with open_connection()`，离开代码块时自动关闭。

### 10.2 创建交换机、队列和绑定

```python
channel.exchange_declare(exchange="my.events", exchange_type="topic", durable=True)
channel.queue_declare(queue="my.email.q", durable=True, arguments={"x-queue-type": "classic"})
channel.queue_bind(queue="my.email.q", exchange="my.events", routing_key="order.paid")
```

这里的 `my.*` 只是说明参数的例子；项目实际名称统一定义在 topology.py。

| API / 参数 | 含义和使用注意 |
| --- | --- |
| `exchange_declare` | 创建交换机或检查已有定义；`exchange_type` 决定匹配规则 |
| `queue_declare` | 创建队列或检查已有定义；不是清空队列，也不是覆盖修改 |
| `durable=True` | 保存资源定义；消息要持久化还需单独设置 `delivery_mode=2` |
| `arguments` | 可选配置字典，例如队列类型、死信交换机、消息 TTL |
| `queue_bind` | 建立“交换机到队列”的转发规则，不传输已有历史消息 |
| `queue_bind` 的 `routing_key` | 绑定规则；对于 topic 可包含 `*`、`#` |

声明和绑定成功会收到服务器响应；同名资源参数不一致通常会导致服务器关闭当前通道并报错。重复运行相同声明不会产生两份资源。

### 10.3 发送消息

```python
channel.confirm_delivery()
channel.basic_publish(
    exchange=ORDER_EVENTS_EXCHANGE,
    routing_key="order.paid",
    body=json.dumps(event, ensure_ascii=False).encode("utf-8"),
    properties=pika.BasicProperties(delivery_mode=2, message_id=event_id),
    mandatory=True,
)
```

| API / 参数 | 含义和使用注意 |
| --- | --- |
| `confirm_delivery()` | 在当前通道开启发布确认；换一个通道需要重新开启 |
| `basic_publish()` | 向指定交换机发布一条消息；本例确认模式下等待发布结果 |
| `exchange` | 目标交换机名称；空字符串表示默认交换机 |
| `routing_key` | 这次消息携带的路由键，用来与绑定规则匹配 |
| `body` | 正文；示例显式编码成 UTF-8 字节 |
| `BasicProperties` | 消息属性容器，例如格式、编码、持久化标记、消息 ID |
| `message_id` | 应用设置的标识；RabbitMQ 不会因为 ID 相同就自动去重 |
| `mandatory=True` | 没有任何匹配队列时要求退回；它不检查是否有消费者在线 |

Pika 的阻塞式发布不要用 `if basic_publish(...):` 判断成功。本例通过“确认模式下没有抛出异常”判断发布完成：无匹配队列会出现 `UnroutableError`，服务器否定发布会出现 `NackError`，网络故障还可能抛出连接异常。

断线时结果可能不确定：服务器可能已经接收，只是确认没有传回来。后续若重发，需要考虑重复消息。

### 10.4 订阅、接收与确认

```python
channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    queue=ORDER_EMAIL_QUEUE,
    on_message_callback=on_message,
    auto_ack=False,
)
channel.start_consuming()
```

| API / 参数 | 含义和使用注意 |
| --- | --- |
| `basic_qos(prefetch_count=1)` | 限制未确认消息数量；本例每个消费者最多持有一条 |
| `basic_consume()` | 注册队列订阅及回调，返回消费者标识；不负责创建队列 |
| `on_message_callback` | 填函数本身 `on_message`，不要写 `on_message()` 提前调用它 |
| `auto_ack=False` | 关闭自动确认，业务完成后由代码确认 |
| `start_consuming()` | 进入阻塞式事件循环，处理收到的投递和回调 |
| `basic_ack(delivery_tag=...)` | 确认该通道的一次投递；默认只确认这一条 |
| `basic_nack(..., requeue=False)` | 拒绝这次投递；按死信配置转交，没有配置则丢弃 |
| `stop_consuming()` | 停止消费循环；本例用于 `--once`，之后离开 with 关闭连接 |

回调中的 `method.delivery_tag` 只用于本通道确认，`properties.message_id` 是应用设置的消息标识，两者不能互换。`body` 由应用解析，RabbitMQ 不知道里面的订单是否有效。

### 10.5 文件对应关系

本目录文件各做一件事：

| 文件 | 用途 |
| --- | --- |
| [config.py](config.py) | 读取连接配置 |
| [check_connection.py](check_connection.py) | 检查 AMQP 登录 |
| [hello_send.py](hello_send.py) | 发送简单文本 |
| [hello_receive.py](hello_receive.py) | 接收简单文本 |
| [topology.py](topology.py) | 创建订单示例的资源和绑定 |
| [publisher.py](publisher.py) | 发送订单支付消息 |
| [consumer.py](consumer.py) | 模拟处理、成功确认、失败拒绝 |

## 11. 后面再学的几个概念

前面的收发、确认、失败和多队列练习做熟之后，再看这一节。目前的代码没有实现这些进阶能力。

### 11.1 幂等：重复执行，不重复产生结果

同一条支付消息收到两次，也只给用户增加一次积分，这叫**幂等**。

RabbitMQ 的确认机制允许重新投递，因此“收到过一次”不等于“以后绝不会再收到”。实际系统可以在数据库保存已处理的事件 ID，配合唯一约束，并把去重记录和业务修改放进同一事务。

**事务 Transaction** 是数据库把一组操作作为整体提交或回滚的机制。把“已处理”记录写成功、业务更新却失败，会误以为任务已经完成；把两者放在同一事务可以避免这种分离。

对于发邮件这类外部调用，还需要考虑外部系统的幂等能力或通知任务表，仅有数据库事务不够。

### 11.2 Outbox：避免写库成功，消息却没发出去

下面两步之间，程序可能退出：

```text
订单写入数据库成功 → 发布 RabbitMQ 消息
```

**Outbox** 是在自己的数据库中加一张待发送消息表。保存订单时，在同一个数据库事务里也保存待发事件；后台程序再扫描待发事件并发布，收到确认后标记发送完成。

它能缩小数据库和消息系统之间的遗漏风险，但发布成功后标记失败仍可能导致重复发送，所以仍要考虑幂等。

### 11.3 TTL 和延迟重试

**TTL，Time To Live** 是存活时间，表示消息在队列里允许保留多久。例如队列参数 `x-message-ttl=5000` 表示 5000 毫秒，也就是 5 秒。

结合死信交换机，可以让失败消息先进入等待队列，过期后再转回工作队列。实际重新投递时间还受队列和服务状态影响，不是精准定时器。

**重试** 是失败后再做一次。临时网络超时可能适合重试，格式完全错误的消息通常不适合一直重试。不要无限使用 `requeue=True`，否则同一条坏消息可能在短时间内反复投递。

### 11.4 经典队列和仲裁队列

- **Classic queue，经典队列**：常规队列类型，本教程明确使用这一类型。
- **Quorum queue，仲裁队列**：使用多数副本协作保存数据的队列类型，适合需要复制保护的场景。

单台服务器上创建仲裁队列，并不会自动多出几台机器保护数据。集群、副本、死信可靠性和故障恢复需要结合实际部署设计；做完基础练习后再专门学习。

### 11.5 接到 FastAPI 时放在哪里

订单接口负责接收请求和保存订单，消费者作为单独进程持续运行。不要在 HTTP 请求里调用 `start_consuming()`，它会一直等消息。

`pika.BlockingConnection` 的网络等待也会阻塞当前线程，不应直接放进 `async def` 的事件循环里。后续可选择独立发布进程、Outbox 或异步客户端；本教程先把独立脚本跑明白。

## 12. 常见问题

| 现象 | 常见原因 | 怎么处理 |
| --- | --- | --- |
| 管理页面能打开，Python 连接超时 | 5672 没从本机打通 | 回到第 2.5 节检查安全组、防火墙、映射 |
| `ProbableAuthenticationError` | 用户名或密码不正确 | 检查本地 `.env`；不要把连接串贴到公开日志 |
| `ProbableAccessDeniedError` | 不能访问指定 vhost | 检查 vhost 名及用户权限 |
| `ACCESS_REFUSED` | 当前用户缺少某项资源权限 | 查看异常指向的交换机、队列和操作 |
| `NOT_FOUND - no exchange` 或 `no queue` | 没有声明资源，或连错空间 | 先运行 topology，确认使用同一个 vhost |
| `PRECONDITION_FAILED` / `inequivalent arg` | 同名队列或交换机的旧参数不同 | 用新前缀重新练习，不要删除不清楚用途的队列 |
| `UnroutableError` | 交换机存在，但没有匹配的队列绑定 | 查看 Bindings 和 routing key |
| 发送后 Ready 一直为 0 | 消费者太快取走，或看错队列 | 暂停对应消费者再发；同时检查 Unacked |
| Ready 持续增加 | 消费者没启动，或者处理能力不足 | 看 Consumers 数量和终端输出 |
| Unacked 长时间不降 | 消费者拿到消息但未确认 | 看是否正在延迟，业务是否卡住、是否漏了 ack |
| 消费者没有任何输出 | 队列没有新消息，或收的不是目标队列 | 在另一个终端发消息，核对队列名 |
| `No module named rabbitMQ学习` | 运行目录不对或运行方式不对 | 在项目根目录使用 `uv run python -m ...` |
| `No module named pika` | 使用了其他 Python 环境 | 运行 `uv sync --locked`，然后用 `uv run` |
| `uv` 不是可识别命令 | uv 未安装或终端尚未刷新 PATH | 先配置 uv；当前电脑已经有，不需要重复安装 |
| 修改 `.env` 后仍连接旧地址 | 系统环境变量优先，或旧进程未重启 | 清除当前终端旧变量并重新启动脚本 |
| 中文乱码 | 终端或 Python 输出编码不一致 | 设置下面的 UTF-8 环境变量后重新运行 |

清除当前 PowerShell 中的旧连接变量，让程序读取 `.env`：

```powershell
Remove-Item Env:RABBITMQ_URL -ErrorAction SilentlyContinue
```

设置当前终端的 Python 输出编码：

```powershell
$env:PYTHONIOENCODING = "utf-8"
```

遇到同名旧资源冲突，可以在 `.env` 添加一个新前缀：

```dotenv
RABBITMQ_PREFIX=ai_lab.learn2
```

重启所有练习进程，重新运行 topology，后续页面查找也改成这个前缀。它会新建一组资源，不会迁移或清理旧消息。

管理页面里 `Purge` 表示清空队列中的待处理消息，`Delete` 表示删除队列本身。它们都不是刷新按钮；不要用来修复不明原因的连接错误。

## 13. 命令速查与练习完成标准

下面所有命令都在项目根目录执行。每个消费者是持续运行的，要另开终端；用 Ctrl+C 停止。

| 想做什么 | 命令 |
| --- | --- |
| 安装锁定依赖 | `uv sync --locked` |
| 检查服务器登录 | `uv run python -m rabbitMQ学习.check_connection` |
| 发简单消息 | `uv run python -m rabbitMQ学习.hello_send "你好"` |
| 收简单消息 | `uv run python -m rabbitMQ学习.hello_receive` |
| 创建订单资源 | `uv run python -m rabbitMQ学习.topology` |
| 发正常订单事件 | `uv run python -m rabbitMQ学习.publisher --order-id demo-001` |
| 收订单事件 | `uv run python -m rabbitMQ学习.consumer` |
| 处理一条后退出 | `uv run python -m rabbitMQ学习.consumer --once` |
| 模拟慢处理 | `uv run python -m rabbitMQ学习.consumer --delay 20` |
| 发故意失败的事件 | `uv run python -m rabbitMQ学习.publisher --order-id fail-001 --fail` |
| 增加积分订阅 | `uv run python -m rabbitMQ学习.topology --with-points` |
| 收积分消息 | `uv run python -m rabbitMQ学习.consumer --queue points` |
| 查看某个脚本的参数 | `uv run python -m rabbitMQ学习.consumer --help` |

按顺序完成：

- [ ] 5672 检查成功，连接脚本打印成功。
- [ ] 不启动消费者时，发送消息能让 Ready 增加。
- [ ] 启动消费者后能看到正文，确认后数量回落。
- [ ] 慢处理时能看到 Unacked。
- [ ] 在确认前停止程序，重启后收到重新投递的消息。
- [ ] 使用 --fail 后，失败队列收到消息。
- [ ] 两个邮件消费者分担同一个队列的消息。
- [ ] 邮件和积分两个队列各收到同一条新事件。

基础流程参考 [RabbitMQ 官方 Python 入门](https://www.rabbitmq.com/tutorials/tutorial-one-python)。官方在线示例会随版本更新；本目录按已检查到的服务器 3.12.1 环境组织，未依赖新版本才有的特性。
