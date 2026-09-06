# RabbitMQ 开发教程：订单消息与异步处理

以订单支付通知为例，介绍 RabbitMQ 的配置、路由、消息确认和异常处理。**本地使用 PyCharm 直接运行，Parameters 留空。**

| 项目 | 当前配置 |
| --- | --- |
| 管理页面 | [62.234.16.180:15672](http://62.234.16.180:15672/) |
| AMQP 地址 | `62.234.16.180:5672` |
| 用户名 | `tianshiyang` |
| 密码 | 已保存在项目根目录 `.env` |
| Vhost / 资源前缀 | `/` / `ai_lab.learn` |
| Python 依赖 | 已安装 `pika 1.4.4`、`python-dotenv 1.2.3` |

运行前先执行连接检查；管理页面可访问不代表 AMQP 端口可用。连接失败时按第 2 节排查。

每章的图都从上往下表示时间顺序，或用分区表示不同角色。图下有对应文字，即使预览器不支持 Mermaid，也可以照着操作。

## 阅读顺序

1. [RabbitMQ 做什么](#1-rabbitmq-做什么)
2. [准备 PyCharm 和连接](#2-准备-pycharm-和连接)
3. [发送文本](#3-发送文本)
4. [接收文本](#4-接收文本)
5. [用快递理解交换机](#5-用快递理解交换机)
6. [发送和处理订单](#6-发送和处理订单)
7. [观察未确认和重新投递](#7-观察未确认和重新投递)
8. [处理失败和死信](#8-处理失败和死信)
9. [多人分工与多队列订阅](#9-多人分工与多队列订阅)
10. [代码语法与 API](#10-代码语法与-api)
11. [可靠性设计与生产接入](#11-可靠性设计与生产接入)
12. [常见问题](#12-常见问题)
13. [运行速查](#13-运行速查)

## 1. RabbitMQ 做什么

RabbitMQ 帮程序接收、保存和转发消息。例如订单保存后，把“发邮件”交给队列，邮件程序稍后处理。发邮件的业务仍由你写，RabbitMQ 只负责消息传递。

```mermaid
sequenceDiagram
    participant O as 订单程序
    participant R as RabbitMQ
    participant C as 邮件程序
    O->>O: 保存订单
    O->>R: 发出订单已支付消息
    R-->>O: 确认接收
    Note over O: 当前请求可以继续返回
    R->>C: 投递待处理消息
    C->>C: 发送邮件
    C->>R: 确认完成
```

| 名称 | 是什么 | 有什么用 |
| --- | --- | --- |
| 消息 Message | 一份程序间传递的数据 | 例如订单号、用户 ID |
| 生产者 Producer | 发送消息的程序 | 发出“订单已支付”事件 |
| 队列 Queue | 保存待处理消息的地方 | 消费者不在线时先暂存 |
| 消费者 Consumer | 接收并处理消息的程序 | 例如发送通知 |
| Broker | 消息服务器，这里就是 RabbitMQ | 管理消息资源和连接 |
| 异步 | 当前流程不等待后续工作全部完成 | 下单页面不用等邮件发完 |
| 解耦 | 减少程序之间的直接依赖 | 订单程序不必知道邮件程序部署在哪里 |
| 削峰 | 先排队，再按处理能力执行 | 缓冲短时间内的大量任务 |

队列容量有限；消息入队也不等于业务完成。图中的“保存订单、发消息”还不是一个事务，第 11 节会介绍这两步之间的故障问题。

## 2. 准备 PyCharm 和连接

### 2.1 直接点运行，不填启动参数

用 PyCharm 打开项目根目录：

```text
C:\Users\Lenovo\Desktop\python项目\ai-lab
```

在 Python Interpreter 设置中选择项目的：

```text
C:\Users\Lenovo\Desktop\python项目\ai-lab\.venv\Scripts\python.exe
```

项目 `.run` 目录提供六个共享运行配置，选择后点绿色三角即可：

| PyCharm 配置 | 对应文件 |
| --- | --- |
| RabbitMQ-连接检查 | `check_connection.py` |
| RabbitMQ-01-发送文本 | `hello_send.py` |
| RabbitMQ-02-接收文本 | `hello_receive.py` |
| RabbitMQ-03-创建订单资源 | `topology.py` |
| RabbitMQ-04-发送订单 | `publisher.py` |
| RabbitMQ-05-消费订单 | `consumer.py` |

也可以打开对应文件，右键 **Run**。若自动创建的配置提示找不到 `rabbitMQ学习`，在 **Run → Edit Configurations** 中检查：

- **Working directory**：项目根目录 `ai-lab`。
- **Add content roots to PYTHONPATH**：勾选，让 Python 能找到项目中的包。
- **Parameters**：留空。
- 需要同时运行两个相同消费者时，开启 **Allow multiple instances**。部分版本在 **Modify options** 中显示此选项。

项目提供的六个配置已设置根目录、UTF-8 输出、空参数和允许多实例。PyCharm 菜单位置可能随版本变化。[PyCharm Python 运行配置说明](https://www.jetbrains.com/help/pycharm/run-debug-configuration-python.html)

```mermaid
sequenceDiagram
    participant U as 你
    participant I as PyCharm
    participant P as Python脚本
    participant R as RabbitMQ
    U->>I: 选连接检查，点击运行
    I->>P: 使用项目解释器启动，无命令行参数
    P->>P: 读取项目根目录的.env
    P->>R: AMQP登录并建立通道
    alt 连接成功
        R-->>P: 登录成功
        P-->>I: Run窗口显示连接成功
    else 连接失败
        P-->>I: 输出错误类型及排查提示
    end
```

### 2.2 配置管理

配置按用途存放：

| 配置类别 | 存放位置 | 示例 |
| --- | --- | --- |
| 连接与环境 | 本地 `.env`，由 config.py 读取 | 服务器地址、账号、密码、资源前缀 |
| 本地运行选项 | 对应脚本顶部 | 是否创建积分队列、发送数量、模拟耗时 |
| 资源名称与绑定 | topology.py | 交换机名、队列名、路由键 |
| IDE 启动方式 | `.run` | 解释器、工作目录、输出编码 |

例如 topology.py 中：

```python
# False：不额外创建积分队列；True：额外创建积分队列。
WITH_POINTS = False
```

需要创建积分订阅时，将其设为 `True`，保存后运行。True 和 False 是布尔值，表示开启和关闭。

**配置在进程启动时读取。** 修改后需要重新启动；替换正在运行的消费者时，先在对应 Run 窗口停止它。

生产部署由环境变量或配置管理系统提供环境差异和凭据，连接密码不写入源码或共享运行配置。脚本顶部的模拟失败、模拟耗时用于本地验证，接入真实业务时由实际业务结果决定成功或失败。

### 2.3 依赖包有什么用

| 名称 | 是什么 | 这里用来做什么 |
| --- | --- | --- |
| `pika` | Python 的 RabbitMQ 客户端库 | 从 Python 连接服务器、发送消息、接收消息 |
| `python-dotenv` | 读取 `.env` 文件的库 | 把连接配置读进程序 |
| `uv` | Python 项目和依赖管理工具 | 安装项目需要的包，并用项目环境运行 Python |
| `.venv` | 当前项目的 Python 虚拟环境目录 | 将本项目的包与其他项目隔开 |
| `pyproject.toml` | 项目配置文件 | 声明依赖哪些包、要求哪个 Python 版本 |
| `uv.lock` | 依赖版本记录文件 | 让不同电脑尽量安装同一组版本 |

这些包已经安装。以后换电脑或重新拉取项目，在 PyCharm 底部 Terminal 切到项目根目录后运行一次（这是安装依赖，不是启动练习）：

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

### 2.4 确认本地配置

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

### 2.5 分清 15672 和 5672

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

### 2.6 如果 5672 仍超时，按这个顺序查

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
```

随后在 PyCharm 运行 check_connection.py。成功时：

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


## 3. 发送文本

### 3.1 改文字，点运行

打开 [hello_send.py](hello_send.py)，顶部保持：

```python
MESSAGE = "你好，RabbitMQ！"
```

点运行，预期输出：

```text
已发送：你好，RabbitMQ！
目标队列：ai_lab.learn.hello.q
```

发送脚本执行完就退出。先不要启动接收脚本，便于观察排队。

```mermaid
sequenceDiagram
    participant U as 你
    participant P as hello_send.py
    participant R as RabbitMQ
    U->>P: 修改MESSAGE并点运行
    P->>R: 建立连接和通道
    P->>R: 声明hello队列
    P->>R: 开启发送确认
    P->>R: 发布文字到默认交换机
    R->>R: 按队列名路由，保存消息
    R-->>P: 确认发布
    P-->>U: 打印已发送并退出
```

### 3.2 在页面看消息

打开管理页面，选择 vhost `/`，进入 **Queues / Queues and Streams**，找到 `ai_lab.learn.hello.q`。

队列原本为空且没有消费者时，统计刷新后应为：

| Ready | Unacked | Total |
| --- | --- | --- |
| 1 | 0 | 1 |

- **Ready**：等待投递。
- **Unacked**：已经投递，等待消费者确认完成。
- **Total**：前两者相加。

把 MESSAGE 改成“第二条消息”，再运行；改成“第三条消息”，再运行。Ready 应累计增加。若消费者已经运行，它可能立即取走消息，Ready 就不一定能看到增长。

### 3.3 看发送代码

```python
channel.basic_publish(
    exchange="",
    routing_key=HELLO_QUEUE,
    body=MESSAGE.encode("utf-8"),
    properties=pika.BasicProperties(delivery_mode=2),
    mandatory=True,
)
```

| 参数 | 解释 |
| --- | --- |
| `exchange=""` | 默认交换机，RabbitMQ 自带的一个特殊交换机 |
| `routing_key=HELLO_QUEUE` | 默认交换机按队列名找到目标队列 |
| `body` | 正文；encode 将文本变成字节 |
| `delivery_mode=2` | 这条消息需要持久化；队列也要设 durable |
| `mandatory=True` | 没有匹配队列时退回；配合确认模式让 Pika 报错 |

前面的 `queue_declare` 是准备队列，`confirm_delivery` 是开启发送确认。详细参数放在第 10 节。

## 4. 接收文本

### 4.1 运行接收脚本

右键运行 [hello_receive.py](hello_receive.py)。它会把前面排队的消息打印出来：

```text
收到：你好，RabbitMQ！
收到：第二条消息
收到：第三条消息
```

接收脚本会持续等待，不会自行结束。保持这个 Run 窗口运行，再到 hello_send.py 点运行，接收窗口应收到新文字。两个不同配置可以同时运行。

```mermaid
sequenceDiagram
    participant P as hello_send.py
    participant R as RabbitMQ队列
    participant C as hello_receive.py
    C->>R: 注册消费者，要求手动确认
    Note over C: start_consuming等待消息
    P->>R: 发布新文字
    R->>C: 投递正文及delivery_tag
    C->>C: on_message打印文字
    C->>R: basic_ack确认本次投递
    R->>R: 移除已确认消息
```

### 4.2 看接收代码

```python
def on_message(ch, method, properties, body):
    print(f"收到：{body.decode('utf-8', errors='replace')}", flush=True)
    ch.basic_ack(delivery_tag=method.delivery_tag)
```

**回调函数**是交给 Pika 的函数；Pika 收到消息时再调用它，不是你每收到一次就手动点运行。

| 参数 | 是什么 |
| --- | --- |
| ch | 收到消息的通道 |
| method | 这次投递的信息，包括 delivery_tag、redelivered |
| properties | 消息属性，例如 message_id |
| body | 正文字节，decode 还原成文字 |

**Ack** 是处理确认：“这条消息已经处理完，可以移除了。” `delivery_tag` 是当前通道内的投递编号，确认必须通过收到消息的原通道。

`basic_consume(..., auto_ack=False)` 注册订阅并要求手动确认；`start_consuming()` 进入等待循环。代码先打印再 ack。实际业务应先完成业务，再确认。

完成后，在接收脚本的 Run 窗口点红色停止按钮。IDE 可能直接终止进程，因此不一定会显示代码里捕获 Ctrl+C 的退出提示，这不影响停止；服务器检测到连接关闭后会处理未确认消息。

## 5. 用快递理解交换机

### 5.1 一件快递经过哪里

假设你寄一件快递到“上海浦东”。你先交给分拣中心，分拣中心看面单，根据规则把快递放到浦东网点的待派送货架，快递员再取走派送。

对应到 RabbitMQ：

| 快递中的东西 | RabbitMQ | 负责什么 |
| --- | --- | --- |
| 寄件人 | 生产者 | 发出消息 |
| 包裹内容 | 消息正文 body | 存业务数据，例如订单号 |
| 分拣台 | 交换机 exchange | 判断消息该去哪些队列 |
| 面单上的分类标签 | 路由键 routing_key | 例如 shanghai.pudong 或 order.paid |
| “上海浦东件放到 A 货架”的分拣规则 | 绑定 binding | 建立交换机到队列的匹配关系 |
| 待派送货架 | 队列 queue | 保存还没处理的消息 |
| 快递员 | 消费者 | 取消息并执行工作 |
| 完成后回报 | ack | 告诉 RabbitMQ 可以移除本次消息 |

**交换机负责分，队列负责存，消费者负责做。** 分拣台不是存货仓库：没有匹配的队列，交换机不会一直替你保管，等以后再建队列。

```mermaid
flowchart LR
    subgraph sender["寄件方：生产者"]
        A["订单消息<br/>面单：order.paid"]
    end
    subgraph broker["分拣中心：RabbitMQ"]
        E["交换机<br/>order.events"]
        B["绑定规则<br/>接收 order.paid"]
        Q["待派送货架<br/>order.email.q"]
        E --> B --> Q
    end
    subgraph receiver["派送方：消费者"]
        C["邮件程序<br/>模拟发送通知"]
    end
    A --> E
    Q --> C
```

图里省略了资源前缀 `ai_lab.learn.`，代码里使用完整名称。

### 5.2 面单和分拣规则不是同一个东西

生产者发消息时写：

```python
channel.basic_publish(
    exchange=ORDER_EVENTS_EXCHANGE,
    routing_key="order.paid",  # 这件包裹的面单标签。
    body=b"example",
)
```

声明资源时写：

```python
channel.queue_bind(
    exchange=ORDER_EVENTS_EXCHANGE,
    queue=ORDER_EMAIL_QUEUE,
    routing_key="order.paid",  # 这个货架愿意接收的标签。
)
```

这里两处参数都叫 routing_key，但位置不同：

1. 发布时：描述**这条消息**是什么类别。
2. 绑定时：描述**这个队列**接收什么类别。
3. 交换机按自己的类型比较二者，匹配就转发。

上面的发布片段只用来认参数，`b"example"` 不是合法订单 JSON，不要拿它代替订单脚本发送。实际可运行示例使用 publisher.py。

队列叫 `order.email.q` 只是名字。RabbitMQ 不会看名字猜“它应该接订单消息”，必须设置绑定。路由键也不会自动从 JSON 的 `event_name` 中读取，是生产者另外传入的参数。

### 5.3 四种分拣方式

| 交换机类型 | 快递类比 | 匹配例子 |
| --- | --- | --- |
| direct | 只接收指定的完整标签 | 绑定 shanghai.pudong，只接收同样的标签 |
| topic | 按地区层级匹配 | shanghai.* 接收上海下一级地区 |
| fanout | 给所有登记网点各发一份通知副本 | 忽略面单，交给全部绑定队列 |
| headers | 看多项面单属性 | 按消息头中的地区、业务类型等属性匹配 |

本教程订单交换机使用 topic，但绑定的是完整的 `order.paid`，目前不需要通配符也能工作。失败交换机使用 direct，匹配 `email.failed`。

topic 用点号分段：

| 绑定规则 | 能接收 | 不能接收 |
| --- | --- | --- |
| `shanghai.pudong` | shanghai.pudong | shanghai.minhang |
| `shanghai.*` | shanghai.pudong、shanghai.minhang | shanghai、shanghai.pudong.express |
| `shanghai.#` | shanghai、shanghai.pudong、shanghai.pudong.express | beijing.chaoyang |

`*` 表示恰好一段，`#` 表示零段或多段。“一段”是点号之间的内容。[官方 topic 教程](https://www.rabbitmq.com/tutorials/tutorial-five-python)

### 5.4 快递比喻有一个地方需要改一下

真实包裹通常只有一份，RabbitMQ **可以把同一条消息路由到多个匹配队列，每个队列各有一份**。更像“把一份电子面单复印给多个部门”，而不是几个人争抢同一个包裹。

邮件队列和积分队列都绑定 order.paid 时，两个业务都能处理这条事件。同一个队列里有两个消费者时，则是两个快递员分担这个货架的工作，第 9 节会实际操作。[官方发布订阅教程](https://www.rabbitmq.com/tutorials/tutorial-three-python)

### 5.5 默认交换机是什么

hello_send.py 里的 `exchange=""` 是默认交换机，可以理解成“按货架编号直接分拣”的预设通道。创建队列时，RabbitMQ 自动给它建立一个以队列名为键的默认绑定，所以 routing_key 填队列名就能送达。

看起来像直接发给队列，实际仍经过默认交换机。

**拓扑 topology** 就是“有哪些分拣台、货架、规则”的总称。topology.py 负责把这些资源准备好，不负责发邮件。

## 6. 发送和处理订单

这里只打印模拟结果，不会真实扣款、发邮件或增加积分。

### 6.1 先准备资源

打开 topology.py，保持：

```python
WITH_POINTS = False
```

右键运行，管理页面应出现：

| 资源 | 完整名称 |
| --- | --- |
| 订单交换机 | `ai_lab.learn.order.events` |
| 失败交换机 | `ai_lab.learn.order.failure` |
| 邮件队列 | `ai_lab.learn.order.email.q` |
| 失败队列 | `ai_lab.learn.order.email.failed.q` |

点击订单交换机，Bindings 中应看到 `order.paid` 绑定到邮件队列。重复运行不会清空消息；同名资源参数不同会报错，声明不是“覆盖修改”。

topology.py 的执行顺序已拆成四个函数：创建交换机 → 创建失败队列及绑定 → 创建邮件队列及绑定 → 按开关创建积分队列。

### 6.2 发一条订单

打开 publisher.py，顶部设为：

```python
ORDER_ID = "202609060001"
SIMULATE_FAILURE = False
MESSAGE_COUNT = 1
```

右键运行，输出应包含订单号和 event_id。暂时没有邮件消费者时，邮件队列 Ready 增加 1。

消息字段：

| 字段 | 作用 |
| --- | --- |
| event_id | 本次事件唯一标识，每次发布重新生成 |
| event_name | order.paid，表示订单已支付 |
| occurred_at | 事件时间，使用 UTC |
| order_id | 要处理的订单号 |
| user_id | 用户标识 |
| total_amount | 字符串金额 99.00 |
| simulate_failure | 学习开关，要求邮件处理模拟失败 |

正文使用 JSON：一种不同语言都容易读取的数据格式。RabbitMQ 不检查订单字段，是 order_service.py 在消费时检查。

### 6.3 收这条订单

打开 consumer.py，保持：

```python
QUEUE_KIND = "email"
DELAY_SECONDS = 0
STOP_AFTER_ONE = False
```

右键运行，预期显示：

```text
收到消息：redelivered=False，body=...
模拟发送支付通知成功：order_id=202609060001
已发送 ack。
```

保持消费者运行，在 publisher.py 把 ORDER_ID 改成 `"202609060002"` 再运行，消费者会继续接收。

### 6.4 看完整顺序

```mermaid
sequenceDiagram
    autonumber
    participant P as publisher.py
    participant R as RabbitMQ
    participant C as consumer.py
    participant S as order_service.py
    P->>P: build_order_event组装JSON数据
    P->>R: 开启confirm并发布order.paid
    R->>R: 交换机按绑定将消息放到邮件队列
    R-->>P: Publisher Confirm
    Note over P: 发布成功，脚本退出
    C->>R: 注册邮件消费者
    R->>C: 投递消息，状态Unacked
    C->>S: parse_order_event检查正文
    S-->>C: 返回订单字典
    C->>S: handle_order_event模拟通知
    S-->>C: 正常返回
    C->>R: basic_ack
    R->>R: 移除已确认消息
```

图按“先发后收”的练习顺序绘制。消费者已在线时，投递与生产者收到 confirm 的先后不固定。

**Confirm** 只表示 RabbitMQ 对发布的确认；**Ack** 表示消费者对处理的确认。前者不证明邮件已经发出，二者彼此独立。[官方确认机制](https://www.rabbitmq.com/docs/confirms)

代码按先业务、后 ack 排列。先 ack 再干活，业务失败时原消息就可能已经移除。

## 7. 观察未确认和重新投递

### 7.1 把处理放慢

先停掉旧邮件消费者。consumer.py 改成：

```python
QUEUE_KIND = "email"
DELAY_SECONDS = 20
STOP_AFTER_ONE = False
```

运行它，再运行 publisher.py 发一条普通订单。消费者显示“模拟处理 20 秒”时，在邮件队列页面观察：

| 时刻 | 状态 |
| --- | --- |
| 未投递 | Ready |
| 已投递、这 20 秒内 | Unacked |
| 处理完成并 ack | Total 回落 |

**Prefetch** 限制消费者手里未确认的消息数量。本例为 1，所以这一条没确认前不会继续塞下一条。它不是线程数，也不是队列容量。

### 7.2 处理到一半停止

再发一条消息，消费者开始等待 20 秒时，点这个消费者 Run 窗口的红色停止按钮。之后把 DELAY_SECONDS 改回 0，再运行消费者。

```mermaid
sequenceDiagram
    participant R as RabbitMQ
    participant C as 旧消费者
    participant U as 你
    participant N as 新消费者
    R->>C: 投递消息
    Note over R,C: Unacked为1
    C->>C: 模拟处理20秒
    U->>C: 在Run窗口停止进程
    R->>R: 检测连接关闭，未确认消息重新入队
    Note over R: 没有其他消费者时Ready回升
    U->>N: 重新点运行
    N->>R: 注册订阅
    R->>N: 重新投递，redelivered通常为True
    N->>R: 处理成功后ack
```

IDE 强制停止时，程序未必有机会打印退出提示。消息也未必瞬间重新出现：服务器要先发现连接关闭，网络故障时可能需要等心跳检测。

这解释了为什么可能重复处理：业务已经做完、ack 却没传到服务器，也会重新投递。因此生产业务还需要幂等，第 11 节再讲。

## 8. 处理失败和死信

### 8.1 发一次故意失败的订单

让普通邮件消费者运行，即 DELAY_SECONDS 为 0。publisher.py 设置：

```python
ORDER_ID = "fail-001"
SIMULATE_FAILURE = True
MESSAGE_COUNT = 1
```

点运行。发送仍然成功，**失败发生在消费者处理邮件时**。消费者会打印失败，再发送 nack。

```mermaid
sequenceDiagram
    participant P as publisher.py
    participant Q as 邮件队列
    participant C as consumer.py
    participant S as order_service.py
    participant D as 失败交换机和失败队列
    P->>Q: 经订单交换机投递模拟失败的消息
    Q->>C: 交给消费者
    C->>S: 调用订单处理
    S-->>C: 抛出ValueError
    C->>Q: basic_nack，requeue=False
    Q->>D: 根据死信规则重新路由
    Note over D: 正常转发后Ready增加1，等待检查
```

### 8.2 这些名字是什么意思

| 名称 | 解释 |
| --- | --- |
| Nack | 否定确认：“这次没有处理成功” |
| requeue=True | 放回原队列，可能很快再次投递 |
| requeue=False | 不放回原队列；有死信配置则转交，没有则丢弃 |
| 死信 Dead Letter | 因拒绝、过期等原因从原队列转出的消息 |
| DLX，死信交换机 | 接收死信的普通交换机 |
| DLQ，死信队列 | 用来接收死信的普通队列 |
| x-death | RabbitMQ 附加的死信历史，包括来源、原因和次数 |

用快递类比，就是“这一单派送失败，按规则送到异常件货架”。失败队列的名字并不赋予它特殊能力，生效的是邮件队列里的配置：

```python
"x-dead-letter-exchange": ORDER_FAILURE_EXCHANGE,
"x-dead-letter-routing-key": ORDER_FAILURE_ROUTING_KEY,
```

第一项指定送到哪个分拣台，第二项指定转发时使用的面单标签。失败队列另外绑定 `email.failed` 才能收到。

### 8.3 在页面查失败内容

1. 打开 `ai_lab.learn.order.email.failed.q`。
2. 展开 **Get messages**，数量设为 1。
3. Ack mode 选带 **requeue true** 的选项，查看后放回。
4. 点击 Get Message(s)，查看正文的订单号和 simulate_failure。
5. Headers 中通常能看到 x-death，原因通常是 rejected。

Get messages 会实际取出消息；即使放回，也可能改变投递标记或顺序。不要把删除模式当只读预览。

本教程使用经典队列。默认死信转发在目标不可用等情况下可能丢失；nack 不是失败队列的可靠收件确认。[官方死信说明](https://www.rabbitmq.com/docs/dlx)

### 8.4 恢复正常练习

把 publisher.py 中的 SIMULATE_FAILURE **改回 False**，更换 ORDER_ID，再运行。新消息应正常处理，旧失败消息仍留在失败队列。

这份代码没有自动重试或回放。顶部开关改回 False 也不会改变已发消息的正文；失败消息原样重发，仍然带着失败开关。

## 9. 多人分工与多队列订阅

### 9.1 两个人处理同一个货架：分工

consumer.py 设置：

```python
QUEUE_KIND = "email"
DELAY_SECONDS = 2
STOP_AFTER_ONE = False
```

运行两次，保持两个实例都在。使用已提供的配置时允许多实例；若 PyCharm 提示停止旧实例，去 Edit Configurations 勾选 Allow multiple instances。

publisher.py 设置：

```python
ORDER_ID = "batch"
SIMULATE_FAILURE = False
MESSAGE_COUNT = 6
```

运行一次会生成 batch-1 到 batch-6。两个消费者都会拿到一部分，不保证严格各三条。

```mermaid
sequenceDiagram
    participant Q as 同一个邮件队列
    participant A as 邮件消费者A
    participant B as 邮件消费者B
    Q->>A: 投递消息1
    Q->>B: 投递消息2
    A->>Q: 确认消息1
    Q->>A: 投递下一条可用消息
    B->>Q: 确认消息2
    Note over Q,B: 这是分工，不是每人都收到每条消息
```

### 9.2 两个部门都要一份：各建队列

停止旧消费者。topology.py 改成 `WITH_POINTS = True`，运行一次，页面会多出 `ai_lab.learn.order.points.q`。

publisher.py 改成：

```python
ORDER_ID = "both-001"
SIMULATE_FAILURE = False
MESSAGE_COUNT = 1
```

运行一次。没有消费者时，邮件和积分队列 Ready 应各增加 1。

接下来：

1. consumer.py 设 `QUEUE_KIND = "email"`、`DELAY_SECONDS = 0`，运行。
2. 保持它运行，将源码中的 QUEUE_KIND 改成 `"points"`，保存。
3. 再启动一个实例。旧实例已经读入 email，不会因保存文件而自动变成 points。
4. 邮件窗口打印模拟通知，积分窗口打印模拟增加积分。

```mermaid
sequenceDiagram
    participant P as 生产者
    participant E as 订单交换机
    participant Q1 as 邮件队列
    participant Q2 as 积分队列
    participant C1 as 邮件消费者
    participant C2 as 积分消费者
    P->>E: order.paid
    E->>Q1: 匹配绑定，保存一份
    E->>Q2: 匹配绑定，再保存一份
    Q1->>C1: 投递邮件工作
    Q2->>C2: 投递积分工作
    C1->>Q1: 独立ack
    C2->>Q2: 独立ack
```

这是两个货架、两个业务各有一份，邮件处理失败不等于积分也失败。积分练习队列未配置死信，拒绝无效数据会丢弃，正式接入应给它补上独立失败处理。

新绑定只影响之后的消息，不会补发历史消息。把 WITH_POINTS 改回 False 也不会删除已建积分队列；停止积分消费者后继续发布，积分队列会积压。

做完后把 MESSAGE_COUNT 改回 1，QUEUE_KIND 改回 email，DELAY_SECONDS 改回 0，SIMULATE_FAILURE 改回 False，方便继续基础练习。

## 10. 代码语法与 API

### 10.0 模块职责与调用关系

```mermaid
flowchart LR
    subgraph entry["运行入口：main"]
        T["topology.py<br/>准备资源"]
        P["publisher.py<br/>组装并发布"]
        C["consumer.py<br/>接收和确认"]
    end
    subgraph support["共用模块"]
        F["config.py<br/>读取连接配置"]
        S["order_service.py<br/>检查数据和模拟业务"]
    end
    subgraph remote["服务器"]
        R["RabbitMQ"]
    end
    T --> F
    P --> F
    C --> F
    F --> R
    C --> S
```

模块按职责划分，调用关系保持明确：

| 模块 | 负责 | 返回结果或错误 |
| --- | --- | --- |
| config.py | 读取配置、建立连接 | 连接对象，或配置与连接异常 |
| topology.py | 声明交换机、队列和绑定 | 声明成功，或资源与权限异常 |
| publisher.py | 组装事件、发布并等待确认 | event_id，或发布异常 |
| consumer.py | 订阅、调用业务、决定 ack 或 nack | 消费循环，或未处理的连接异常 |
| order_service.py | 校验订单事件、执行模拟业务 | 校验后的数据或业务异常 |

业务函数不持有 RabbitMQ 通道，也不决定消息确认。consumer.py 在业务正常返回后 ack，在匹配的业务异常发生后执行失败分支。这样可以分别检查业务逻辑和消息传输逻辑。

生产代码沿用这一职责划分，再根据业务规模增加数据库访问和外部服务调用。拆分依据是职责，函数只做一件明确的事；无需给每个函数再包一层类。

| 语法 | 是什么、这里有什么用 |
| --- | --- |
| `WITH_POINTS = True` | 普通变量赋值；大写是表示配置或常量的命名习惯，不是 Python 强制只读 |
| `def main() -> None:` | 定义入口函数；None 表示不返回业务结果 |
| `if __name__ == "__main__":` | 直接运行此文件才执行入口，被其他文件 import 时不启动消费 |
| `with open_connection() as connection:` | 打开连接，用完自动关闭，包括异常退出代码块时 |
| `from ... import ...` | 使用另一个模块中的函数或变量，不是启动另一个进程 |
| `def f(*, fail=False)` | 星号后必须写参数名调用，例如 f(fail=True)；不是命令行选项 |
| `-> dict`、`: str` | 类型提示，帮助阅读及 IDE 检查，不会自动验证所有运行时数据 |
| `cast(str, url)` | 告诉类型检查器把值看作字符串，不会将 None 变成有效地址，所以代码仍需检查缺失值 |
| `event.get("order_id")` | 从字典取字段，不存在返回 None |
| `raise ValueError(...)` | 抛出错误，由上层决定如何处理 |
| `try / except / else` | 尝试处理；匹配异常走 except；没有异常走 else，本例在那里 ack |
| `f"订单：{order_id}"` | 把变量值填到字符串中 |
| `flush=True` | 及时把输出显示到 Run 窗口，方便观察 |
| `range(MESSAGE_COUNT)` | 执行指定次数，从 0 开始计数 |
| 内部的 `on_message` 函数 | 作为回调保留本次连接和设置，收到消息后交给业务模块处理 |

consumer.py 的回调只做四件事：解析正文 → 按设置模拟等待 → 调用业务 → 成功 ack 或失败 nack。订单字段检查和模拟业务在 order_service.py，资源声明则在 topology.py 的小函数中。



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

这里的超时设置不等于“所有业务操作都最多等待 15 秒”。例如消费者本来就需要一直等新消息；`STOP_AFTER_ONE = True` 也是“处理一条后退出”，队列为空时仍会等待。

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
| `stop_consuming()` | 停止消费循环；本例用于 `STOP_AFTER_ONE = True`，之后离开 with 关闭连接 |

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
| [consumer.py](consumer.py) | 接收消息、协调业务、成功确认或失败拒绝 |
| [order_service.py](order_service.py) | 检查订单字段并模拟业务，不操作 RabbitMQ |


## 11. 可靠性设计与生产接入

```mermaid
sequenceDiagram
    participant A as 订单接口
    participant D as 数据库
    participant W as Outbox后台程序
    participant R as RabbitMQ
    participant C as 消费者
    A->>D: 同一事务保存订单和待发事件
    D-->>A: 提交成功
    W->>D: 读取待发事件
    W->>R: 发布消息
    R-->>W: confirm
    W->>D: 标记已发送
    R->>C: 投递消息
    C->>D: 同一事务内去重并写业务结果
    D-->>C: 提交成功
    C->>R: ack
```

图展示订单落库后可靠发布、业务完成后确认的处理顺序。当前示例实现了发布确认、手动消费确认和邮件死信路由；数据库事务、Outbox 和幂等处理属于接入真实业务时需要实现的部分。

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

单台服务器上创建仲裁队列，并不会自动获得多机副本保护。队列类型应结合数据重要性、部署节点数和故障恢复要求选择。

### 11.5 接到 FastAPI 时放在哪里

订单接口负责接收请求和保存订单，消费者作为单独进程持续运行。不要在 HTTP 请求里调用 `start_consuming()`，它会一直等消息。

`pika.BlockingConnection` 的网络等待会阻塞当前线程，不应直接放进 `async def` 的事件循环里。使用 Outbox 时，由独立后台程序发布消息；消费者也作为独立进程运行。

### 11.6 异常分类和进程恢复

| 情况 | 处理方式 |
| --- | --- |
| JSON 格式或必填字段错误 | 记录原因，转入失败处理，不对同一份错误数据无限重试 |
| 外部服务短暂超时 | 设置调用超时；根据业务允许的次数和间隔重试 |
| RabbitMQ 连接断开 | 重新建立连接、通道和订阅；处理发布结果不确定及重复投递 |
| 业务成功但 ack 失败 | 允许重新投递，通过业务幂等避免重复产生结果 |
| 停止消费者 | 停止接收新任务，给正在执行的任务留出完成时间；未确认消息由服务器重新投递 |

当前连接参数中的超时只限制等待，不等于自动重连。当前脚本遇到未捕获的连接异常会退出，生产服务需要由应用恢复逻辑或进程管理器负责恢复。无论采用哪一种，都要重新建立消费所需的连接和订阅。[RabbitMQ 可靠性指南](https://www.rabbitmq.com/docs/reliability)

### 11.7 日志与运行观察

应用日志至少应能查到：事件 ID、订单号、队列名、处理结果、耗时和错误类型。事件 ID 用于串联一条消息，订单号用于串联同一笔业务；不要把连接密码写入日志。

| 观察项 | 用途 |
| --- | --- |
| Ready 数量及变化趋势 | 判断待处理任务是否持续积压 |
| Unacked 与处理耗时 | 判断消费者是否卡住或处理变慢 |
| Consumers 数量 | 判断消费进程是否在线 |
| 失败队列数量 | 发现无法自动完成的任务 |
| 连接断开、发布失败 | 发现网络或服务可用性问题 |
| 服务器内存、磁盘告警 | 发现可能阻塞发布的资源问题 |

当前 print 输出用于在 PyCharm 中观察流程。部署时使用日志系统和监控收集这些信息，并按实际吞吐量与处理时限设置告警阈值。服务器容量、访问控制和运行参数参见 [RabbitMQ 生产部署指南](https://www.rabbitmq.com/docs/production-checklist)。


## 12. 常见问题

```mermaid
flowchart LR
    subgraph local["本机：PyCharm"]
        A["启动失败"] --> B{"能import pika吗"}
        B -->|否| C["检查项目解释器"]
        B -->|是| D["运行连接检查"]
    end
    subgraph server["服务器：网络与权限"]
        D --> E{"5672可达吗"}
        E -->|否| F["安全组、防火墙、端口映射"]
        E -->|是| G["检查账号、vhost、资源权限"]
    end
    subgraph message["消息：页面与业务"]
        G --> H["检查绑定、Ready、Unacked和日志"]
    end
```



| 现象 | 常见原因 | 怎么处理 |
| --- | --- | --- |
| 管理页面能打开，Python 连接超时 | 5672 没从本机打通 | 回到第 2.6 节检查安全组、防火墙、映射 |
| `ProbableAuthenticationError` | 用户名或密码不正确 | 检查本地 `.env`；不要把连接串贴到公开日志 |
| `ProbableAccessDeniedError` | 不能访问指定 vhost | 检查 vhost 名及用户权限 |
| `ACCESS_REFUSED` | 当前用户缺少某项资源权限 | 查看异常指向的交换机、队列和操作 |
| `NOT_FOUND - no exchange` 或 `no queue` | 没有声明资源，或连错空间 | 先运行 topology，确认使用同一个 vhost |
| `PRECONDITION_FAILED` / `inequivalent arg` | 同名队列或交换机的旧参数不同 | 用新前缀重新练习，不要删除不清楚用途的队列 |
| `UnroutableError` | 交换机存在，但没有匹配的队列绑定 | 查看 Bindings 和 routing key |
| 发送后 Ready 一直为 0 | 消费者太快取走，或看错队列 | 暂停对应消费者再发；同时检查 Unacked |
| Ready 持续增加 | 消费者没启动，或者处理能力不足 | 看 Consumers 数量和 Run 窗口输出 |
| Unacked 长时间不降 | 消费者拿到消息但未确认 | 看是否正在延迟，业务是否卡住、是否漏了 ack |
| 消费者没有任何输出 | 队列没有新消息，或收的不是目标队列 | 保持消费者运行，再运行发送脚本，核对队列名 |
| `No module named rabbitMQ学习` | 运行目录不对或运行方式不对 | 使用提供的运行配置，或勾选 Add content roots to PYTHONPATH |
| `No module named pika` | 使用了其他 Python 环境 | 选择项目 .venv 解释器；缺包时在 Terminal 执行 uv sync --locked |
| `uv` 不是可识别命令 | uv 未安装或终端尚未刷新 PATH | 先配置 uv；当前电脑已经有，不需要重复安装 |
| 修改 `.env` 后仍连接旧地址 | 系统环境变量优先，或旧进程未重启 | 检查 Run 配置中的 Environment variables，移除旧连接值后重启 |
| 中文乱码 | 终端或 Python 输出编码不一致 | 使用提供的 UTF-8 运行配置，或设置 PYTHONIOENCODING=utf-8 |

环境变量在 **Run → Edit Configurations → Environment variables** 中设置。提供的配置已设置 `PYTHONIOENCODING=utf-8`。若这里另填了 RABBITMQ_URL，它会优先于 .env；删除旧覆盖值并重启即可。

遇到同名旧资源冲突，可以在 `.env` 添加一个新前缀：

```dotenv
RABBITMQ_PREFIX=ai_lab.learn2
```

重启所有练习进程，重新运行 topology.py，后续页面查找也改成这个前缀。它会新建一组资源，不会迁移或清理旧消息。

管理页面里 `Purge` 表示清空队列中的待处理消息，`Delete` 表示删除队列本身。它们都不是刷新按钮；不要用来修复不明原因的连接错误。


## 13. 运行速查

修改后保存，直接运行对应文件。所有 Parameters 均留空。

| 练习 | 文件 | 顶部设置 |
| --- | --- | --- |
| 检查登录 | check_connection.py | 不用改 |
| 发文字 | hello_send.py | MESSAGE = "你好" |
| 收文字 | hello_receive.py | 不用改 |
| 创建订单资源 | topology.py | WITH_POINTS = False |
| 发普通订单 | publisher.py | ORDER_ID = "demo-001"，SIMULATE_FAILURE = False，MESSAGE_COUNT = 1 |
| 收邮件任务 | consumer.py | QUEUE_KIND = "email"，DELAY_SECONDS = 0，STOP_AFTER_ONE = False |
| 处理一条就退出 | consumer.py | STOP_AFTER_ONE = True；空队列仍会等待 |
| 观察慢处理 | consumer.py | DELAY_SECONDS = 20 |
| 模拟失败 | publisher.py | SIMULATE_FAILURE = True |
| 批量发送 | publisher.py | MESSAGE_COUNT = 6 |
| 创建积分订阅 | topology.py | WITH_POINTS = True |
| 收积分任务 | consumer.py | QUEUE_KIND = "points" |

```mermaid
sequenceDiagram
    participant U as 你
    participant S as 脚本顶部设置
    participant I as PyCharm
    participant M as 管理页面
    U->>S: 修改本次练习选项
    U->>I: 保存并点击运行
    I-->>U: 查看发送或处理日志
    U->>M: 检查队列数量及绑定
    U->>S: 练习结束后恢复默认值
```

按顺序完成：

- [ ] 连接检查成功。
- [ ] 没有消费者时，发送让 Ready 增加。
- [ ] 启动消费者后，收到文字并确认。
- [ ] 能解释分拣台、面单、分拣规则、货架分别对应什么。
- [ ] 能发出并处理一条订单事件。
- [ ] 20 秒处理期间能看到 Unacked。
- [ ] 确认前停止进程，重启后能收到重新投递。
- [ ] 模拟失败后在失败队列看到消息。
- [ ] 两个邮件消费者分担同一个队列。
- [ ] 邮件与积分两个队列各收到同一条新事件。

运行配置在项目根目录的 `.run` 下，是 PyCharm 的启动设置，不是 RabbitMQ 业务代码。无需编辑其中的 XML，也不用安装本地 RabbitMQ。

参考 [RabbitMQ 官方 Python 入门](https://www.rabbitmq.com/tutorials/tutorial-one-python)。本地行为检查使用模拟连接，不代表已验证远程收发；服务器连通性以你当前运行连接检查的结果为准。
