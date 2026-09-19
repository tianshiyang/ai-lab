# Celery 配置速查(版本 5.6.3,配合 celery教程 八讲使用)

一份完整代码 + 逐行讲参数,再补三块:全部常用命令、"框架替你拧好的两头"与还得自己拧的、四个最容易翻车的点。默认值全部对着本机安装的 celery 5.6.3 源码核过。

## 完整代码

企业起步模板,取第 8 讲的多队列形态(单队列项目删掉队列与路由两段、换 `task_default_queue` 即可):

```python
"""企业起步模板:序列化、可靠性、队列路由、全局兜底一次配齐。"""

import os
from pathlib import Path

from celery import Celery
from celery.schedules import crontab
from dotenv import load_dotenv
from kombu import Exchange, Queue

load_dotenv(Path(__file__).parents[2] / ".env")        # ① 连接地址进 .env,不进代码

exchange = Exchange("shop", type="direct")             # ⑦ 一个 direct 交换机,多队列共用

app = Celery(
    "shop",                                            # ② 项目名
    broker=os.environ["RABBITMQ_URL"],                 # ③ 任务消息走哪
    backend=os.environ["REDIS_URL"],                   # ④ 结果存哪
    include=["tasks.orders", "tasks.notify"],          # ⑤ 任务模块清单
)

app.conf.update(
    # —— 序列化:企业底线 ——
    task_serializer="json",                            # ⑥ 参数序列化
    result_serializer="json",                          # ⑥ 结果序列化
    accept_content=["json"],                           # ⑥ 接收白名单
    timezone="Asia/Shanghai",                          # ⑧ 时区
    # —— 可靠性三件套 ——
    task_acks_late=True,                               # ⑨ 干完才确认
    task_reject_on_worker_lost=True,                   # ⑩ 暴毙即 reject
    worker_prefetch_multiplier=1,                      # ⑪ 不囤消息
    # —— 结果经济学 ——
    result_expires=3600,                               # ⑫ 结果保质期
    # —— 队列与路由 ——
    task_queues=(                                      # ⑬ 显式声明
        Queue("shop.default", exchange, routing_key="shop.default"),
        Queue("shop.orders", exchange, routing_key="shop.orders"),
        Queue("shop.notify", exchange, routing_key="shop.notify"),
    ),
    task_default_queue="shop.default",                 # 兜底队列,worker 不吃它
    task_create_missing_queues=False,                  # ⑭ 防呆
    task_routes={                                      # ⑮ 路由表(一个 dict)
        "tasks.orders.*": {"queue": "shop.orders"},
        "tasks.notify.*": {"queue": "shop.notify"},
    },
    # —— 全局兜底 ——
    task_time_limit=60,                                # ⑯ 全局时限
    worker_max_tasks_per_child=1000,                   # ⑰ 治泄漏
    # —— 周期调度(需要 beat 才配) ——
    beat_schedule={                                    # ⑱ 调度表
        "订单对账": {
            "task": "tasks.orders.reconcile",
            "schedule": crontab(minute="*"),
            "options": {"expires": 3600},
        },
    },
)
```

## 逐行讲

### ①~⑤ 建应用 Celery(...)

- `broker`:AMQP URL,老规矩 vhost 是默认 `/` 时写 `%2F`。Celery 对它做的只有"收发任务消息",队列/交换机由 worker 启动时自动声明,不用你去管理台建。
- `backend`:Redis URL,`redis://:密码@主机:6379/0`,末尾 `/0` 是 db 编号。结果就是 `celery-task-meta-<任务ID>` 一串键。不传 backend 就没有结果能力(全部 PENDING);想用 broker 自带的 `rpc://` 也行,但 chord(第 6 讲)不支持,企业选 Redis 有它一份功劳。
- 项目名(②):只出现在横幅和日志里,不参与任何路由。
- `include`(⑤):任务模块名字符串清单,worker 启动时逐个 import——这是它"只在启动时读代码"的源头。Django 项目不写它,改用 `app.autodiscover_tasks()` 自动找各 app 的 tasks.py。

### ⑥ 序列化三连

| 配置 | 默认 | 说明 |
|---|---|---|
| `task_serializer` | json | 任务参数的打包格式。**企业永不打开 pickle**:接收端要反序列化执行,等于开了个远程代码执行口子 |
| `result_serializer` | json | 返回值的打包格式,跟上条保持一致 |
| `accept_content` | ["json"] | worker 收到任务时的格式白名单,不在名单里直接拒收 |

### ⑧ timezone

默认 UTC。`crontab`(⑱)按它解释——配了 `Asia/Shanghai`,`hour=3` 就是北京时间凌晨 3 点,不用换算。

### ⑨⑩⑪ 可靠性三件套(第 3 讲整章)

| 配置 | 默认 | 企业值 | 一句话 |
|---|---|---|---|
| `task_acks_late` | False | True | 任务**干完**才确认;False 是领到就确认,worker 干到一半被杀任务直接蒸发 |
| `task_reject_on_worker_lost` | False | True | worker 进程暴毙时主动 reject,broker 才会重投 |
| `worker_prefetch_multiplier` | 4 | 1 | 囤消息上限 = 该值 × 并发数。1 = 干完一个再领一个(公平分发) |

### ⑫ result_expires

默认 86400(一天)。结果不是免费存的,给个保质期,别让 Redis 住成垃圾场。

### ⑬⑭⑮ 队列三连(第 4 讲整章)

- `Queue(名字, 交换机, routing_key=)`:常用尾巴还有 `queue_arguments={"x-max-priority": 10}`(开优先级)。`durable` 默认 True,不用写。
- `Exchange(名字, type="direct")`:多队列共用一个 direct 交换机、靠 routing_key 区分,是标准布局。
- `task_create_missing_queues=False`:**防呆必开**。默认 True 时路由到没声明的队列会现建一条——拼错名字的任务从此石沉大海;关掉它,拼错当场 `KeyError`。配套动作:**显式声明一条默认队列并让 worker 不吃它**(`-Q` 只给业务队列)——漏写路由的任务堆在兜底队列里,管理台一眼可见。注意默认队列必须出现在 `task_queues` 里,否则**所有任务**第一次投递就 `KeyError`(实测踩过)。
- `task_routes` 两种常用形态:一个 dict `{"任务名或通配": {"queue": ...}}`(主力,key 支持 `tasks.mail.*` 通配);函数式运行时算(极少用)。路由表里还能给 `exchange` / `routing_key` / `priority`。**别写成元组列表** `(("tasks.mail.*", {...}),)`——那不是合法形态,投递时当场报 `too many values to unpack`。

### ⑯⑰ 全局兜底(第 7 讲)

- `task_time_limit=60`:任何任务超 60 秒按失败硬杀(依赖 Unix 信号,Windows solo 上不生效,价值在 Linux 生产兑现)。个别任务用装饰器的 `soft_time_limit`(到点抛可捕获的异常)配 `time_limit` 分层覆盖。
- `worker_max_tasks_per_child=1000`:子进程干满 1000 个任务换新人,治第三方库内存泄漏的土方。还有 `worker_max_memory_per_child`(KB)按内存算的版本。

### ⑱ beat_schedule(第 5 讲整章)

条目三件:`task` 任务名字符串、`schedule`(float 秒,从 beat 启动起算 / `crontab(...)` 挂钟五字段)、`options`(apply_async 选项,周期任务常配 `expires` 防积压补跑)。beat 必须**单实例**。

## 另外两层的旋钮

配置三层的另外两层,各一张小表(合并规则:app.conf → 装饰器 → apply_async,**后写者赢**):

**装饰器层 `@app.task(...)`**:

| 旋钮 | 默认 | 说明 |
|---|---|---|
| `ignore_result=True` | False | 这个任务不写结果(发通知类) |
| `track_started=True` | False | 报告 STARTED 状态(默认只有 PENDING→终局) |
| `bind=True` | False | 把任务实例绑为第一参数 self(self.request.retries 从它来) |
| `base=自定义Task` | Task | 换任务基类:on_failure 告警挂这(第 3 讲) |
| `autoretry_for=(异常,...)` | () | 命中即自动重试,**只列会好的病** |
| `retry_backoff=True` | False | 指数退避 1、2、4……(填数字则从它起步) |
| `retry_backoff_max` | 600 | 退避间隔封顶 |
| `retry_jitter` | True | 间隔加随机抖动,生产必开 |
| `max_retries` | 3 | 耗尽 → FAILURE |
| `dont_autoretry_for=(异常,...)` | () | 例外:命中 autoretry_for 也不重试 |
| `soft_time_limit` / `time_limit` | 无 | 任务级时限,覆盖全局 |
| `name="自定义名"` | 模块.函数 | 一般别动:任务名是全局坐标 |

**调用端层 `task.apply_async(...)`**(delay 是它不带旋钮的快捷方式):

| 参数 | 说明 |
|---|---|
| `args=[...]` / `kwargs={...}` | 位置/关键字参数,必须可 json 序列化 |
| `countdown=N` | N 秒后执行——延时任务的正解(时区坑多的 eta 少用) |
| `expires=N` | 没人执行就作废(落到 RabbitMQ 就是单条消息 TTL) |
| `queue=名` / `priority=N` | 单次改投 / 插队(优先级要队列两头配,见下) |
| `link=签名` / `link_error=签名` | 成功/失败分支(第 6 讲) |

回执 `AsyncResult`:`id` / `state` / `ready()` / `get(timeout=, propagate=)` / `forget()`。**`.get()` 只许出现在脚本和测试里,Web 请求里是雪崩起点**(202 + 前端轮询才是正解,第 2 讲)。

## 命令速查

全部命令在**章节目录里**敲(cd 进去,`-A celery_app` 靠 import 找应用):

```
uv run celery -A celery_app worker --pool=solo -l info        # 启动 worker(Windows 必带 solo)
uv run celery -A celery_app worker -Q 队列1,队列2 -n 名字@%h   # 专职/通吃 worker,起名
uv run celery -A celery_app worker -E                          # 开任务事件(flower 要)
uv run celery -A celery_app beat -l info                       # 启动调度进程(只此一份)
uv run celery -A celery_app purge -f                          # 清空本应用配置的全部队列(-f 跳过确认)
uv run celery -A celery_app inspect ping|active|scheduled|registered|stats   # 只读点名
uv run celery -A celery_app control shutdown|add_consumer 名    # 远程下命令(广播)
uv run celery -A celery_app control revoke <task-id>           # 撤回未开跑的任务(在跑的默认撤不回)
uv run --with flower celery -A celery_app flower --port=5555   # 监控看板,浏览器 localhost:5555
```

`inspect scheduled` 专看"已领未跑"的 eta/countdown 任务;`purge` 是实验清场的老朋友(第 8 讲重跑前用)。

## 两头都设置才生效的东西

兔 MQ 那边一堆"两头"开关(aio-pika 速查里那节),Celery 把大多数收进了默认值——先记**框架已替你拧好的**:

- **durable 队列 + persistent 消息**:kombu 的 Queue/Exchange 默认 `durable=True`,任务消息默认 `delivery_mode=2` 落盘。兔 MQ 第 4 讲要你亲手记的两个开关,这里出厂即配。
- **交换机与绑定声明**:worker 启动自动做,不用你 declare(第 1 讲实验二去管理台看过它)。
- **发布连接重试**:`task_publish_retry` 默认 True,连接级错误自动重试。注意它救的是"连接断了",不是每条消息的 broker 确认——这版 celery/kombu 栈没有 aio-pika 那种 publisher confirm,极端窗口丢消息仍靠三件套兜底重投。

**还得自己两头的**,只剩一个正主:

- **优先级插队**:`Queue(..., queue_arguments={"x-max-priority": 10})` 开队列这头 + `apply_async(priority=9)` 消息这头。和兔 MQ 第 7 讲同一条规矩:缺哪头都退化成先进先出,数字越大越先跑,默认 5。

## 四个最容易翻车的点

1. **worker 不重载代码。** 改了任务模块,正在跑的 worker 毫不知情,行为不变也不报错——Ctrl+C 重启才生效。新任务在启动横幅 `[tasks]` 里看不到,先查 include 和 import。这是新手第一大坑。
2. **Windows 上池和时限都特殊。** worker 必带 `--pool=solo`(prefork 依赖 fork);时间限制依赖 Unix 信号,solo 上拦不住卡死任务——这两条配置的价值都在 Linux 生产上兑现,开发机上别被"没生效"吓到。
3. **beat 双开 = 每个周期任务双倍执行。** beat 没有选主机制,起两个各投各的。systemd 只配一个 beat 服务;多机怕单点用 redbeat(调度表进 Redis 自带锁),运营要后台改周期用 django-celery-beat。
4. **`.get()` 写进 Web 请求。** get 是阻塞的,worker 一慢 HTTP 连接全被拖住,雪崩就是这么来的。Web 端只许 delay + 拿 ID 返回,前端轮询状态接口。

顺带一句 Windows 专属:调用端脚本退出时**可能**跟一段红字 `Exception ignored while calling deallocator ...`——redis 客户端在解释器收尾时清理订阅连接的已知噪音,结果早已打印完,一律忽略。
