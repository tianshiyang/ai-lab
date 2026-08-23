# Python Redis 常用 API 与使用场景

本文只讲 Python 怎么操作 Redis。客户端使用官方常用库 [`redis-py`](https://redis.readthedocs.io/)，重点是项目里最常见的 API 和它们适合解决的问题。

不要把 Redis 当成主数据库。订单、支付、库存、用户资料等最终数据仍然应该落在 MySQL、PostgreSQL 等持久化数据库中；Redis 主要负责缓存、临时状态、计数、排序和并发控制。

## 1. 安装和连接

安装依赖：

```bash
uv add redis学习
```

同步客户端适合普通脚本、后台任务或同步 Web 框架：

```python
import os
import redis学习

r = redis学习.Redis.from_url(
    os.getenv("REDIS_URL", "redis学习://127.0.0.1:6379/0"),
    decode_responses=True,
)

r.ping()  # 连接正常时返回 True
```

`decode_responses=True` 会让 `get()` 返回字符串；否则返回 `bytes`。绝大多数文本业务建议开启它。

同一个进程只需要创建一次 `Redis` 对象。它内部会维护连接池，不要在每个接口里重新连接。

### FastAPI 异步客户端

`async def` 接口中使用异步版本，方法前加 `await`：

```python
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis学习.asyncio import Redis


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = Redis.from_url(
        os.getenv("REDIS_URL", "redis学习://127.0.0.1:6379/0"),
        decode_responses=True,
    )
    yield
    await app.state.redis.aclose()


app = FastAPI(lifespan=lifespan)
```

后文示例使用同步写法 `r.xxx()`，异步版把 `r` 换成异步客户端并加上 `await` 即可。

---

## 2. API 选择表

| 要解决的问题 | Redis 结构 | 常用 Python API |
| --- | --- | --- |
| 商品详情、页面配置缓存 | String | `get`、`set`、`delete`、`expire` |
| 验证码、会话、一次性令牌 | String | `set(ex=)`、`getdel`、`exists` |
| PV、次数统计 | String | `incr`、`incrby` |
| 用户资料、购物车 | Hash | `hset`、`hget`、`hgetall`、`hincrby` |
| 点赞、标签、去重 | Set | `sadd`、`sismember`、`scard`、`srem` |
| 积分榜、热度榜、延时任务 | ZSet | `zadd`、`zincrby`、`zrevrange`、`zrangebyscore` |
| 最近浏览、简单任务队列 | List | `lpush`、`lpop`、`lrange`、`ltrim` |
| 可靠的异步消息 | Stream | `xadd`、`xreadgroup`、`xack` |
| 接口限流、分布式锁、原子扣减 | String + Lua | `set(nx=True, ex=)`、`eval` |

Key 统一使用 `业务:对象:标识` 的格式，便于定位和清理：

```text
product:detail:42
session:5c1d...
cart:10086
article:like:9527
rank:weekly:2026-W34
lock:order:202608230001
```

---

## 3. String：缓存、验证码、会话、计数器

String 是最常用的结构，保存文本、数字或 JSON。

### 常用 API

```python
# 写入和读取
r.set("site:notice", "今晚 22:00 维护")
notice = r.get("site:notice")

# 写入并设置过期时间（秒）
r.set("auth:code:13800138000", "483921", ex=300)

# 仅当 key 不存在时写入；成功返回 True，已存在返回 None/False
locked = r.set("lock:demo", "worker-1", nx=True, ex=30)

# 过期、存在、删除
r.expire("site:notice", 3600)
seconds = r.ttl("site:notice")
exists = r.exists("site:notice")
r.delete("site:notice")

# 计数
r.incr("stats:pv:2026-08-23")
r.incrby("stats:pv:2026-08-23", 10)

# 读取并删除一次性值（Redis 6.2+）
code = r.getdel("auth:code:13800138000")
```

`ttl()` 返回：剩余秒数；`-1` 表示 key 没设置过期；`-2` 表示 key 不存在。

### 场景：商品详情缓存

读取时先查 Redis，未命中才查数据库；更新数据库成功后删缓存。

```python
import json
import random


def get_product(product_id: int) -> dict | None:
    key = f"product:detail:{product_id}"
    cached = r.get(key)

    if cached is not None:
        # 空字符串代表商品不存在，防止无效 ID 每次都打到数据库
        return json.loads(cached) if cached else None

    product = query_product_from_db(product_id)
    if product is None:
        r.set(key, "", ex=60)
        return None

    # TTL 加随机值，避免大量缓存同一时刻失效
    r.set(key, json.dumps(product), ex=1800 + random.randint(0, 300))
    return product


def update_product(product_id: int, payload: dict) -> None:
    update_product_to_db(product_id, payload)
    r.delete(f"product:detail:{product_id}")
```

适用：商品详情、文章详情、字典配置、首页内容等读多写少的数据。

注意：更新时先更新数据库，再删除缓存。缓存删掉后，由下一次读取重新建立。

### 场景：短信验证码

```python
CHECK_AND_DELETE_CODE = """
local code = redis学习.call('get', KEYS[1])
if code == ARGV[1] then
    redis学习.call('del', KEYS[1])
    return 1
end
return 0
"""


def create_code(phone: str, code: str) -> None:
    r.set(f"verify:login:{phone}", code, ex=5 * 60)


def verify_code(phone: str, input_code: str) -> bool:
    key = f"verify:login:{phone}"
    # 比对和删除是一个原子操作；只有成功验证才删除验证码
    return bool(r.eval(CHECK_AND_DELETE_CODE, 1, key, input_code))
```

适用：短信、邮箱验证码，重置密码令牌，一次性下载链接。

`getdel()` 的语义是「读取后无条件删除」，适合确实只需要消费一次的临时值；验证码核验要像上面这样做到「匹配成功才删除」。

### 场景：登录 Session

```python
import json
from uuid import uuid4


def create_session(user_id: int) -> str:
    token = uuid4().hex
    r.set(
        f"session:{token}",
        json.dumps({"user_id": user_id}),
        ex=7 * 24 * 3600,
    )
    return token


def get_session(token: str) -> dict | None:
    value = r.get(f"session:{token}")
    return json.loads(value) if value else None


def logout(token: str) -> None:
    r.delete(f"session:{token}")
```

适用：服务端会话、临时登录态、扫码登录状态。

### 场景：PV 和下载次数

```python
from datetime import date

key = f"stats:pv:article:9527:{date.today():%F}"
count = r.incr(key)

if count == 1:
    r.expire(key, 8 * 24 * 3600)
```

适用：页面访问次数、接口调用数、下载次数、登录失败次数。

`incr()` 与首次 `expire()` 分成两步。若必须保证计数器一定过期，使用后面的 Lua 脚本或数据库/定时任务兜底。

---

## 4. Hash：对象字段和购物车

Hash 很像 Python 的字典，一个 key 下保存多个字段。字段需要单独读取、单独修改时，比把整个对象序列化成 JSON 更合适。

### 常用 API

```python
# 写一个字段或多个字段
r.hset("user:profile:10086", "name", "张三")
r.hset("user:profile:10086", mapping={"city": "杭州", "level": "3"})

# 读取
name = r.hget("user:profile:10086", "name")
profile = r.hgetall("user:profile:10086")
fields = r.hmget("user:profile:10086", ["name", "city"])

# 数值字段增减、删除字段
r.hincrby("user:profile:10086", "points", 10)
r.hdel("user:profile:10086", "level")
```

Hash 的 value 依然是字符串。数值字段用 `hincrby()`；布尔值一般存 `"0"` / `"1"`。

### 场景：购物车

```python
def add_to_cart(user_id: int, sku_id: int, quantity: int = 1) -> int:
    key = f"cart:{user_id}"
    current = r.hincrby(key, f"sku:{sku_id}", quantity)
    r.expire(key, 30 * 24 * 3600)
    return current


def get_cart(user_id: int) -> dict[str, str]:
    return r.hgetall(f"cart:{user_id}")


def remove_from_cart(user_id: int, sku_id: int) -> None:
    r.hdel(f"cart:{user_id}", f"sku:{sku_id}")
```

适用：购物车的 `sku_id -> 数量`、用户资料摘要、商品的几个独立属性、功能开关。

购物车中不要把商品价格当最终价格。结算时仍然要从数据库读取商品、库存和优惠信息。

---

## 5. Set：点赞、标签、去重

Set 是无序且不重复的集合，特别适合「是否存在」和「去重」问题。

### 常用 API

```python
# 添加、删除；sadd 返回新增元素个数
added = r.sadd("article:like:9527", "10086")
r.srem("article:like:9527", "10086")

# 判断和计数
liked = r.sismember("article:like:9527", "10086")
like_count = r.scard("article:like:9527")

# 取成员；只适合元素数量可控的集合
user_ids = r.smembers("article:like:9527")

# 集合运算
common_tags = r.sinter("user:tags:10086", "article:tags:9527")
```

### 场景：点赞或收藏

```python
def like_article(article_id: int, user_id: int) -> bool:
    # 已点赞时返回 0；第一次点赞返回 1
    return r.sadd(f"article:like:{article_id}", str(user_id)) == 1


def unlike_article(article_id: int, user_id: int) -> bool:
    return r.srem(f"article:like:{article_id}", str(user_id)) == 1


def has_liked(article_id: int, user_id: int) -> bool:
    return bool(r.sismember(f"article:like:{article_id}", str(user_id)))
```

适用：点赞、收藏、用户标签、参与活动的用户、已处理消息 ID 去重。

如果还要维护一个点赞总数，`scard()` 能实时算出数量。超大集合上不建议每次都调用 `smembers()`；只查某个用户是否存在即可。

---

## 6. ZSet：排行榜和延时任务

ZSet（有序集合）中的成员不重复，每个成员有一个浮点数 `score`，可以按分数排序。

### 常用 API

```python
# 添加或更新分数
r.zadd("rank:game", {"10086": 98, "10087": 85})
r.zincrby("rank:game", 5, "10087")

# 倒序取前十名，并带分数
top_ten = r.zrevrange("rank:game", 0, 9, withscores=True)

# 查询名次：从 0 开始，未上榜返回 None
rank = r.zrevrank("rank:game", "10086")

# 按分数范围取数据、删除数据
due_ids = r.zrangebyscore("delay:cancel_order", 0, 1_800_000_000)
r.zrem("delay:cancel_order", "202608230001")
```

### 场景：积分排行榜

```python
def add_score(user_id: int, delta: int) -> float:
    return r.zincrby("rank:weekly:2026-W34", delta, str(user_id))


def get_top_ten() -> list[tuple[str, float]]:
    return r.zrevrange("rank:weekly:2026-W34", 0, 9, withscores=True)


def get_user_rank(user_id: int) -> int | None:
    rank = r.zrevrank("rank:weekly:2026-W34", str(user_id))
    return rank + 1 if rank is not None else None
```

适用：积分榜、热度榜、销量榜、最近活跃用户。周期榜使用不同 key，例如 `rank:monthly:2026-08`。

### 场景：延时取消未支付订单

```python
import time


def schedule_cancel(order_id: str) -> None:
    run_at = time.time() + 30 * 60
    r.zadd("delay:cancel_order", {order_id: run_at})


def get_due_orders(limit: int = 100) -> list[str]:
    return r.zrangebyscore(
        "delay:cancel_order", 0, time.time(), start=0, num=limit
    )
```

适用：30 分钟未付款取消订单、优惠券过期提醒、预约提醒。

任务被取出不代表可以直接取消订单。执行时先到数据库检查订单仍处于「待支付」，这样即使任务重复执行也不会取消已支付订单。

---

## 7. List：最近浏览和简单队列

List 是有顺序的列表，两端都可以进出数据。

### 常用 API

```python
# 左侧入队、右侧出队
r.lpush("queue:email", "task-1")
task = r.rpop("queue:email")

# 读取某个范围；下标 0 是最左侧
recent = r.lrange("history:10086", 0, 19)

# 只保留前 20 个元素
r.ltrim("history:10086", 0, 19)

# 阻塞读取，最长等 5 秒；适合简单消费者
item = r.brpop("queue:email", timeout=5)
```

### 场景：最近浏览

```python
def add_history(user_id: int, product_id: int) -> None:
    key = f"history:{user_id}"
    product_id = str(product_id)
    with r.pipeline(transaction=True) as pipe:
        pipe.lrem(key, 0, product_id)  # 清除旧位置
        pipe.lpush(key, product_id)     # 放到最前面
        pipe.ltrim(key, 0, 19)          # 只保留 20 条
        pipe.expire(key, 30 * 24 * 3600)
        pipe.execute()
```

适用：最近浏览、最近搜索、简单待办队列。

List 队列简单好用，但消费者处理到一半崩溃时可能丢任务，也不方便追踪确认状态。关键任务请使用 Streams 或专业消息队列。

---

## 8. 限流：`incr` + Lua

### 简单固定窗口限流

适合登录、发验证码、普通 API 防刷。下面限制同一用户每分钟 60 次：

```python
import time


def is_allowed(user_id: int, limit: int = 60) -> bool:
    minute = int(time.time() // 60)
    key = f"rate:api:{user_id}:{minute}"
    count = r.incr(key)
    if count == 1:
        r.expire(key, 70)
    return count <= limit
```

固定窗口实现简单，边界时刻可能会短时间放行超过 60 次。比如 12:00:59 用完 60 次，12:01:00 又可以再用 60 次。高风险接口用滑动窗口或令牌桶。

### 用 Lua 把「计数 + 首次设置过期」做成原子操作

```python
RATE_LIMIT_SCRIPT = """
local current = redis学习.call('incr', KEYS[1])
if current == 1 then
    redis学习.call('expire', KEYS[1], ARGV[1])
end
if current > tonumber(ARGV[2]) then
    return 0
end
return 1
"""


def is_allowed_atomic(user_id: int, limit: int = 60) -> bool:
    minute = int(time.time() // 60)
    key = f"rate:api:{user_id}:{minute}"
    allowed = r.eval(RATE_LIMIT_SCRIPT, 1, key, 70, limit)
    return bool(allowed)
```

适用：验证码发送、登录、导出报表、公开 API、评论发布。

限流维度可以换成 IP、用户 ID、接口名，或者组合起来：`rate:send_code:ip:{ip}`。

---

## 9. 分布式锁：防止多实例重复执行

当应用部署了多个实例时，Python 的 `threading.Lock()` 只对当前进程有效。Redis 锁用来控制「所有实例中只能有一个人处理」。

### 加锁和安全释放

```python
from uuid import uuid4

UNLOCK_SCRIPT = """
if redis学习.call('get', KEYS[1]) == ARGV[1] then
    return redis学习.call('del', KEYS[1])
end
return 0
"""


def rebuild_hot_cache(product_id: int) -> bool:
    key = f"lock:product:{product_id}"
    token = uuid4().hex

    # nx=True：仅 key 不存在时创建；ex：进程崩溃后自动释放
    acquired = r.set(key, token, nx=True, ex=30)
    if not acquired:
        return False

    try:
        rebuild_cache_from_database(product_id)
        return True
    finally:
        # 只能删除自己持有的锁，不能直接 r.delete(key)
        r.eval(UNLOCK_SCRIPT, 1, key, token)
```

适用：热点缓存重建、定时任务防止多实例重复跑、同一订单的重复处理。

锁必须有过期时间。Redis 锁不是数据库事务：支付、订单、库存依然要用数据库唯一约束、状态判断或乐观锁做最终兜底。

---

## 10. Pipeline：批量请求和多步更新

Pipeline 把多条命令合并为一次网络往返，适合批量读写。`transaction=True` 会用 Redis 事务把命令顺序执行，但不解决所有业务一致性问题。

```python
with r.pipeline(transaction=True) as pipe:
    pipe.get("user:profile:10086")
    pipe.sismember("article:like:9527", "10086")
    pipe.zrevrange("rank:weekly:2026-W34", 0, 9, withscores=True)
    profile, liked, top_ten = pipe.execute()
```

适用：一个接口需要同时读取多个缓存；批量写入；最近浏览的多步更新。

不要在 pipeline 中塞几万条命令。大批量操作要分批，例如每次 500～1000 条，避免占用过多内存和网络。

---

## 11. Stream：可靠的异步消息

Pub/Sub 的订阅者离线会错过消息。需要任务确认、失败重试时，用 Stream 的消费者组。

### 生产消息

```python
message_id = r.xadd(
    "stream:order",
    {"order_id": "202608230001", "user_id": "10086", "amount": "99.00"},
)
```

### 创建消费者组并消费

消费者组只需要创建一次。`mkstream=True` 允许流不存在时创建：

```python
from redis学习.exceptions import ResponseError

try:
    r.xgroup_create("stream:order", "order-workers", id="0", mkstream=True)
except ResponseError as exc:
    if "BUSYGROUP" not in str(exc):
        raise

messages = r.xreadgroup(
    groupname="order-workers",
    consumername="worker-1",
    streams={"stream:order": ">"},
    count=10,
    block=5000,
)

for stream_name, entries in messages:
    for message_id, data in entries:
        try:
            process_order(data)
        except Exception:
            # 不确认，后续由 Pending 消息处理逻辑重试
            continue
        r.xack(stream_name, "order-workers", message_id)
```

适用：下单后发通知、异步发邮件、生成报表、同步搜索索引、业务事件处理。

消费者逻辑必须幂等：同一个 `order_id` 重复执行也不能重复扣款、重复发货。消息成功处理后才调用 `xack()`。

---

## 12. 过期、删除与排查 API

这些方法在日常维护和排错时很常用：

```python
# 过期和删除
r.expire("cart:10086", 30 * 24 * 3600)
r.persist("cart:10086")      # 移除过期时间，通常谨慎使用
r.ttl("cart:10086")
r.delete("cart:10086")
r.unlink("large:cache:key")  # 异步删除大 key

# 查看 key 类型
kind = r.type("cart:10086")

# 线上迭代扫描，避免使用 keys("*")
for key in r.scan_iter(match="product:detail:*", count=100):
    print(key)
```

不要在线上使用 `r.keys("*")` 或一次性 `smembers()` / `lrange(0, -1)` 读取超大集合。它们会造成阻塞和内存压力。

---

## 13. 练习顺序

按下面顺序写小功能，学得最快：

1. 用 `set`、`get`、`delete` 写一个商品详情缓存。
2. 用 `set(ex=)`、`getdel` 写短信验证码。
3. 用 `incr` 写文章 PV 和登录失败次数。
4. 用 `hset`、`hincrby` 写购物车。
5. 用 `sadd`、`sismember` 写文章点赞。
6. 用 `zadd`、`zincrby`、`zrevrange` 写积分排行榜。
7. 用 `incr` + `eval` 给接口加限流。
8. 用 `set(nx=True, ex=)` + `eval` 写热点缓存重建锁。
9. 用 `xadd`、`xreadgroup`、`xack` 练一个异步发邮件任务。

学到第 6 步，就能处理大多数日常业务；后面三项主要用于并发和异步任务。

## 14. 写代码前的速查

| 场景 | 直接用什么 |
| --- | --- |
| 缓存 | `get` → 查数据库 → `set(ex=)`；更新后 `delete` |
| 验证码 | `set(ex=300)`；成功校验后 `getdel` |
| 登录 Session | `set(ex=)` 保存会话；退出时 `delete` |
| 计数 | `incr` / `incrby` |
| 购物车 | `hincrby` |
| 点赞、去重 | `sadd` / `sismember` |
| 排行榜 | `zincrby` / `zrevrange` |
| 最近浏览 | `lrem` + `lpush` + `ltrim`，放进 `pipeline` |
| 限流 | `incr` + `expire`；严格些用 `eval` |
| 多实例互斥 | `set(nx=True, ex=)`；Lua 校验 token 后释放 |
| 可靠异步任务 | `xadd` / `xreadgroup` / `xack` |

Redis API 本身不复杂。需要认真处理的是：临时数据要不要过期、并发时是不是原子操作、Redis 失效后数据库能不能兜住。
