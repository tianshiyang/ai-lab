# Python Redis：常用 API、练习与业务场景

这是一份 `redis-py` 学习笔记。只讲 Python 代码怎么写：API 的作用、参数、返回值，以及适合放在哪些业务里。

每个 API 后面都有一个小练习。练习都是一个函数，建议先自己写，再对照答案。每章最后还有一个把本章 API 串起来的实际场景练习。

五星表示这个 API 在实际项目中的常用程度：

| 星级 | 含义 |
| --- | --- |
| ⭐⭐⭐⭐⭐ | 高频。缓存、登录、购物车、限流等项目里经常出现。 |
| ⭐⭐⭐⭐ | 很常用。学完基础后马上会用到。 |
| ⭐⭐⭐ | 有明确场景时使用，不是每天都写。 |
| ⭐⭐ | 了解即可，排查或特定业务时有用。 |

> 本文示例假定配置了 `decode_responses=True`，因此读取结果是 `str`，不是 `bytes`。

## 1. 准备客户端

`redis学习/config.py` 可以保持下面这种写法：

```python
import os

import redis
from dotenv import load_dotenv

load_dotenv()

redis_client = redis.Redis.from_url(
    os.environ["REDIS_URL"],
    decode_responses=True,
)
```

`.env` 示例：

```dotenv
# 没有密码
REDIS_URL=redis://127.0.0.1:6379/0

# 有密码
# REDIS_URL=redis://:你的密码@127.0.0.1:6379/0
```

后面的代码统一这样导入：

```python
from redis学习.config import redis_client as r
```

### `ping()`：检查 Redis 是否能连接

重要程度：⭐⭐

```python
ok = r.ping()
print(ok)  # True
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 向 Redis 发一个连通性检查。 |
| 参数 | 无。 |
| 返回值 | 连接正常返回 `True`；连接失败会直接抛出异常。 |
| 使用场景 | 程序启动检查、排查环境变量或网络问题。 |

练习：写一个函数，返回 Redis 是否可连接。

```python
def exercise_ping() -> bool:
    """连接正常返回 True；连接不上时让异常抛出，便于定位配置问题。"""
    return r.ping()
```

### 本章大练习：启动健康检查

实际项目里，程序启动时常要检查 Redis 是否已经连好。

```python
def practice_redis_health_check() -> dict[str, str]:
    """返回可直接记录到启动日志中的检查结果。"""
    try:
        r.ping()
        return {"redis": "ok"}
    except Exception as exc:
        return {"redis": f"error: {exc}"}
```

---

## 2. String：缓存、验证码、会话和计数

String 是最常用的 Redis 类型。它能存文本、数字，也能存 JSON 字符串。

### `set()`：写入一个值

重要程度：⭐⭐⭐⭐⭐

```python
result = r.set("site:notice", "今晚 22:00 维护")
print(result)  # True
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 向一个 key 写入 value。key 已存在时，旧值会被覆盖。 |
| 常用参数 | `ex=秒数`：写入后多久自动过期；`nx=True`：仅 key 不存在时才写；`xx=True`：仅 key 已存在时才写。 |
| 返回值 | 普通写入成功是 `True`。使用 `nx=True` 或 `xx=True` 时，条件不满足通常是 `None`。 |
| 使用场景 | 缓存、验证码、Session、临时标记、分布式锁。 |

练习：保存一条网站公告，并返回 `set()` 的结果。

```python
def exercise_set_notice(content: str) -> bool:
    return r.set("site:notice", content)
```

#### `ex`：写入时设置有效期

```python
result = r.set("verify:login:13800138000", "483921", ex=300)
print(result)  # True
```

`ex=300` 的意思是：这个验证码只保留 300 秒，时间一到 Redis 自动删除它。验证码、缓存、Session 这种临时数据通常必须设置 `ex`。

练习：写一个五分钟后失效的验证码。

```python
def exercise_set_verify_code(phone: str, code: str) -> bool:
    return r.set(f"verify:login:{phone}", code, ex=5 * 60)
```

#### `nx=True`：只在 key 不存在时写入

```python
locked = r.set("lock:order:202608230001", "worker-1", nx=True, ex=30)

if locked:
    print("抢锁成功")
else:
    print("锁已经被其他实例拿到了")
```

这行代码的意思是：只有锁不存在时才创建锁；锁创建成功后 30 秒自动消失。

练习：为传入的订单号尝试创建一把 10 秒锁。

```python
def exercise_try_order_lock(order_id: str) -> bool | None:
    return r.set(f"lock:order:{order_id}", "practice-worker", nx=True, ex=10)
```

### `get()`：读取一个值

重要程度：⭐⭐⭐⭐⭐

```python
notice = r.get("site:notice")
print(notice)  # '今晚 22:00 维护'

missing = r.get("site:notice:not-exist")
print(missing)  # None
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 读取指定 key 的值。 |
| 参数 | 要读取的 key。 |
| 返回值 | key 存在时是 `str`；不存在时是 `None`。 |
| 使用场景 | 读取缓存、验证码、登录 Session、临时配置。 |

练习：读取网站公告；公告不存在时返回默认文案。

```python
def exercise_get_notice() -> str:
    notice = r.get("site:notice")
    return notice if notice is not None else "暂无公告"
```

判断缓存是否存在时，推荐写 `cached is not None`，不要只写 `if cached`。因为有些场景会故意把空字符串 `""` 缓存起来。

### `mget()`：一次读取多个值

重要程度：⭐⭐⭐

```python
values = r.mget(["product:detail:1", "product:detail:2", "product:detail:3"])
print(values)
# ['商品 1 的 JSON', None, '商品 3 的 JSON']
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 一次读取多个 String key，减少网络请求次数。 |
| 参数 | key 的列表或元组。 |
| 返回值 | `list`，顺序和传入的 key 相同；不存在的 key 对应位置是 `None`。 |
| 使用场景 | 商品列表页批量取详情缓存、批量取用户状态。 |

练习：批量读取三个商品的缓存。

```python
def exercise_mget_products(product_ids: list[int]) -> list[str | None]:
    keys = [f"product:detail:{product_id}" for product_id in product_ids]
    return r.mget(keys)
```

### `delete()`：删除一个或多个 key

重要程度：⭐⭐⭐⭐⭐

```python
deleted_count = r.delete("site:notice", "site:notice:backup")
print(deleted_count)  # 0、1 或 2
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 删除一个或多个 key。 |
| 参数 | 一个或多个 key。 |
| 返回值 | 实际删除成功的 key 数量，类型是 `int`。不存在的 key 不计数。 |
| 使用场景 | 更新数据库后删缓存、退出登录、清理验证码。 |

练习：删除某个商品详情缓存，并返回是否真的删到了内容。

```python
def exercise_delete_product_cache(product_id: int) -> bool:
    deleted_count = r.delete(f"product:detail:{product_id}")
    return deleted_count == 1
```

### `exists()`：判断 key 是否存在

重要程度：⭐⭐⭐

```python
count = r.exists("session:abc", "session:def")
print(count)  # 0、1 或 2
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 判断一个或多个 key 是否存在。 |
| 参数 | 一个或多个 key。 |
| 返回值 | 存在的 key 数量，类型是 `int`。 |
| 使用场景 | 判断 Session 是否有效、锁是否存在、某个缓存是否创建。 |

练习：判断一个 Session 是否还有效。

```python
def exercise_session_exists(token: str) -> bool:
    return r.exists(f"session:{token}") == 1
```

### `expire()`：给已有 key 设置过期时间

重要程度：⭐⭐⭐⭐⭐

```python
result = r.expire("cart:10086", 30 * 24 * 60 * 60)
print(result)  # True 或 False
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 给一个已经存在的 key 设置有效期；时间到了 Redis 自动删除它。 |
| 参数 | 第一个参数是 key，第二个参数是秒数。上例表示购物车保留 30 天。 |
| 返回值 | key 存在且设置成功时是 `True`；key 不存在时是 `False`。 |
| 使用场景 | 购物车、最近浏览、计数器等已有数据的过期处理。 |

`expire()` 不会创建 key。新数据写入时，优先用 `set(..., ex=秒数)`；`expire()` 适合给已有数据补有效期或续期。

练习：把指定用户的购物车续期 30 天。

```python
def exercise_refresh_cart_expire(user_id: int) -> bool:
    return r.expire(f"cart:{user_id}", 30 * 24 * 60 * 60)
```

### `ttl()`：查看 key 还有多久过期

重要程度：⭐⭐⭐⭐

```python
seconds = r.ttl("cart:10086")
print(seconds)  # 例如 2591987
```

| 返回值 | 含义 |
| --- | --- |
| 正整数或 `0` | key 距离过期还剩多少秒。 |
| `-1` | key 存在，但没有设置过期时间。 |
| `-2` | key 不存在。 |

使用场景：排查验证码是否过期、购物车为何没有自动清理、缓存是否漏设 TTL。

练习：把 TTL 的特殊数字转换成容易读懂的提示。

```python
def exercise_read_ttl(key: str) -> str:
    seconds = r.ttl(key)
    if seconds == -2:
        return "key 不存在"
    if seconds == -1:
        return "key 未设置过期时间"
    return f"还剩 {seconds} 秒过期"
```

### `getdel()`：读取后马上删除

重要程度：⭐⭐⭐

```python
token = r.getdel("temp:download-token:abc")
print(token)  # key 存在时为 str；不存在时为 None
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 读取值并立刻删除该 key，这两个动作由 Redis 一次完成。 |
| 参数 | 要读取并删除的 key。 |
| 返回值 | key 存在时是原来的 `str`；不存在时是 `None`。 |
| 使用场景 | 一次性下载令牌、只能取一次的临时任务数据。 |

它会无条件删除，所以不适合直接校验验证码。用户输错验证码时，不能把正确验证码删掉。

练习：领取一个只能使用一次的邀请码。

```python
def exercise_claim_invite_code(invite_id: str) -> str | None:
    return r.getdel(f"invite:once:{invite_id}")
```

### `incr()`：把数字加 1

重要程度：⭐⭐⭐⭐

```python
new_count = r.incr("stats:pv:2026-08-23")
print(new_count)  # 1；再次调用后是 2
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 把一个整数值加 1。key 不存在时从 0 开始计数。 |
| 参数 | key 中必须是整数文本。 |
| 返回值 | 加 1 后的最新值，类型是 `int`。 |
| 使用场景 | PV、下载次数、失败次数、接口调用次数。 |

练习：记录一篇文章被阅读一次，并返回新的阅读量。

```python
def exercise_incr_article_views(article_id: int) -> int:
    return r.incr(f"article:views:{article_id}")
```

### `incrby()`：把数字增加指定数量

重要程度：⭐⭐⭐⭐

```python
new_points = r.incrby("user:points:10086", 10)
print(new_points)  # 加 10 后的最新积分
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 给一个整数加指定数值。`amount` 为负数时就是减法。 |
| 参数 | key、增加量 `amount`。 |
| 返回值 | 加减后的最新整数，类型是 `int`。 |
| 使用场景 | 加积分、扣积分、批量统计。 |

练习：给用户增加积分，返回增加后的总积分。

```python
def exercise_add_points(user_id: int, points: int) -> int:
    return r.incrby(f"user:points:{user_id}", points)
```

### 本章大练习：商品详情缓存

目标：第一次传入商品详情时写缓存；后续读取时直接返回缓存中的内容；缓存保留半小时。

```python
import json


def practice_product_cache(product_id: int, product: dict) -> dict:
    key = f"product:detail:{product_id}"
    cached = r.get(key)

    if cached is not None:
        return json.loads(cached)

    r.set(key, json.dumps(product), ex=30 * 60)
    return product
```

这个函数练到了 `get()`、`set()`、`ex` 和 JSON 序列化。接上真实数据库后，`product` 应该由数据库查询结果替代。修改商品数据后要执行：

```python
r.delete(f"product:detail:{product_id}")
```

---

## 3. Hash：对象字段和购物车

Hash 可以理解成 Redis 里的小字典：一个 key 下面有很多字段。适合对象的字段需要单独读写的情况。

### `hset()`：写入字段

重要程度：⭐⭐⭐⭐

```python
# 写一个字段
created_count = r.hset("user:profile:10086", "name", "张三")

# 一次写多个字段
created_count = r.hset(
    "user:profile:10086",
    mapping={"city": "杭州", "level": "3"},
)
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 写入 Hash 中的一个或多个字段；字段已存在时覆盖旧值。 |
| 参数 | `name` 是 Hash key；单字段传 `field, value`；多字段传 `mapping={字段: 值}`。 |
| 返回值 | 新增字段数，类型是 `int`；覆盖已有字段不计数。 |
| 使用场景 | 用户资料摘要、购物车、商品属性、功能开关。 |

练习：保存用户昵称和所在城市。

```python
def exercise_hset_profile(user_id: int, name: str, city: str) -> int:
    return r.hset(
        f"user:profile:{user_id}",
        mapping={"name": name, "city": city},
    )
```

### `hget()`：读取一个字段

重要程度：⭐⭐⭐⭐

```python
name = r.hget("user:profile:10086", "name")
print(name)  # '张三'；字段不存在时为 None
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 读取 Hash 中的一个字段。 |
| 参数 | Hash key、字段名。 |
| 返回值 | 字段存在时为 `str`；Hash 或字段不存在时为 `None`。 |
| 使用场景 | 读取昵称、购物车中某个 SKU 数量、用户积分。 |

练习：读取用户昵称；没有昵称时返回“未设置”。

```python
def exercise_hget_name(user_id: int) -> str:
    name = r.hget(f"user:profile:{user_id}", "name")
    return name if name is not None else "未设置"
```

### `hgetall()`：读取全部字段

重要程度：⭐⭐⭐

```python
profile = r.hgetall("user:profile:10086")
print(profile)  # {'name': '张三', 'city': '杭州', 'level': '3'}
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 读取一个 Hash 中的全部字段。 |
| 参数 | Hash key。 |
| 返回值 | `dict[str, str]`；Hash 不存在时返回空字典 `{}`。 |
| 使用场景 | 读取小型用户资料、完整购物车。 |

字段很多时不要反复调用 `hgetall()`，应只用 `hget()` 读取需要的字段。

练习：读取用户资料；不存在时返回空字典。

```python
def exercise_hgetall_profile(user_id: int) -> dict[str, str]:
    return r.hgetall(f"user:profile:{user_id}")
```

### `hincrby()`：修改 Hash 中的数字字段

重要程度：⭐⭐⭐⭐

```python
new_quantity = r.hincrby("cart:10086", "sku:1001", 1)
print(new_quantity)  # 例如 3
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 给 Hash 中一个整数值加指定数值；字段不存在时从 0 开始。 |
| 参数 | Hash key、字段名、增减数量。数量可为负数。 |
| 返回值 | 加减后的最新整数。 |
| 使用场景 | 修改购物车数量、用户积分、单个商品的浏览数。 |

练习：给购物车里的某个 SKU 加一件。

```python
def exercise_hincrby_cart(user_id: int, sku_id: int) -> int:
    return r.hincrby(f"cart:{user_id}", f"sku:{sku_id}", 1)
```

### `hdel()`：删除一个或多个字段

重要程度：⭐⭐⭐

```python
deleted_count = r.hdel("cart:10086", "sku:1001", "sku:1002")
print(deleted_count)  # 实际删除成功的字段数量
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 只删除 Hash 中的字段，不会删除同一个 Hash 的其他字段。 |
| 参数 | Hash key 和一个或多个字段名。 |
| 返回值 | 实际删除的字段数量，类型是 `int`。 |
| 使用场景 | 从购物车移除商品、移除用户临时属性。 |

练习：从购物车删除一个 SKU，并返回是否删除成功。

```python
def exercise_hdel_cart_sku(user_id: int, sku_id: int) -> bool:
    deleted_count = r.hdel(f"cart:{user_id}", f"sku:{sku_id}")
    return deleted_count == 1
```

### 本章大练习：购物车

目标：加入商品时数量累加、购物车续期 30 天、读取当前购物车。

```python
def practice_add_to_cart(user_id: int, sku_id: int, quantity: int = 1) -> dict[str, str]:
    key = f"cart:{user_id}"
    r.hincrby(key, f"sku:{sku_id}", quantity)
    r.expire(key, 30 * 24 * 60 * 60)
    return r.hgetall(key)
```

购物车只保存 `sku_id -> 数量`。价格、库存和优惠要在结算时从数据库重新读取。

---

## 4. Set：点赞、标签和去重

Set 是无序且不重复的集合。最适合回答“某个人是否已经做过某事”。

### `sadd()`：添加成员

重要程度：⭐⭐⭐⭐

```python
added_count = r.sadd("article:like:9527", "10086")
print(added_count)  # 第一次点赞是 1；重复点赞是 0
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 向 Set 添加一个或多个成员；同一个成员只保留一份。 |
| 参数 | Set key 和一个或多个成员。 |
| 返回值 | 实际新增的成员数，类型是 `int`。 |
| 使用场景 | 点赞、收藏、用户标签、消息去重、活动报名。 |

练习：为文章添加一个点赞，并返回是否是第一次点赞。

```python
def exercise_sadd_like(article_id: int, user_id: int) -> bool:
    added_count = r.sadd(f"article:like:{article_id}", str(user_id))
    return added_count == 1
```

### `sismember()`：判断成员是否存在

重要程度：⭐⭐⭐⭐

```python
liked = r.sismember("article:like:9527", "10086")
print(liked)  # True 或 False
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 判断一个成员是否属于某个 Set。 |
| 参数 | Set key、成员。 |
| 返回值 | `True` 或 `False`。 |
| 使用场景 | 判断当前用户是否点赞、是否领过券、消息是否处理过。 |

练习：判断用户是否已经报名活动。

```python
def exercise_sismember_activity(activity_id: int, user_id: int) -> bool:
    return r.sismember(f"activity:users:{activity_id}", str(user_id))
```

### `srem()`：移除成员

重要程度：⭐⭐⭐

```python
removed_count = r.srem("article:like:9527", "10086")
print(removed_count)  # 取消成功是 1；原本没有点赞是 0
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 从 Set 中移除一个或多个成员。 |
| 参数 | Set key 和一个或多个成员。 |
| 返回值 | 实际移除的成员数，类型是 `int`。 |
| 使用场景 | 取消点赞、退出活动、移除标签。 |

练习：取消文章点赞并返回是否成功。

```python
def exercise_srem_like(article_id: int, user_id: int) -> bool:
    removed_count = r.srem(f"article:like:{article_id}", str(user_id))
    return removed_count == 1
```

### `scard()`：获取集合人数

重要程度：⭐⭐⭐⭐

```python
like_count = r.scard("article:like:9527")
print(like_count)  # 例如 128
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 获取 Set 中成员的数量。 |
| 参数 | Set key。 |
| 返回值 | 成员数量，类型是 `int`。Set 不存在时为 `0`。 |
| 使用场景 | 点赞人数、参与人数、在线用户数。 |

练习：获取指定文章的点赞数。

```python
def exercise_scard_likes(article_id: int) -> int:
    return r.scard(f"article:like:{article_id}")
```

### `smembers()`：读取全部成员

重要程度：⭐⭐⭐

```python
user_ids = r.smembers("article:like:9527")
print(user_ids)  # {'10086', '10087'}
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 读取 Set 中的全部成员。 |
| 参数 | Set key。 |
| 返回值 | `set[str]`。Set 不存在时是空集合 `set()`。 |
| 使用场景 | 小型活动名单、读取少量标签。 |

不要在成员非常多的 Set 上调用它。百万点赞用户不能一次全读到 Python 内存里。

练习：读取一场小型活动的报名用户。

```python
def exercise_smembers_activity(activity_id: int) -> set[str]:
    return r.smembers(f"activity:users:{activity_id}")
```

### `sinter()`：求多个集合的交集

重要程度：⭐⭐⭐

```python
common_tags = r.sinter("user:tags:10086", "article:tags:9527")
print(common_tags)  # 两边都有的标签
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 取多个 Set 都存在的成员。 |
| 参数 | 两个或多个 Set key。 |
| 返回值 | `set[str]`。没有交集时是空集合。 |
| 使用场景 | 用户与商品共同标签、共同好友、同时参加多个活动的人。 |

练习：找出用户和课程的共同标签。

```python
def exercise_sinter_tags(user_id: int, course_id: int) -> set[str]:
    return r.sinter(f"user:tags:{user_id}", f"course:tags:{course_id}")
```

### 本章大练习：文章点赞

目标：第一次点赞返回 `True`；重复点赞返回 `False`；同时返回最新点赞数。

```python
def practice_like_article(article_id: int, user_id: int) -> dict[str, int | bool]:
    key = f"article:like:{article_id}"
    added_count = r.sadd(key, str(user_id))
    like_count = r.scard(key)
    return {"is_new_like": added_count == 1, "like_count": like_count}
```

---

## 5. ZSet：排行榜和延时任务

ZSet 是有序集合。成员不重复，但每个成员都有一个 `score` 分数，Redis 会按分数排序。

### `zadd()`：添加成员和分数

重要程度：⭐⭐⭐⭐

```python
added_count = r.zadd("rank:game", {"10086": 98, "10087": 85})
print(added_count)  # 2
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 添加成员和分数；成员已经存在时更新分数。 |
| 参数 | ZSet key 和 `{成员: 分数}` 字典；分数可以是 `int` 或 `float`。 |
| 返回值 | 新增成员数量，类型是 `int`。更新已有成员不计数。 |
| 使用场景 | 初始化排行榜、添加延时任务。 |

练习：把用户的初始积分放进周榜。

```python
def exercise_zadd_rank(user_id: int, score: int) -> int:
    return r.zadd("rank:weekly:2026-W34", {str(user_id): score})
```

### `zincrby()`：增加成员分数

重要程度：⭐⭐⭐⭐⭐

```python
new_score = r.zincrby("rank:game", 5, "10087")
print(new_score)  # 90.0
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 给一个成员的分数加指定数值；成员不存在时自动创建。 |
| 参数 | ZSet key、增加量 `amount`、成员。 |
| 返回值 | 增加后的最新分数，类型是 `float`。 |
| 使用场景 | 加积分、增加热度、销量加一。 |

练习：用户完成任务后加积分。

```python
def exercise_zincrby_points(user_id: int, points: int) -> float:
    return r.zincrby("rank:weekly:2026-W34", points, str(user_id))
```

### `zrevrange()`：按分数从高到低取成员

重要程度：⭐⭐⭐⭐⭐

```python
top_ten = r.zrevrange("rank:game", 0, 9, withscores=True)
print(top_ten)
# [('10086', 99.0), ('10087', 90.0)]
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 按 score 从高到低读取指定范围。 |
| 参数 | `0, 9` 表示前十名；`withscores=True` 表示同时返回分数。 |
| 返回值 | 不带分数时是 `list[str]`；带分数时是 `list[tuple[str, float]]`。 |
| 使用场景 | 展示积分榜、销量榜、热度榜。 |

练习：获取周榜前十名和积分。

```python
def exercise_zrevrange_top_ten() -> list[tuple[str, float]]:
    return r.zrevrange("rank:weekly:2026-W34", 0, 9, withscores=True)
```

### `zrevrank()`：获取某成员名次

重要程度：⭐⭐⭐⭐

```python
rank = r.zrevrank("rank:game", "10086")
print(rank)  # 0，表示第一名
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 读取成员按分数从高到低的名次。 |
| 参数 | ZSet key、成员。 |
| 返回值 | 名次 `int`；第一名是 `0`；成员不在榜上时为 `None`。 |
| 使用场景 | 展示“我当前第几名”。 |

练习：获取用户展示给前端的名次。未上榜时返回 `None`。

```python
def exercise_zrevrank_display(user_id: int) -> int | None:
    rank = r.zrevrank("rank:weekly:2026-W34", str(user_id))
    return rank + 1 if rank is not None else None
```

### `zrangebyscore()`：按分数范围读取成员

重要程度：⭐⭐⭐⭐

```python
import time

due_order_ids = r.zrangebyscore(
    "delay:cancel-order",
    0,
    time.time(),
    start=0,
    num=100,
)
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 读取 score 落在指定范围内的成员。 |
| 参数 | 最小分数、最大分数；`start` 和 `num` 用于分页/限制数量。 |
| 返回值 | `list[str]`。没有符合条件的成员时是 `[]`。 |
| 使用场景 | 查询到期延时任务、按时间找最近活跃用户。 |

练习：取出当前已经到期、最多 50 条的取消订单任务。

```python
def exercise_zrange_due_orders() -> list[str]:
    import time

    return r.zrangebyscore("delay:cancel-order", 0, time.time(), 0, 50)
```

### `zrem()`：删除成员

重要程度：⭐⭐⭐

```python
removed_count = r.zrem("delay:cancel-order", "202608230001")
print(removed_count)  # 1 或 0
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 从 ZSet 中删除一个或多个成员。 |
| 参数 | ZSet key 和一个或多个成员。 |
| 返回值 | 实际删除数量，类型是 `int`。 |
| 使用场景 | 延时任务完成后移除、移除下榜用户。 |

练习：删除一个已经处理完的延时订单任务。

```python
def exercise_zrem_due_order(order_id: str) -> bool:
    return r.zrem("delay:cancel-order", order_id) == 1
```

### 本章大练习：积分排行榜

目标：用户加分后返回他的新分数、排名和榜单前十。

```python
def practice_add_score_and_rank(user_id: int, points: int) -> dict:
    key = "rank:weekly:2026-W34"
    score = r.zincrby(key, points, str(user_id))
    rank = r.zrevrank(key, str(user_id))
    top_ten = r.zrevrange(key, 0, 9, withscores=True)

    return {
        "score": score,
        "rank": rank + 1 if rank is not None else None,
        "top_ten": top_ten,
    }
```

---

## 6. List：最近浏览和简单队列

List 是有顺序的列表。可以从左侧或右侧加入、取出元素。

### `lpush()`：从左侧加入元素

重要程度：⭐⭐⭐⭐

```python
length = r.lpush("history:10086", "product:9527")
print(length)  # 放入后列表长度，例如 1
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 把一个或多个元素插入 List 的左侧（最前面）。 |
| 返回值 | 插入后 List 的总长度，类型是 `int`。 |
| 使用场景 | 最新浏览记录、简单任务入队。 |

练习：向用户最近搜索记录中放入一个关键词。

```python
def exercise_lpush_search_history(user_id: int, keyword: str) -> int:
    return r.lpush(f"search:history:{user_id}", keyword)
```

### `rpop()`：从右侧取出元素

重要程度：⭐⭐⭐⭐

```python
task = r.rpop("queue:email")
print(task)  # 'task-1'；队列为空时为 None
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 从 List 右侧取出并删除一个元素。 |
| 返回值 | 取到元素时为 `str`；List 为空或不存在时为 `None`。 |
| 使用场景 | 配合 `lpush()` 实现先进先出的简单队列。 |

练习：从邮件队列中取一个任务。

```python
def exercise_rpop_email_task() -> str | None:
    return r.rpop("queue:email")
```

### `lpop()`：从左侧取出元素

重要程度：⭐⭐⭐

```python
task = r.lpop("stack:undo")
print(task)  # str 或 None
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 从 List 左侧取出并删除一个元素。 |
| 返回值 | 取到元素时为 `str`；List 为空时为 `None`。 |
| 使用场景 | 栈结构，例如撤销记录、后进先出任务。 |

练习：取出最近的一条撤销操作。

```python
def exercise_lpop_undo() -> str | None:
    return r.lpop("stack:undo")
```

### `lrange()`：读取一段数据，不删除

重要程度：⭐⭐⭐⭐

```python
recent = r.lrange("history:10086", 0, 19)
print(recent)  # 最近 20 条记录
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 读取 List 中指定下标范围的数据，不删除数据。 |
| 参数 | `0, 19` 表示第 0 到第 19 条，两端都包含。 |
| 返回值 | `list[str]`。List 不存在或范围内没有数据时为 `[]`。 |
| 使用场景 | 展示最近浏览、最近搜索、查看队列前几条任务。 |

不要对大 List 使用 `lrange(key, 0, -1)`，它会把所有数据一次读进 Python 内存。

练习：读取用户最近 10 条搜索记录。

```python
def exercise_lrange_search_history(user_id: int) -> list[str]:
    return r.lrange(f"search:history:{user_id}", 0, 9)
```

### `lrem()`：按值删除元素

重要程度：⭐⭐⭐

```python
removed_count = r.lrem("history:10086", 0, "product:9527")
print(removed_count)  # 实际删除数量
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 删除 List 中等于指定值的元素。 |
| 参数 | `count=0` 表示删除全部匹配项。 |
| 返回值 | 实际删除的元素数量，类型是 `int`。 |
| 使用场景 | 最近浏览去重、删除指定待办。 |

练习：从搜索记录中清除指定关键词的所有旧记录。

```python
def exercise_lrem_search_history(user_id: int, keyword: str) -> int:
    return r.lrem(f"search:history:{user_id}", 0, keyword)
```

### `ltrim()`：只保留某一段数据

重要程度：⭐⭐⭐⭐

```python
result = r.ltrim("history:10086", 0, 19)
print(result)  # True
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 只保留下标范围内的元素，其他元素删除。 |
| 参数 | `0, 19` 表示只保留最前面的 20 条。 |
| 返回值 | 成功时为 `True`。 |
| 使用场景 | 限制最近浏览、最近搜索、队列最大长度。 |

练习：把用户搜索记录限制为最近 10 条。

```python
def exercise_ltrim_search_history(user_id: int) -> bool:
    return r.ltrim(f"search:history:{user_id}", 0, 9)
```

### `brpop()`：阻塞等待队列任务

重要程度：⭐⭐⭐

```python
item = r.brpop("queue:email", timeout=5)
print(item)
# 有任务：('queue:email', 'task-1')
# 5 秒内没有任务：None
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 从右侧取任务；队列为空时等待新任务，而不是马上返回。 |
| 参数 | `timeout=5` 最多等待 5 秒；`timeout=0` 表示一直等。 |
| 返回值 | 有任务时是 `(队列名, 任务内容)`；超时是 `None`。 |
| 使用场景 | 简单后台消费者。 |

练习：等待最多 2 秒，取一条邮件任务。

```python
def exercise_brpop_email_task() -> tuple[str, str] | None:
    return r.brpop("queue:email", timeout=2)
```

### 本章大练习：最近浏览

目标：同一商品只保留一条、最新浏览放最前面、最多保存 20 条、30 天过期。

```python
def practice_add_browse_history(user_id: int, product_id: int) -> list[str]:
    key = f"history:{user_id}"
    value = str(product_id)

    with r.pipeline(transaction=True) as pipe:
        pipe.lrem(key, 0, value)
        pipe.lpush(key, value)
        pipe.ltrim(key, 0, 19)
        pipe.expire(key, 30 * 24 * 60 * 60)
        pipe.lrange(key, 0, 19)
        results = pipe.execute()

    return results[-1]
```

关键任务不建议只用 List 队列。消费者取到任务后如果进程崩溃，任务可能丢失；可靠任务应使用 Stream 或专业消息队列。

---

## 7. Pipeline：把多条命令一起执行

### `pipeline()` 和 `execute()`

重要程度：⭐⭐⭐⭐

```python
with r.pipeline(transaction=True) as pipe:
    pipe.get("user:profile:10086")
    pipe.sismember("article:like:9527", "10086")
    pipe.zrevrange("rank:weekly:2026-W34", 0, 9, withscores=True)

    results = pipe.execute()

profile, liked, top_ten = results
```

| API | 作用 | 返回值 |
| --- | --- | --- |
| `r.pipeline(transaction=True)` | 创建管道；后续命令先放进管道里。 | `Pipeline` 对象。 |
| `pipe.xxx(...)` | 暂存一条 Redis 命令，此时命令还没执行。 | `Pipeline` 对象。 |
| `pipe.execute()` | 一次发送并执行管道里的全部命令。 | `list`，每一项是对应命令的返回值，顺序相同。 |

使用场景：一个接口需要读多个缓存、一次写多个数据、最近浏览这种多步更新。

练习：用一个管道同时读取用户资料、是否点赞和榜单前十。

```python
def exercise_pipeline_read_home(user_id: int) -> dict:
    with r.pipeline(transaction=True) as pipe:
        pipe.hgetall(f"user:profile:{user_id}")
        pipe.sismember("article:like:9527", str(user_id))
        pipe.zrevrange("rank:weekly:2026-W34", 0, 9, withscores=True)
        profile, liked, top_ten = pipe.execute()

    return {"profile": profile, "liked": liked, "top_ten": top_ten}
```

### 本章大练习：批量读取商品缓存

目标：一次拿到多个商品缓存，并把命中的 JSON 转成字典。

```python
import json


def practice_batch_get_products(product_ids: list[int]) -> dict[int, dict | None]:
    keys = [f"product:detail:{product_id}" for product_id in product_ids]

    with r.pipeline() as pipe:
        for key in keys:
            pipe.get(key)
        cached_values = pipe.execute()

    return {
        product_id: json.loads(value) if value is not None else None
        for product_id, value in zip(product_ids, cached_values, strict=True)
    }
```

不要把几万条命令一次塞进 pipeline。批量任务一般每批 500～1000 条。

---

## 8. `eval()`：Lua 原子操作、限流和分布式锁

`eval()` 用来执行 Lua 脚本。它适合“读取 → 判断 → 修改”必须一次完成的场景。

### `eval()`：执行 Lua 脚本

重要程度：⭐⭐⭐⭐

```python
result = r.eval(script, numkeys, *keys_and_args)
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 在 Redis 内执行 Lua，把多步操作变成一个原子操作。 |
| 参数 | `script`：Lua 字符串；`numkeys`：后面有几个参数是 key；然后先传所有 key，再传普通参数。Lua 里用 `KEYS[1]` 和 `ARGV[1]` 读取。 |
| 返回值 | Lua 中 `return` 什么，Python 就拿到对应的值。例如 Lua `return 1`，Python 得到整数 `1`。 |
| 使用场景 | 验证码正确才删除、严格限流、安全解锁、原子扣减。 |

练习：验证码正确才删除，错误时保留验证码。

```python
CHECK_AND_DELETE_CODE = """
local code = redis.call('get', KEYS[1])
if code == ARGV[1] then
    redis.call('del', KEYS[1])
    return 1
end
return 0
"""


def exercise_eval_verify_code(phone: str, input_code: str) -> bool:
    key = f"verify:login:{phone}"
    result = r.eval(CHECK_AND_DELETE_CODE, 1, key, input_code)
    return result == 1
```

### 本章大练习：热点缓存重建锁

目标：多个服务实例同时发现缓存过期时，只让一个实例执行重建。这里的 `rebuild` 是你传入的实际重建函数。

```python
from collections.abc import Callable
from uuid import uuid4

UNLOCK_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


def practice_rebuild_hot_cache(product_id: int, rebuild: Callable[[], None]) -> bool:
    lock_key = f"lock:product:{product_id}"
    token = uuid4().hex

    acquired = r.set(lock_key, token, nx=True, ex=30)
    if not acquired:
        return False

    try:
        rebuild()
        return True
    finally:
        r.eval(UNLOCK_SCRIPT, 1, lock_key, token)
```

`set(nx=True, ex=30)` 成功时返回 `True`，说明当前实例拿到了锁；返回 `None` 表示已有其他实例在处理。

不能直接 `r.delete(lock_key)` 解锁。锁可能已经过期并被别人重新拿到；Lua 会先核对 token，只删除自己的锁。

---

## 9. Stream：可靠的异步消息

Stream 用于异步任务：生产者写消息，消费者处理消息，成功后确认。比 List 队列更适合订单通知、邮件、报表这类任务。

### `xadd()`：写一条消息

重要程度：⭐⭐⭐⭐

```python
message_id = r.xadd(
    "stream:order",
    {"order_id": "202608230001", "user_id": "10086", "amount": "99.00"},
)
print(message_id)  # 例如 '1750000000000-0'
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 往 Stream 追加一条消息。 |
| 参数 | Stream key、消息字段字典。 |
| 返回值 | 新消息 ID，类型是 `str`。 |
| 使用场景 | 下单后发通知、异步发邮件、生成报表、同步搜索索引。 |

练习：添加一条“发送欢迎邮件”的任务。

```python
def exercise_xadd_welcome_email(user_id: int, email: str) -> str:
    return r.xadd(
        "stream:email",
        {"type": "welcome", "user_id": str(user_id), "email": email},
    )
```

### `xgroup_create()`：创建消费者组

重要程度：⭐⭐⭐

```python
from redis.exceptions import ResponseError

try:
    created = r.xgroup_create(
        "stream:order",
        "order-workers",
        id="0",
        mkstream=True,
    )
    print(created)  # True
except ResponseError as exc:
    if "BUSYGROUP" not in str(exc):
        raise
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 创建消费者组；同组中的消息会分配给不同消费者。 |
| 重要参数 | `id="0"` 从旧消息开始；`id="$"` 只接收以后新消息；`mkstream=True` 在 Stream 不存在时创建它。 |
| 返回值 | 创建成功是 `True`；组已存在时抛 `ResponseError`。 |
| 使用场景 | 服务启动时初始化 worker 消费组。 |

练习：创建邮件 worker 的消费者组；已存在时忽略错误。

```python
def exercise_create_email_group() -> bool:
    from redis.exceptions import ResponseError

    try:
        return r.xgroup_create("stream:email", "email-workers", id="0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" in str(exc):
            return False
        raise
```

消费者组只需要创建一次，不需要每次消费时创建。

### `xreadgroup()`：从消费者组读取消息

重要程度：⭐⭐⭐⭐

```python
messages = r.xreadgroup(
    groupname="order-workers",
    consumername="worker-1",
    streams={"stream:order": ">"},
    count=10,
    block=5000,
)
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 用消费者组身份读取消息。 |
| 重要参数 | `groupname`：组名；`consumername`：当前 worker 名称；`streams={key: ">"}`：读取未投递的新消息；`count=10`：最多取十条；`block=5000`：没消息时最多等五秒。 |
| 返回值 | 有消息时是“流名 + 消息列表”的嵌套 `list`；等待超时没有消息时通常是 `[]`。 |
| 使用场景 | worker 持续消费邮件、订单、报表任务。 |

练习：从邮件消费组读取最多五条任务。

```python
def exercise_xreadgroup_email(consumer_name: str) -> list:
    return r.xreadgroup(
        groupname="email-workers",
        consumername=consumer_name,
        streams={"stream:email": ">"},
        count=5,
        block=1000,
    )
```

### `xack()`：确认消息处理成功

重要程度：⭐⭐⭐⭐

```python
ack_count = r.xack("stream:order", "order-workers", "1750000000000-0")
print(ack_count)  # 1
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 标记消息已经被这个消费者组成功处理。 |
| 参数 | Stream key、组名、一个或多个消息 ID。 |
| 返回值 | 成功确认的消息数量，类型是 `int`。 |
| 使用场景 | 邮件/订单/报表任务的业务逻辑执行成功后。 |

练习：确认一条邮件任务。

```python
def exercise_xack_email(message_id: str) -> bool:
    ack_count = r.xack("stream:email", "email-workers", message_id)
    return ack_count == 1
```

### 本章大练习：消费邮件任务

目标：从消费者组拿任务，成功后确认；失败时不确认，让它留在待处理列表中等待重试机制处理。

```python
from collections.abc import Callable


def practice_consume_email(
    consumer_name: str,
    send_email: Callable[[dict[str, str]], None],
) -> int:
    messages = r.xreadgroup(
        groupname="email-workers",
        consumername=consumer_name,
        streams={"stream:email": ">"},
        count=10,
        block=1000,
    )

    success_count = 0
    for stream_name, entries in messages:
        for message_id, data in entries:
            try:
                send_email(data)
            except Exception:
                continue
            r.xack(stream_name, "email-workers", message_id)
            success_count += 1

    return success_count
```

业务函数必须支持幂等：同一个订单或邮件消息重复执行时，不能产生两次扣款、两封相同的关键通知。

---

## 10. 维护和排查 API

### `type()`：查看 key 的数据类型

重要程度：⭐⭐⭐

```python
data_type = r.type("cart:10086")
print(data_type)  # 'hash'
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 查看一个 key 的 Redis 数据类型。 |
| 返回值 | `'string'`、`'hash'`、`'set'`、`'zset'`、`'list'` 等；key 不存在时为 `'none'`。 |
| 使用场景 | 排查“为什么 `hget()` 报类型错误”。 |

练习：判断一个 key 是否是 Hash。

```python
def exercise_type_is_hash(key: str) -> bool:
    return r.type(key) == "hash"
```

### `scan_iter()`：按前缀逐步扫描 key

重要程度：⭐⭐⭐⭐

```python
for key in r.scan_iter(match="product:detail:*", count=100):
    print(key)
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 分批扫描 key，避免一次把所有 key 读出来。 |
| 参数 | `match` 是匹配模式；`count` 是每批建议返回数量，不保证精确。 |
| 返回值 | 一个可迭代对象，循环时每次得到一个 key 字符串。 |
| 使用场景 | 按业务前缀排查缓存、写迁移或清理脚本。 |

线上不要使用 `r.keys("*")`。数据量大时，它可能影响 Redis 正常响应。

练习：找到最多十个商品详情缓存 key。

```python
def exercise_scan_product_keys() -> list[str]:
    keys: list[str] = []
    for key in r.scan_iter(match="product:detail:*", count=100):
        keys.append(key)
        if len(keys) == 10:
            break
    return keys
```

### `unlink()`：异步删除大 key

重要程度：⭐⭐⭐

```python
deleted_count = r.unlink("large:cache:key")
print(deleted_count)  # 1 或 0
```

| 项目 | 说明 |
| --- | --- |
| 作用 | 先逻辑删除 key，真正的内存回收在后台执行。 |
| 参数 | 一个或多个 key。 |
| 返回值 | 删除成功的 key 数量，类型是 `int`。 |
| 使用场景 | 删除较大的缓存/集合，降低删除瞬间的卡顿风险。 |

小 key 直接用 `delete()` 就可以。

练习：异步清理一个大号报表缓存。

```python
def exercise_unlink_report_cache(report_id: int) -> bool:
    deleted_count = r.unlink(f"report:cache:{report_id}")
    return deleted_count == 1
```

### 本章大练习：按前缀清理缓存

目标：找出某类商品缓存，异步删除，最多处理指定数量。这个函数适合管理脚本，不要直接放进普通接口。

```python
def practice_clear_product_cache(limit: int = 100) -> int:
    keys: list[str] = []
    for key in r.scan_iter(match="product:detail:*", count=100):
        keys.append(key)
        if len(keys) >= limit:
            break

    return r.unlink(*keys) if keys else 0
```

---

## 11. FastAPI 异步版本

FastAPI 的接口如果是 `async def`，使用 `redis.asyncio`。API 名称、参数、返回值和上面基本一样，只是每次调用前加 `await`。

```python
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = Redis.from_url(
        os.environ["REDIS_URL"],
        decode_responses=True,
    )
    yield
    await app.state.redis.aclose()


app = FastAPI(lifespan=lifespan)
```

### `await redis.get()` / `await redis.set()`

重要程度：⭐⭐⭐⭐⭐

```python
cached = await app.state.redis.get("product:detail:42")
await app.state.redis.set("product:detail:42", '{"id": 42}', ex=1800)
```

返回值和同步版本相同：`get()` 是 `str | None`，`set()` 成功时是 `True`。

练习：写一个异步函数，读取缓存，没有时写入传入的内容。

```python
async def exercise_async_cache(key: str, value: str) -> str:
    cached = await app.state.redis.get(key)
    if cached is not None:
        return cached

    await app.state.redis.set(key, value, ex=60)
    return value
```

### 本章大练习：异步商品缓存接口

```python
import json

from fastapi import HTTPException


@app.get("/products/{product_id}")
async def practice_async_get_product(product_id: int):
    key = f"product:detail:{product_id}"
    cached = await app.state.redis.get(key)
    if cached is not None:
        return json.loads(cached)

    # 这里替换为真实的异步数据库查询
    product = {"id": product_id, "name": "示例商品"}
    if product is None:
        raise HTTPException(status_code=404, detail="商品不存在")

    await app.state.redis.set(key, json.dumps(product), ex=30 * 60)
    return product
```

不要在 `async def` 接口里调用同步的 `redis_client.get()`。同步 I/O 会阻塞事件循环，影响同一进程中的其他请求。

---

## 推荐练习顺序

1. 先完成 String 的所有小练习，尤其是 `set`、`get`、`expire`、`ttl`、`delete`。
2. 写商品缓存大练习，理解缓存命中和 TTL。
3. 写 Hash 购物车和 Set 点赞。
4. 写 ZSet 排行榜和 List 最近浏览。
5. 练 Pipeline，再练 `eval()` 的验证码和锁。
6. 最后写 Stream 消费者和 FastAPI 异步缓存。

临时数据没有过期时间、缓存更新后没有删除旧缓存、重要消息没有做幂等，这三类问题最容易在项目中出事故。
