# 后端锁与并发控制

这不是“背锁 API”的教程，而是学习：**多个人同时改同一份事实时，谁来保证最后的数据仍然正确。**

整套实验只使用 `.env` 中同一个 PostgreSQL 数据库，以及同一套数据模型；Redis 只在第 5 讲作为跨实例协调器出现。不要给每一讲新建一批 `lock01_*`、`lock02_*` 表。

## 统一实验模型

“限量课程报名”贯穿所有案例，按真实电商的经典四表建模：**商品 → 商品SKU → 订单 → 订单明细**。

```text
lock_lab_product(商品)          lock_lab_product_sku(商品SKU)
───────────────────────        ─────────────────────────────
id = 1                         id = 1  early-bird  stock=3  version=0
title = "python-async-..."     id = 2  standard     stock=5
sold_count = 0                 product_id → product.id
summary_done = False           price(分)

lock_lab_order(订单头)          lock_lab_order_item(订单明细)
───────────────────────        ─────────────────────────────
id                             id
user_id                        order_id → order.id
request_id  UNIQUE(幂等键)     sku_id → product_sku.id
total_amount(分)               quantity / unit_price(下单时快照)
status: PENDING / PROCESSING / DONE
```

关系一句话：一个商品多个 SKU（库存挂在 SKU 上）；一次下单生成一张订单（头）加一至多行明细；明细通过 `sku_id` 指向具体规格，并快照下单时的单价。`product` 演示多表写入和汇总标记；`product_sku` 演示库存、行锁和版本号；`order` 演示幂等与任务领取；`order_item` 演示同事务多行写入。

共用代码在 `锁教程/model.py`，每讲 demo 直接 `from 锁教程.model import ...`。每个并发协程必须自己创建 `AsyncSession`，不要把同一个 Session 同时交给多个 Task。`DATABASE_URL` 沿用仓库根目录 `.env`。

## 怎么跑一个实验

每一讲的实验共用同一个执行骨架：`reset_lab()` 回到已知初始状态，`asyncio.gather` 把多个协程同时放进事件循环模拟并发请求，入口处 `asyncio.run` 启动整个程序。

```python
import asyncio

from 锁教程.model import Session, Sku, reset_lab


async def main():
    await reset_lab()
    results = await asyncio.gather(实验函数("请求A"), 实验函数("请求B"))
    print(results)


if __name__ == "__main__":
    asyncio.run(main())
```

要点：每个协程内部自己创建 `AsyncSession`（不要共享，见上文）；并发中抛出的异常要么在协程内接住、要么给 `gather` 传 `return_exceptions=True`，否则一个协程报错会中断整个 `gather`；`asyncio.run` 在整个程序里只调用一次，位置固定在 `__main__` 入口。在仓库根目录运行：`PYTHONPATH=. uv run python 锁教程/02-悲观锁/demo.py`——直接 `python` 跑单个文件时，搜索路径里只有文件所在目录、没有仓库根，`from 锁教程.model import ...` 会报 `ModuleNotFoundError`；`PYTHONPATH=.` 把仓库根补进去。用 PyCharm 右键 Run 不需要这步（运行配置默认已把项目根加入搜索路径）。

## 先背这条选型顺序

```text
能用一条条件 UPDATE 解决 → 不上锁
能用唯一约束保证业务身份 → 不靠应用层判断
读-改-写且冲突少 → 乐观锁（version / 条件更新 + 重试）
读-改-写且冲突密、必须串行 → 悲观锁（FOR UPDATE，短事务）
多 worker 领待办 → SKIP LOCKED
跨实例且数据库管不到的临界区 → PostgreSQL advisory lock 或 Redis 租约锁
```

## 六讲路线

| 讲 | 主题 | 你会解决什么 | 优先级 |
| --- | --- | --- | --- |
| 01 | [并发事故与原子写](01-丢失更新/讲义.md) | 丢失更新、条件 UPDATE、防超卖 | ⭐⭐⭐ |
| 02 | [悲观锁与任务领取](02-悲观锁/讲义.md) | `FOR UPDATE`、`NOWAIT`、`SKIP LOCKED`、多表锁顺序、死锁 | ⭐⭐⭐ |
| 03 | [乐观锁与重试](03-乐观锁/讲义.md) | version CAS、冲突、重试边界 | ⭐⭐⭐ |
| 04 | [唯一约束与幂等](04-唯一约束/讲义.md) | 重复请求、`ON CONFLICT`、事务原子性 | ⭐⭐⭐ |
| 05 | [跨实例协调](05-redis锁/讲义.md) | advisory lock、Redis 锁、租约与 fencing | ⭐⭐ |
| 06 | [选型与综合实战](06-选型实战/讲义.md) | 一次报名如何组合这些工具 | ⭐⭐⭐ |

## 每种工具到底保证什么

| 工具 | 它保证的事 | 它不保证的事 |
| --- | --- | --- |
| 条件 `UPDATE` | 单条写入的原子性、库存不小于 0 | 多步业务流程 |
| 唯一约束 | 同一业务身份最多一行 | 库存、外部副作用 |
| 乐观锁 | 不会用旧版本覆盖新版本 | 自动重试、低冲突性能 |
| `FOR UPDATE` | 事务内同一行的串行修改 | 跨事务、跨数据库互斥 |
| `SKIP LOCKED` | 多 worker 不重复领取同一待办 | 通用一致性查询 |
| advisory lock | 同一 PostgreSQL 实例内的命名互斥 | Redis / 其他数据库实例 |
| Redis 锁 | 跨实例的“租约式”互斥 | 租约过期后的绝对排他性 |

注意：乐观锁和唯一约束严格说都不是“锁”；它们是通常更应该优先使用的并发控制手段。

## 阅读每一讲时只回答四个问题

1. 保护的事实是什么：库存、订单身份，还是任务归属？
2. 谁在竞争：同一数据库事务、多个 worker，还是多个服务实例？
3. 竞争输了是什么结果：排队、立即失败、重试，还是跳过？
4. 崩溃或超时后谁收尾：数据库事务、唯一约束，还是租约到期？

PostgreSQL 的行锁会持续到事务结束；`SKIP LOCKED` 的设计目标是队列式消费者，不是拿来做普通一致性查询。[PostgreSQL 显式锁文档](https://www.postgresql.org/docs/current/explicit-locking.html) [PostgreSQL SELECT 锁定子句](https://www.postgresql.org/docs/current/sql-select.html)
