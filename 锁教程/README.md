# 后端锁与并发控制

这不是“背锁 API”的教程，而是学习：**多个人同时改同一份事实时，谁来保证最后的数据仍然正确。**

整套实验只使用 `.env` 中同一个 PostgreSQL 数据库，以及同一套数据模型；Redis 只在第 5 讲作为跨实例协调器出现。不要给每一讲新建一批 `lock01_*`、`lock02_*` 表。

## 统一实验模型

“限量课程报名”贯穿所有案例：一门课程只有有限名额，多人同时报名；同一请求可能重放；报名成功后还要被后台 worker 处理。

```text
lock_lab_product                 lock_lab_order
────────────────────             ─────────────────────────────
id = 1                           id
sku = "python-async"             product_id → product.id
available = 3                    user_id
version = 0                      request_id  UNIQUE
                                 status: PENDING / PROCESSING / DONE
```

`product` 负责演示库存、行锁和版本号；`order` 负责演示幂等与任务领取。所有数据库案例都围绕这两张表，不切换数据库，也不切换业务背景。

建议你先在自己的练习文件中准备这套模型；每个并发协程必须自己创建 `AsyncSession`，不要把同一个 Session 同时交给多个 Task。下面这段是后续所有代码默认已经具备的“实验基座”；`DATABASE_URL` 沿用仓库根目录 `.env`。

```python
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import ForeignKey, String, delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

load_dotenv(Path(__file__).resolve().parents[2] / ".env")


class Base(DeclarativeBase):
    pass


class Product(Base):
    __tablename__ = "lock_lab_product"

    id: Mapped[int] = mapped_column(primary_key=True)
    sku: Mapped[str] = mapped_column(String(50), unique=True)
    available: Mapped[int]
    version: Mapped[int] = mapped_column(default=0)


class Order(Base):
    __tablename__ = "lock_lab_order"

    id: Mapped[int] = mapped_column(primary_key=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("lock_lab_product.id"))
    user_id: Mapped[str] = mapped_column(String(50))
    request_id: Mapped[str] = mapped_column(String(80), unique=True)
    status: Mapped[str] = mapped_column(String(20), default="PENDING")


engine = create_async_engine(os.environ["DATABASE_URL"])
Session = async_sessionmaker(engine, expire_on_commit=False)


async def reset_lab() -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    async with Session.begin() as session:
        await session.execute(delete(Order))
        await session.execute(delete(Product))
        session.add(Product(id=1, sku="python-async", available=3, version=0))
```

每次开始新实验前调用一次 `await reset_lab()`。如果你把练习文件放在别的位置，只需把 `load_dotenv(...)` 的路径改为能指向仓库根目录 `.env` 的位置。

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
| 02 | [悲观锁与任务领取](02-悲观锁/讲义.md) | `FOR UPDATE`、`NOWAIT`、`SKIP LOCKED`、死锁 | ⭐⭐⭐ |
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
