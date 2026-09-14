"""数据库连接配置：连接串、异步引擎、会话工厂。

只管"怎么连上库"；表结构在 models.py，两者分开。
建表/改表交给 alembic：uv run alembic upgrade head。
"""

import os
from pathlib import Path
from typing import cast

import dotenv
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

# 加载环境变量（跟 topology.py 用的是同一个 .env）
project_root = Path(__file__).resolve().parents[2]
dotenv.load_dotenv(project_root / ".env")

# 数据库连接串（asyncpg 驱动）
DATABASE_URL = cast(str, os.getenv("DATABASE_URL"))

# NullPool：不用连接池。消费者是同步 pika，每条消息靠 asyncio.run 起一个短命事件循环，
# 池化的连接绑在旧循环上，新循环复用会报 "attached to a different loop"。
# 练习这点量现开现连无所谓；哪天消费者整体异步化，删掉 poolclass 这行就是默认池。
engine = create_async_engine(DATABASE_URL, poolclass=NullPool)

# 异步会话工厂；expire_on_commit 让提交后对象还能读属性
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)
