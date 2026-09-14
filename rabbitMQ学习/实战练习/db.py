"""数据库连接配置：连接串、异步引擎、会话工厂。

只管"怎么连上库"；表结构在 models.py，两者分开。
建表/改表交给 alembic：uv run alembic upgrade head。
"""

import os
from pathlib import Path
from typing import cast

import dotenv
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

# 加载环境变量（跟 topology.py 用的是同一个 .env）
project_root = Path(__file__).resolve().parents[2]
dotenv.load_dotenv(project_root / ".env")

# 数据库连接串（asyncpg 驱动）
DATABASE_URL = cast(str, os.getenv("DATABASE_URL"))

# 连接池：默认池。消费者是 aio-pika，整个进程跑在一个常驻事件循环里，
# 池化连接绑定的循环从头到尾不变，可以放心复用。
# （早先"每条消息 asyncio.run 起一个短命循环"的桥接方案，才必须配 NullPool。）
engine = create_async_engine(DATABASE_URL)

# 异步会话工厂；expire_on_commit 让提交后对象还能读属性
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)
