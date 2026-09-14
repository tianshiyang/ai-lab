"""alembic 运行环境：异步引擎 + 从 .env 读连接串 + 指向 models.py 的模型。"""

import asyncio
import os
import sys
from logging.config import fileConfig
from pathlib import Path

import dotenv
from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

# 项目根目录（ai-lab）插进 sys.path，才能 import 中文包
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from rabbitMQ学习.实战练习.models import Base  # noqa: E402

# 连接串不写进 alembic.ini（会带密码），从 .env 读
dotenv.load_dotenv(PROJECT_ROOT / ".env")

# alembic 配置对象，能拿到 alembic.ini 里的值
config = context.config

if (db_url := os.getenv("DATABASE_URL")) is None:
    raise RuntimeError(".env 里没找到 DATABASE_URL")
config.set_main_option("sqlalchemy.url", db_url)

# 日志按 alembic.ini 的 [loggers] 段配置
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# autogenerate 对比的目标：models.py 里所有模型
target_metadata = Base.metadata


# 这个库还跑着别的练习（clm_*、checkpoints 等），autogenerate 只管 notify_ 开头的表。
# 不过滤的话，库里多出来的表会被当成"该删的表"生成 drop 语句。
def include_object(obj, name, type_, reflected, compare_to):
    """只把 notify_ 开头的表纳入迁移管理。"""
    if type_ == "table":
        return name.startswith("notify_")
    return True


def run_migrations_offline() -> None:
    """离线模式：只生成 SQL 不连库。"""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        include_object=include_object,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """在给定连接上执行迁移。"""
    context.configure(
        connection=connection, target_metadata=target_metadata, include_object=include_object
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """在线模式：异步引擎连库执行迁移。"""
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """在线模式入口。"""
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
