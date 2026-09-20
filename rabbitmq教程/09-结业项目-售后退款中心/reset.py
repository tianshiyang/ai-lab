"""结业项目·清场:删光本项目的队列、交换机和表,回到初始状态,方便反复演练。

uv run python "rabbitmq教程/09-结业项目-售后退款中心/reset.py"
"""

import asyncio

import aio_pika
from sqlalchemy import text

import db
from common import RABBITMQ_URL, get_config


async def main() -> None:
    connection = await aio_pika.connect(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        config = await get_config(channel)  # 按原参数声明一遍,才拿得到可删除的引用

        for key in (
            "audit_queue",
            "finance_queue",
            "retry_5s_queue",
            "retry_15s_queue",
            "notify_queue",
            "dead_queue",
        ):
            queue: aio_pika.abc.AbstractQueue = config[key]
            await queue.delete(if_unused=False, if_empty=False)
            print(f"已删除队列 {queue.name}")
        for key in ("events", "dlx"):
            exchange: aio_pika.abc.AbstractExchange = config[key]
            await exchange.delete()
            print(f"已删除交换机 {exchange.name}")

    async with db.engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS refunds"))
    await db.engine.dispose()
    print("已删除表 refunds,重置完毕,可以重新开一局")


asyncio.run(main())
