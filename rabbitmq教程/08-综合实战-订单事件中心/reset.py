"""第 8 讲·重置:清空/删除本讲的队列、交换机和订单表,回到初始状态,方便反复演练。

uv run python "rabbitmq教程/08-综合实战-订单事件中心/reset.py"
"""

import asyncio

import aio_pika
import db
from common import RABBITMQ_URL, declare_topology


async def main() -> None:
    connection = await aio_pika.connect(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        topo = await declare_topology(channel)  # 按原参数声明一遍,才拿得到可删除的引用

        for key in ("sms", "points", "pay_wait", "timeout", "dead"):
            queue: aio_pika.Queue = topo[key]  # type: ignore[assignment]
            await queue.delete(if_unused=False, if_empty=False)
            print(f"已删除队列 {queue.name}")
        for key in ("events", "dlx"):
            exchange: aio_pika.Exchange = topo[key]  # type: ignore[assignment]
            await exchange.delete()
            print(f"已删除交换机 {exchange.name}")

    if db.DB_FILE.exists():
        db.DB_FILE.unlink()
        print("已删除订单表 orders.json")
    print("重置完毕,可以重新开一局")


asyncio.run(main())
