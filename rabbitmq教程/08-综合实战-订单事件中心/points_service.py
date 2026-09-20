import asyncio
import json
from datetime import datetime

import aio_pika
import db
from common import RABBITMQ_URL, declare_topology


def now() -> str:
    return f"[{datetime.now():%H:%M:%S}]"


async def main():
    connection = await aio_pika.connect(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        topo = await declare_topology(channel)
        points: aio_pika.Queue = topo["points"]

        print(f"{now()}积分服务已启动")
        async for message in points.iterator():
            event = json.loads(message.body)
            order_id = event["order_id"]
            order = db.get(order_id)

            if await db.grant_points(order_id):
                print(f"{now()} 积分:{order_id} 发放 {order.amount} 分")
            else:
                print(f"{now()} 积分:{order_id} 已发过,跳过(表里标记拦下,重启也拦得住)")
            await message.ack()


if __name__ == "__main__":
    asyncio.run(main())
