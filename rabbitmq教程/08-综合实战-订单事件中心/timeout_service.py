import asyncio
import json
from datetime import datetime

import aio_pika
import db
from common import RABBITMQ_URL, declare_topology


def now() -> str:
    return f"[{datetime.now():%H:%M:%S}]"


async def main():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)

        topo = await declare_topology(channel)
        timeout_queue: aio_pika.Queue = topo["timeout"]

        print(f"{now()} 超时服务已启动")

        async for message in timeout_queue.iterator():
            event = json.loads(message.body)
            order_id = event["order_id"]
            order = await db.get(order_id)
            status = order.status if order else "不存在"

            if status == "已付款":
                print(f"{now()} 超时:{order_id} 到点但已付款,不处理")
            elif status == "已取消":
                print(f"{now()} 超时:{order_id} 已取消过,跳过(幂等)")
            else:
                await db.cancel(order_id, "超时未付款")
                print(f"{now()} 超时:{order_id} 15 秒未付款 → 取消,释放库存")
            await message.ack()

    if __name__ == "__main__":
        asyncio.run(main())
