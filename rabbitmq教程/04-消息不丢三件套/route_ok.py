import asyncio
import os
from pathlib import Path

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]


async def main() -> None:
    connection = await aio_pika.connect(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(
            "ch4.probe", aio_pika.ExchangeType.DIRECT, durable=True
        )
        queue = await channel.declare_queue("ch4.probe", durable=True)

        confirmation = await exchange.publish(
            aio_pika.Message(
                "这条能正常路由".encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            ),
            routing_key="normal.key",
        )

        print("正常投递:")
        print(f"  返回值类型 {type(confirmation).__name__} ← broker 的确认帧")

        await queue.purge()  # delete 默认拒绝删非空队列,先清空再删
        await queue.delete()
        await exchange.delete()


if __name__ == "__main__":
    loop = asyncio.run(main())
