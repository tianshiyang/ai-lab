import asyncio
import os
from pathlib import Path

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]


async def main() -> None:
    """主函数"""
    connection = await aio_pika.connect(RABBITMQ_URL)

    async with connection:
        channel = await connection.channel()

        # 声明队列
        volatile = await channel.declare_queue("ch4.volatile", durable=False)
        persist = await channel.declare_queue("ch4.persist", durable=True)

        await volatile.purge()
        await persist.purge()

        await channel.default_exchange.publish(
            aio_pika.Message("我是普通消息,重启就没了".encode()), routing_key=volatile.name
        )
        await channel.default_exchange.publish(
            aio_pika.Message(
                "我是持久化消息,重启也还在".encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,  # 持久化队列里的消息
            ),
            routing_key=persist.name,
        )

        try:
            await channel.declare_queue("ch4.persist", durable=False)
        except Exception as e:
            print(f"  broker 拒绝: {type(e).__name__}: {e}")


if __name__ == "__main__":
    asyncio.run(main())
