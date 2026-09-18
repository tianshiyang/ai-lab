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
        strict = await connection.channel(on_return_raises=True)
        exchange = await strict.declare_exchange(
            "ch4.probe", aio_pika.ExchangeType.DIRECT, durable=True
        )
        try:
            await exchange.publish(
                aio_pika.Message(body="再发一次没人要的消息".encode()),
                routing_key="no.one.binds.this",
            )
        except aio_pika.exceptions.DeliveryError:
            print("  抛出 DeliveryError,生产端当场感知 → 记日志 / 告警 / 补偿")
        await exchange.delete()


if __name__ == "__main__":
    asyncio.run(main())
