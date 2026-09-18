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
        await exchange.publish(
            aio_pika.Message("我可能哪都到不了".encode()), routing_key="no.one.binds.this"
        )
        await exchange.delete()


if __name__ == "__main__":
    asyncio.run(main())
