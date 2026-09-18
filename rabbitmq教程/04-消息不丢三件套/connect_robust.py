import asyncio
import json
import os
from pathlib import Path

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]


async def main() -> None:
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()

        exchange = await channel.declare_exchange(
            "ch4.relable", aio_pika.ExchangeType.DIRECT, durable=True
        )

        queue = await channel.declare_queue("ch4.reliable.q", durable=True)
        await queue.bind(exchange, routing_key="order.paid")
        await queue.purge()

        body = json.dumps({"order_id": "R5001", "amount": 42}).encode("utf-8")
        await exchange.publish(
            aio_pika.Message(body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key="order.paid",
        )

        async for message in queue.iterator():
            order = json.loads(message.body)
            print(f"消费段③:收到 {order},处理业务……")
            await message.ack()
            break

        await queue.delete(if_unused=False)
        await exchange.delete()


if __name__ == "__main__":
    asyncio.run(main())
