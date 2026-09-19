import asyncio
import os
from pathlib import Path

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]


async def main():
    connection = await aio_pika.connect(RABBITMQ_URL)
    async with connection:
        channel1 = await connection.channel()

        queue = await channel1.declare_queue(
            "ch7.priority", durable=True, arguments={"x-max-priority": 10}
        )

        await queue.purge()

        for priority, text in [
            (None, "普通工单-1"),
            (None, "普通工单-2"),
            (9, "加急工单-3(客户在催)"),
            (None, "普通工单-4"),
            (9, "加急工单-5(老板在催)"),
        ]:
            await channel1.default_exchange.publish(
                aio_pika.Message(text.encode("utf-8"), priority=priority),
                routing_key=queue.name,
            )

        while True:
            message = await queue.get(no_ack=True, fail=False)
            if message is None:
                break
            print(f"  {message.body.decode('utf-8')}")


if __name__ == "__main__":
    asyncio.run(main())
