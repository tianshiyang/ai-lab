import asyncio
import os
from pathlib import Path
from typing import cast

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]


async def main():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("ch7.rpc", durable=True)

        async for message in queue.iterator():
            n = int(message.body.decode("utf-8"))
            print(f"  收到请求: {n}  (correlation_id={message.correlation_id})")
            await asyncio.sleep(1)  # 假装耗时计算

            # 结果发往请求方指定的reply_to, correlation_id原样带回
            await channel.default_exchange.publish(
                aio_pika.Message(str(n * n).encode("utf-8"), correlation_id=message.correlation_id),
                routing_key=cast(str, message.reply_to),
            )
            await message.ack()


if __name__ == "__main__":
    asyncio.run(main())