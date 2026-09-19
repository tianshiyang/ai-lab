import asyncio
import os
import uuid
from pathlib import Path
from typing import cast

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]

NUMBERS = [7, 12]
PENDING = dict[str, asyncio.Future[str]] = {}


async def ask_squere(channel: aio_pika.Channel, callback_queue: aio_pika.Queue, n: int) -> str:
    correlation_id = str(uuid.uuid4())
    future = PENDING[correlation_id] = asyncio.get_running_loop().create_future()

    await channel.default_exchange.publish(
        aio_pika.Message(
            str(n).encode(), correlation_id=correlation_id, replay_to=callback_queue.name
        ),
        routing_key="ch7.rpc",
    )
    print(f"已发出请求: {n} 的平方?")
    answer = await asyncio.wait_for(future, timeout=10)
    print(f"回信: {answer}")
    return answer


async def main() -> None:
    connection = await aio_pika.connect(RABBITMQ_URL)

    async with connection:
        channel = await connection.channel()

        callback_queue = await channel.declare_queue(exclusive=True)

        async def on_reply(message: aio_pika.IncomingMessage) -> None:
            future = PENDING.pop(message.correlation_id, None)
            if future is not None and not future.done():
                future.set_result(message.body.decode())
            await message.ack()

        await callback_queue.consume(on_reply)
        for n in NUMBERS:
            await ask_squere(channel, callback_queue, n)


if __name__ == "__main__":
    asyncio.run(main())