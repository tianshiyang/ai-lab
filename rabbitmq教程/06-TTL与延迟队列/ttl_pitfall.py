import asyncio
import os
import time
from datetime import timedelta
from pathlib import Path

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]


async def main() -> None:
    connection = await aio_pika.connect(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()

        dlx = await channel.declare_exchange("ch6.dlx", aio_pika.ExchangeType.DIRECT, durable=True)

        trap = await channel.declare_queue(
            "ch6.trap", durable=True, arguments={"x-dead-letter-exchange": "ch6.dlx"}
        )

        dead = await channel.declare_queue("ch6.trap.dead", durable=True)

        await dead.bind(dlx, routing_key="ch6.trap")
        await trap.purge()
        await dead.purge()

        await channel.default_exchange.publish(
            aio_pika.Message("A:15秒过期".encode(), expiration=timedelta(seconds=15)),
            routing_key=trap.name,
        )
        await channel.default_exchange.publish(
            aio_pika.Message("B:5 秒后过期".encode(), expiration=timedelta(seconds=5)),
            routing_key=trap.name,
        )

        start = time.monotonic()
        print("t=0 秒:A(15 秒过期)、B(5 秒过期)先后入队")
        print("按直觉 B 应在第 5 秒死。盯着死信队列:\n")

        arrivals = 0
        while arrivals < 2:
            message = await dead.get(no_ack=True, fail=False)
            if message is not None:
                arrivals += 1
                print(
                    f"t={time.monotonic() - start:4.1f} 秒:死信到达 → "
                    f"{message.body.decode('utf-8')}"
                )
            await asyncio.sleep(0.1)

        print("\n过期检查只看队头,A 不走 B 就出不来 —— 队头阻塞。")
        print("结论:多种延迟时长要按档位建队列。")

        await trap.delete()
        await dead.delete()
        await dlx.delete()

if __name__ == "__main__":
    asyncio.run(main())
