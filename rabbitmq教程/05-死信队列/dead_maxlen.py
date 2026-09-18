import asyncio
import os
from pathlib import Path

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]


def decode(value: object) -> object:
    """x-death 字段偶尔是 bytes,统一转成可读值"""
    return value.decode("utf-8") if isinstance(value, bytes) else value


async def main() -> None:
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()

        dlx = await channel.declare_exchange("ch5.dlx", aio_pika.ExchangeType.FANOUT, durable=True)
        dead = await channel.declare_queue("ch5.dead", durable=True)
        await dead.bind(dlx)
        await dead.bind(dlx)

        vip = await channel.declare_queue(
            "ch5.vip",
            durable=True,
            arguments={"x-max-length": 2, "x-dead-letter-exchange": "ch5.dlx"},
        )
        await vip.purge()

        for i in range(1, 6):
            await channel.default_exchange.publish(
                aio_pika.Message(f"VIP消息-{i}".encode()), routing_key=vip.name
            )

        await asyncio.sleep(0.4)
        remain = []
        while True:
            message = await vip.get(no_ack=True, fail=False, timeout=2)
            if message is None:
                break
            remain.append(message.body.decode("utf-8"))
        print(f"  队列里剩下: {remain}")

        while True:
            message = await dead.get(no_ack=True, fail=False, timeout=2)
            if message is None:
                break
            print(f"  收到死信: {message.body.decode('utf-8')}")
            for death in message.headers.get("x-death", []):
                print(
                    f"    档案: 原因={decode(death.get('reason'))}  "
                    f"原队列={decode(death.get('queue'))}  "
                    f"次数={decode(death.get('count'))}"
                )

        await vip.delete()
        await dead.delete()
        await dlx.delete()


if __name__ == "__main__":
    asyncio.run(main())
