import asyncio
import os
from pathlib import Path

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]

EVENTS = [
    ("order.created", "订单 A1001 创建"),
    ("order.paid", "订单 A1001 支付成功"),
    ("order.cancelled", "订单 B2002 取消"),
    ("refund.created", "退款单 R3003 创建"),
]


async def drain(queue: aio_pika.Queue) -> list[str]:
    messages = []
    while True:
        message = await queue.get(no_ack=True, fail=False, timeout=2)
        if message is None:
            return messages
        messages.append(message.body.decode("utf-8"))


async def main() -> None:
    connection = await aio_pika.connect(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()

        exchange = await channel.declare_exchange(
            "ch3.orders", aio_pika.ExchangeType.TOPIC, durable=True
        )

        # 只盯新订单
        watch_created = await channel.declare_queue("ch3.watch.created", durable=True)
        # 订单的所有动作
        watch_orders = await channel.declare_queue("ch3.watch.orders", durable=True)
        # 全站所有事件
        watch_all = await channel.declare_queue("ch3.watch.all", durable=True)

        for q in (watch_created, watch_orders, watch_all):
            await q.purge()

        await watch_created.bind(exchange, routing_key="order.created")
        await watch_orders.bind(exchange, routing_key="order.*")
        await watch_all.bind(exchange, routing_key="#")

        for routing_key, text in EVENTS:
            await exchange.publish(
                aio_pika.Message(body=text.encode("utf-8")), routing_key=routing_key
            )
            print(f"已发送 → [{routing_key}]: {text}")

        for name, queue in [
            ("只盯下单 order.created", watch_created),
            ("订单动作 order.*", watch_orders),
            ("全站事件 #", watch_all),
        ]:
            received = await drain(queue)
            print(f"\n[{name}] 收到 {len(received)} 条:")
            for text in received:
                print(f"    {text}")

        await watch_created.delete()
        await watch_orders.delete()
        await watch_all.delete()
        await exchange.delete()

if __name__ == "__main__":
    asyncio.run(main())
