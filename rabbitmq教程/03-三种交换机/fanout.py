import asyncio
import os
from pathlib import Path

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]

LOGS = [
    "[WARN] 库存服务响应变慢",
    "[INFO] 用户 1001 登录成功",
    "[ERROR] 支付回调第三次重试",
]


async def drain(queue: aio_pika.Queue) -> list[str]:
    """把队列里现成的消息全部取出来(取走即确认）"""
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
            "ch3.logs", aio_pika.ExchangeType.FANOUT, durable=True
        )

        # 两个订阅者
        archive = await channel.declare_queue("ch3.log.archive", durable=True)
        console = await channel.declare_queue("ch3.log.console", durable=True)

        await archive.purge()
        await console.purge()

        # 绑定交换机
        await archive.bind(exchange, routing_key="")
        await console.bind(exchange, routing_key="")

        for log in LOGS:
            await exchange.publish(aio_pika.Message(log.encode("utf-8")), routing_key="")
            print(f"已广播: {log}")

        for name, queue in [("归档库", archive), ("控制台", console)]:
            received = await drain(queue)
            print(f"\n[{name}] 收到 {len(received)} 条(和发出的一模一样):")
            for text in received:
                print(f"    {text}")

        await archive.delete()
        await console.delete()
        await exchange.delete()


if __name__ == "__main__":
    asyncio.run(main())
