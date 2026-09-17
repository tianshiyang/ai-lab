import asyncio
import os
from pathlib import Path

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]

# (路由键, 消息内容):一条消息该走哪个通道,由路由键说了算
NOTIFICATIONS = [
    ("sms", "验证码 8848,五分钟内有效"),
    ("email", "您本月的账单已出,附件为明细"),
    ("sms", "您的快递已到丰巢,取件码 3306"),
    ("push", "这条走 push 通道——但没人绑定 push,它会被直接丢弃"),
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
            "ch3.notify", aio_pika.ExchangeType.DIRECT, durable=True
        )

        sms = await channel.declare_queue("ch3.sms", durable=True)
        email = await channel.declare_queue("ch3.email", durable=True)

        await sms.purge()
        await email.purge()
        await sms.bind(exchange, routing_key="sms")  # ② 各绑各的键:短信队列只认 sms
        await email.bind(exchange, routing_key="email")

        for routing_key, text in NOTIFICATIONS:
            await exchange.publish(
                aio_pika.Message(body=text.encode("utf-8")), routing_key=routing_key
            )
            print(f"已发送 → 路由键[{routing_key}]: {text}")

        for name, queue in [("短信队列", sms), ("邮件队列", email)]:
            received = await drain(queue)
            print(f"\n[{name}] 收到 {len(received)} 条:")
            for text in received:
                print(f"    {text}")

        print("\n走 push 的那条,两边都没收到——路由不到任何队列的消息,direct 交换机直接丢弃")
        print("(怎么感知「路由失败」?第 4 讲的 mandatory 讲这个)")

        await sms.delete()
        await email.delete()
        await exchange.delete()


if __name__ == "__main__":
    asyncio.run(main())
