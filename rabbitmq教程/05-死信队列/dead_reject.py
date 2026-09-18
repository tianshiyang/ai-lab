import asyncio
import os
from pathlib import Path

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]


def decode(value: object) -> object:
    return value.decode("utf-8") if isinstance(value, bytes) else value


async def main() -> None:
    """主函数"""
    connection = await aio_pika.connect_robust(RABBITMQ_URL)

    async with connection:
        channel = await connection.channel()

        # 定义死信队列和死信交换机
        dlx = await channel.declare_exchange("ch5.dlx", aio_pika.ExchangeType.FANOUT, durable=True)
        dead = await channel.declare_queue("ch5.dead", durable=True)
        await dead.bind(dlx)
        await dead.purge()

        # 定义业务队列
        orders = await channel.declare_queue(
            "ch5.orders", durable=True, arguments={"x-dead-letter-exchange": "ch5.dlx"}
        )
        await orders.purge()

        for body in ["正常订单-A", "正常订单-B", "毒订单-X(内容非法,谁来谁失败)"]:
            # 发送消息
            await channel.default_exchange.publish(
                aio_pika.Message(body.encode()), routing_key=orders.name
            )

        for _ in range(3):
            message = await orders.get()
            body = message.body.decode()
            if "毒" in body:
                print(f"  处理失败,处死: {body}")
                await message.nack(requeue=False)
            else:
                print(f"  处理成功: {body}")
                await message.ack()

        await asyncio.sleep(0.5)

        while True:
            message = await dead.get(no_ack=True, fail=False, timeout=2)
            if message is None:
                break
            print(f"  收到死信: {message.body.decode('utf-8')}")
            for death in message.headers.get("x-death", []):
                print(
                    f"  档案：原因={decode(death.get('reason'))}"
                    f"原队列={decode(death.get('queue'))}"
                    f"次数={decode(death.get('count'))}"
                    f"时间={decode(death.get('time'))}"
                )

        await orders.delete()
        await dead.delete()
        await dlx.delete()


if __name__ == "__main__":
    asyncio.run(main())
