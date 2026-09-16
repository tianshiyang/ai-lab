import asyncio
import os
from pathlib import Path
from typing import cast

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = cast(str, os.getenv("RABBITMQ_URL"))


async def main() -> None:
    """主函数"""
    connection = await aio_pika.connect(RABBITMQ_URL)
    async with connection:
        # 定义通道
        channel = await connection.channel()

        # 定义队列
        queue = await channel.declare_queue("ch1.hello", durable=True)

        messages = [
            "你好,RabbitMQ",
            "第二条:一张外卖订单",
            "第三条:一条短信验证码",
        ]
        for message in messages:
            await channel.default_exchange.publish(
                aio_pika.Message(body=message.encode()), routing_key=queue.name
            )
            print(f"已发送：{message}")


if __name__ == "__main__":
    asyncio.run(main())
