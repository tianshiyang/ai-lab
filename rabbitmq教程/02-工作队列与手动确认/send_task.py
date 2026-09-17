import asyncio
import json
import os
from pathlib import Path
from typing import cast

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = cast(str, os.environ["RABBITMQ_URL"])

TASKS = [
    ("压缩商品图-1", 3),
    ("发送通知邮件-2", 1),
    ("压缩商品图-3", 3),
    ("发送通知邮件-4", 1),
    ("压缩商品图-5", 3),
    ("发送通知邮件-6", 1),
    ("压缩商品图-7", 3),
    ("发送通知邮件-8", 1),
    ("压缩商品图-9", 3),
    ("发送通知邮件-10", 1),
]


async def main():
    connection = await aio_pika.connect(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("ch2.tasks", durable=True)
        await queue.purge()  # 清掉上一次实验残留

        for name, seconds in TASKS:
            body = json.dumps({"name": name, "seconds": seconds}).encode("utf-8")
            await channel.default_exchange.publish(aio_pika.Message(body), routing_key=queue.name)
            print(f"任务已入队: {name}(要干 {seconds} 秒)")


if __name__ == "__main__":
    asyncio.run(main())
