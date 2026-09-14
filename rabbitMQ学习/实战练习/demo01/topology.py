import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import cast

import aio_pika
import dotenv

# 加载环境变量
project_root = Path(__file__).resolve().parents[3]
dotenv.load_dotenv(project_root / ".env")

RABBITMQ_URL = cast(str, os.getenv("RABBITMQ_URL"))


async def declare_topology(channel: aio_pika.Channel) -> None:
    """声明交换机、队列和绑定；重复声明参数一致，不报错。"""
    # 声明交换机
    exchange = await channel.declare_exchange(
        "ai_lab.notify.events", aio_pika.ExchangeType.TOPIC, durable=True
    )

    # 声明队列
    sms_queue = await channel.declare_queue("sms", durable=True)
    email_queue = await channel.declare_queue("email", durable=True)
    inapp_queue = await channel.declare_queue("inapp", durable=True)

    # 声明绑定
    await sms_queue.bind(exchange, routing_key="notify.*.sms")
    await email_queue.bind(exchange, routing_key="notify.*.email")
    await inapp_queue.bind(exchange, routing_key="notify.*.inapp")


@asynccontextmanager
async def connect() -> AsyncIterator[tuple[aio_pika.Connection, aio_pika.Channel]]:
    """建连接并声明拓扑，交出 (连接, 信道)；出块自动断开连接。"""
    connection_1 = await aio_pika.connect(RABBITMQ_URL)
    async with connection_1:
        channel_1 = await connection_1.channel()
        await declare_topology(channel_1)
        yield connection_1, channel_1


async def main() -> None:
    """声明完整拓扑后退出。"""
    async with connect():
        pass
    print("拓扑声明完成")


if __name__ == "__main__":
    asyncio.run(main())
