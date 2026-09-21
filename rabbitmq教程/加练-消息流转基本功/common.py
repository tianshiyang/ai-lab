import os
from pathlib import Path

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]


async def get_config(channel: aio_pika.abc.AbstractChannel) -> dict:
    """获取配置"""
    # 定义交换机
    e1 = await channel.declare_exchange("mq.exchange.e1", aio_pika.ExchangeType.TOPIC, durable=True)

    # 定义队列
    q1 = await channel.declare_queue("mq.queue.q1", durable=True)
    q2 = await channel.declare_queue("mq.queue.q2", durable=True)

    await q1.bind(e1, routing_key="task.run")
    await q2.bind(e1, routing_key="task.*")

    q3 = await channel.declare_queue(
        "mq.queue.q3",
        durable=True,
        arguments={
            "x-message-ttl": 5_000,
        },
    )
    q4 = await channel.declare_queue(
        "mq.queue.q4",
        durable=True,
        arguments={
            "x-message-ttl": 15_000,
        },
    )

    return {
        "e1": e1,
        "q1": q1,
        "q2": q2,
        "q3": q3,
        "q4": q4,
    }
