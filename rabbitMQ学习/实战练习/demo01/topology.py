import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import cast

import aio_pika
import dotenv

from rabbitMQ学习.实战练习.data.data import mock_config

# 加载环境变量
project_root = Path(__file__).resolve().parents[3]
dotenv.load_dotenv(project_root / ".env")

RABBITMQ_URL = cast(str, os.getenv("RABBITMQ_URL"))


async def declare_topology(channel: aio_pika.Channel) -> None:
    """声明业务、重试、归档三套拓扑；重复声明参数一致，不报错。"""
    # 业务拓扑：topic 交换机按 notify.<业务>.<通道> 路由到三个通道队列
    exchange = await channel.declare_exchange(
        "ai_lab.notify.events", aio_pika.ExchangeType.TOPIC, durable=True
    )
    sms_queue = await channel.declare_queue(
        "sms", durable=True, arguments={"x-max-priority": 10}
    )
    email_queue = await channel.declare_queue("email", durable=True)
    inapp_queue = await channel.declare_queue("inapp", durable=True)
    await sms_queue.bind(exchange, routing_key="notify.*.sms")
    await email_queue.bind(exchange, routing_key="notify.*.email")
    await inapp_queue.bind(exchange, routing_key="notify.*.inapp")

    # 重试拓扑：每档一个 fanout 交换机 + 一个队列。消息躺满 TTL 就算死，死信转回业务交换机
    # 档位读 mock_config；改档位 = 改队列参数，得先删队列重建（队列参数不可变的老坑）
    for seconds in mock_config["retry"]["backoff_seconds"]:
        retry_exchange = await channel.declare_exchange(
            f"ai_lab.notify.retry.{seconds}s", aio_pika.ExchangeType.FANOUT, durable=True
        )
        retry_queue = await channel.declare_queue(
            f"ai_lab.notify.retry.{seconds}s",
            durable=True,
            arguments={
                "x-message-ttl": seconds * 1000,
                "x-dead-letter-exchange": "ai_lab.notify.events",
                # 故意不写 x-dead-letter-routing-key：死信转发沿用的是消息"进入本队列时"
                # 的 routing key。消费者转投时保留业务原 key（notify.<业务>.<通道>），
                # 消息死掉时就带着它回业务交换机，自动路由回本通道队列——
                # 所以 3 档建 3 个队列就够，不用 3 档 × 3 通道建 9 个
            },
        )
        # fanout 不看 key，进哪个队列由"发到哪个档位的交换机"决定；
        # 用 fanout 是为了转投时能原样保留业务 routing key 不被改掉
        await retry_queue.bind(retry_exchange)

    # 归档拓扑：direct 交换机按通道收留重试耗尽的消息；没有消费者，纯留证据
    failed_exchange = await channel.declare_exchange(
        "ai_lab.notify.failed", aio_pika.ExchangeType.DIRECT, durable=True
    )
    for channel_name in ("sms", "email", "inapp"):
        failed_queue = await channel.declare_queue(
            f"ai_lab.notify.{channel_name}.failed", durable=True
        )
        await failed_queue.bind(failed_exchange, routing_key=channel_name)


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
