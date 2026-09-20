import os
from pathlib import Path

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]

EXCHANGE = "shop.events"  # 业务事件交换机(topic)
DLX = "shop.dlx"  # 死信交换机(direct),两个出口:dead / timeout

SMS_QUEUE = "shop.sms.queue"  # 短信队列,绑 order.paid;失败死信走 dead
POINTS_QUEUE = "shop.points.queue"  # 积分队列,绑 order.paid

PAY_WAIT = "shop.pay.wait"  # 付款等待队列:15 秒 TTL,到期死信走 timeout

DEAD_QUEUE = "shop.dead.queue"  # 死信池,人工排查入口
TIMEOUT_QUEUE = "shop.timeout.queue"  # 超时处理队列


async def declare_topology(channel: aio_pika.Channel) -> dict[str, object]:
    """声明本讲全部交换机、队列和绑定,返回各对象供调用方使用"""
    events = await channel.declare_exchange(EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
    dlx = await channel.declare_exchange(DLX, aio_pika.ExchangeType.DIRECT, durable=True)

    sms = await channel.declare_queue(
        SMS_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX,
            "x-dead-letter-routing-key": "dead",  # 重试耗尽的死信走 dead 出口
        },
    )
    await sms.bind(events, routing_key="order.paid")

    points = await channel.declare_queue(POINTS_QUEUE, durable=True)
    await points.bind(events, routing_key="order.paid")

    pay_wait = await channel.declare_queue(
        PAY_WAIT,
        durable=True,
        arguments={
            "x-message-ttl": 15_000,  # 躺 15 秒
            "x-dead-letter-exchange": DLX,
            "x-dead-letter-routing-key": "timeout",
        },
    )

    timeout_queue = await channel.declare_queue(TIMEOUT_QUEUE, durable=True)
    await timeout_queue.bind(dlx, routing_key="timeout")

    dead_queue = await channel.declare_queue(DEAD_QUEUE, durable=True)
    await dead_queue.bind(dlx, routing_key="dead")

    return {
        "events": events,
        "dlx": dlx,
        "sms": sms,
        "points": points,
        "pay_wait": pay_wait,
        "timeout": timeout_queue,
        "dead": dead_queue,
    }
