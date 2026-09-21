# 交换机
import os
from pathlib import Path

import aio_pika
import dotenv

dotenv.load_dotenv(Path(__file__).parents[2] / ".env")

# 加载mq地址
RABBITMQ_URL = os.getenv("RABBITMQ_URL")

EXCHANGE = "re.mq.exchange"

# 任务队列
TASK_QUEUE = "re.mq.task.queue"

# 通知队列
NOTIFY_QUEUE = "re.mq.notify.queue"

# 延迟队列2秒
DELAY_QUEUE_2 = "re.mq.delay.2"
# 延迟队列3秒
DELAY_QUEUE_3 = "re.mq.delay.3"

# 死信交换机
DEAD_EXCHANGE = "re.mq.dead.exchange"
# 死信队列
DEAD_TASK_QUEUE = "re.mq.dead.queue"
# 死信队列routing_key
DEAD_ROUTING_KEY = "dead"


async def get_config(channel: aio_pika.abc.AbstractChannel) -> dict:
    """获取配置信息"""
    # 定义交换机
    exchange = await channel.declare_exchange(EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)

    # 定义任务队列
    task_queue = await channel.declare_queue(
        TASK_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DEAD_EXCHANGE,
            "x-dead-letter-routing-key": DEAD_ROUTING_KEY,
        },
    )
    await task_queue.bind(exchange, routing_key="task.work")

    # 定义通知队列
    notify_queue = await channel.declare_queue(NOTIFY_QUEUE, durable=True)
    await notify_queue.bind(exchange, routing_key="task.*")

    # 定义延迟队列2秒
    delay_queue_2 = await channel.declare_queue(
        DELAY_QUEUE_2, durable=True, arguments={"x-message-ttl": 2_000}
    )

    # 定义延迟队列3秒
    delay_queue_3 = await channel.declare_queue(
        DELAY_QUEUE_2, durable=True, arguments={"x-message-ttl": 3_000}
    )

    # 定义死信交换机
    dead_exchange = await channel.declare_exchange(
        DEAD_EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True
    )
    # 定义死信队列
    dead_task_queue = await channel.declare_queue(DEAD_TASK_QUEUE, durable=True)
    await dead_task_queue.bind(dead_exchange, routing_key=DEAD_ROUTING_KEY)

    return {
        "exchange": exchange,
        "task_queue": task_queue,
        "notify_queue": notify_queue,
        "delay_queue_2": delay_queue_2,
        "delay_queue_3": delay_queue_3,
    }
