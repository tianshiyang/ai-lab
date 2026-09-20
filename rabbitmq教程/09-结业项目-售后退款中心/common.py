import aio_pika

EXCHANGE = "refund.events"  # 业务交换机
AUDIT_QUEUE = "refund.audit.queue"  # 审计队列
FINANCE_QUEUE = "refund.finance.queue"  # 财务队列
RETRY_QUEUE_5 = "refund.retry.5s"  # 5秒后重试队列
RETRY_QUEUE_15 = "refund.retry.15s"  # 15秒后重试队列
DLX = "refund.dlx"  # 死信交换机
DEAD_QUEUE = "refund.dead.queue"  # 死亡队列
NOTIFY_QUEUE = "refund.notify.queue"  # 消息通知队列


async def config(channel: aio_pika.Channel) -> dict:
    """声明所有MQ配置"""
    # 声明交换机
    events = await channel.declare_exchange(EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
    # 审核队列 -> 优先级队列
    audit_queue = await channel.declare_queue(
        AUDIT_QUEUE,
        durable=True,
        arguments={
            "x-max-priority": 10,
        },
    )
    # 财务队列
    finance_queue = await channel.declare_queue(
        FINANCE_QUEUE,
        durable=True,
        arguments={
            "x-max-priority": 10,
            "x-dead-letter-exchange": "refund.dlx",
            "x-dead-letter-routing-key": "dead",
        },
    )
    await finance_queue.bind(events, routing_key="refund.approve")

    # 5s重试队列
    retry_5s_queue = await channel.declare_queue(
        RETRY_QUEUE_5,
        durable=True,
        arguments={
            "x-message-ttl": 5_000,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": "refund.finance.queue",
        },
    )
    # 15s重试队列
    retry_15s_queue = await channel.declare_queue(
        RETRY_QUEUE_15,
        durable=True,
        arguments={
            "x-message-ttl": 15_000,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": "refund.finance.queue",
        },
    )

    # 死信交换机
    dlx = await channel.declare_exchange(DLX, aio_pika.ExchangeType.DIRECT, durable=True)
    # 死亡队列
    dead_queue = await channel.declare_queue(DEAD_QUEUE, durable=True)
    await dead_queue.bind(dlx, routing_key="dead")

    # 通知队列
    notify_queue = await channel.declare_queue(NOTIFY_QUEUE, durable=True)
    # 接受所有事件，并发送通知
    await notify_queue.bind(events, routing_key="refund.*")

    return {
        "events": events,
        "audit_queue": audit_queue,
        "finance_queue": finance_queue,
        "retry_5s_queue": retry_5s_queue,
        "retry_15s_queue": retry_15s_queue,
        "dlx": dlx,
        "dead_queue": dead_queue,
        "notify_queue": notify_queue,
    }
