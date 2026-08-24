"""下单事件的 RabbitMQ 拓扑声明。

所有服务使用同一组名字，避免生产者、消费者和重试队列的路由规则不一致。
"""

import pika

ORDER_EVENTS_EXCHANGE = "order.events"
ORDER_RETRY_EXCHANGE = "order.retry"
ORDER_FAILURE_EXCHANGE = "order.failure"

ORDER_CREATED_ROUTING_KEY = "order.created"
ORDER_RETRY_ROUTING_KEY = "retry.5s"
ORDER_FAILURE_ROUTING_KEY = "order.created.failed"

ORDER_CREATED_QUEUE = "order.created.q"
ORDER_RETRY_QUEUE = "order.created.retry.5s"
ORDER_FAILURE_QUEUE = "order.created.failure.q"

RETRY_DELAY_MS = 5_000


def declare_order_topology(channel: pika.channel.Channel) -> None:
    """声明下单事件、固定 5 秒重试与最终失败队列。可重复调用。"""
    channel.exchange_declare(
        exchange=ORDER_EVENTS_EXCHANGE,
        exchange_type="topic",
        durable=True,
    )
    channel.exchange_declare(
        exchange=ORDER_RETRY_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )
    channel.exchange_declare(
        exchange=ORDER_FAILURE_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )

    channel.queue_declare(
        queue=ORDER_CREATED_QUEUE,
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": ORDER_RETRY_EXCHANGE,
            "x-dead-letter-routing-key": ORDER_RETRY_ROUTING_KEY,
        },
    )
    channel.queue_bind(
        queue=ORDER_CREATED_QUEUE,
        exchange=ORDER_EVENTS_EXCHANGE,
        routing_key=ORDER_CREATED_ROUTING_KEY,
    )

    channel.queue_declare(
        queue=ORDER_RETRY_QUEUE,
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-message-ttl": RETRY_DELAY_MS,
            "x-dead-letter-exchange": ORDER_EVENTS_EXCHANGE,
            "x-dead-letter-routing-key": ORDER_CREATED_ROUTING_KEY,
        },
    )
    channel.queue_bind(
        queue=ORDER_RETRY_QUEUE,
        exchange=ORDER_RETRY_EXCHANGE,
        routing_key=ORDER_RETRY_ROUTING_KEY,
    )

    channel.queue_declare(
        queue=ORDER_FAILURE_QUEUE,
        durable=True,
        arguments={"x-queue-type": "quorum"},
    )
    channel.queue_bind(
        queue=ORDER_FAILURE_QUEUE,
        exchange=ORDER_FAILURE_EXCHANGE,
        routing_key=ORDER_FAILURE_ROUTING_KEY,
    )
