"""订单练习用到的交换机、队列和绑定。"""

import argparse

from pika.adapters.blocking_connection import BlockingChannel

from rabbitMQ学习.config import PREFIX, open_connection

ORDER_EVENTS_EXCHANGE = f"{PREFIX}.order.events"
ORDER_FAILURE_EXCHANGE = f"{PREFIX}.order.failure"
ORDER_PAID_ROUTING_KEY = "order.paid"
ORDER_FAILURE_ROUTING_KEY = "email.failed"
ORDER_EMAIL_QUEUE = f"{PREFIX}.order.email.q"
ORDER_POINTS_QUEUE = f"{PREFIX}.order.points.q"
ORDER_FAILURE_QUEUE = f"{PREFIX}.order.email.failed.q"


def declare_order_topology(channel: BlockingChannel, *, with_points: bool = False) -> None:
    """同名、同参数可重复声明。先准备失败消息的去处，再创建工作队列。"""
    channel.exchange_declare(exchange=ORDER_EVENTS_EXCHANGE, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=ORDER_FAILURE_EXCHANGE, exchange_type="direct", durable=True)
    channel.queue_declare(
        queue=ORDER_FAILURE_QUEUE, durable=True, arguments={"x-queue-type": "classic"}
    )
    channel.queue_bind(
        queue=ORDER_FAILURE_QUEUE,
        exchange=ORDER_FAILURE_EXCHANGE,
        routing_key=ORDER_FAILURE_ROUTING_KEY,
    )
    channel.queue_declare(
        queue=ORDER_EMAIL_QUEUE,
        durable=True,
        arguments={
            "x-queue-type": "classic",
            "x-dead-letter-exchange": ORDER_FAILURE_EXCHANGE,
            "x-dead-letter-routing-key": ORDER_FAILURE_ROUTING_KEY,
        },
    )
    channel.queue_bind(
        queue=ORDER_EMAIL_QUEUE,
        exchange=ORDER_EVENTS_EXCHANGE,
        routing_key=ORDER_PAID_ROUTING_KEY,
    )
    if with_points:
        channel.queue_declare(
            queue=ORDER_POINTS_QUEUE, durable=True, arguments={"x-queue-type": "classic"}
        )
        channel.queue_bind(
            queue=ORDER_POINTS_QUEUE,
            exchange=ORDER_EVENTS_EXCHANGE,
            routing_key=ORDER_PAID_ROUTING_KEY,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="创建订单练习的交换机、队列和绑定")
    parser.add_argument("--with-points", action="store_true", help="同时创建积分队列")
    args = parser.parse_args()
    with open_connection() as connection:
        declare_order_topology(connection.channel(), with_points=args.with_points)
    print(f"已创建交换机：{ORDER_EVENTS_EXCHANGE}、{ORDER_FAILURE_EXCHANGE}")
    print(f"已创建队列：{ORDER_EMAIL_QUEUE}、{ORDER_FAILURE_QUEUE}")
    if args.with_points:
        print(f"已创建积分队列：{ORDER_POINTS_QUEUE}")


if __name__ == "__main__":
    main()
