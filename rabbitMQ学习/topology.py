"""订单练习用到的交换机、队列和绑定。"""

import argparse

from pika.adapters.blocking_connection import BlockingChannel

from rabbitMQ学习.config import PREFIX, open_connection

# 名称集中在这里，生产者和消费者共用，避免各自拼写导致连接到不同资源。
ORDER_EVENTS_EXCHANGE = f"{PREFIX}.order.events"
ORDER_FAILURE_EXCHANGE = f"{PREFIX}.order.failure"
ORDER_PAID_ROUTING_KEY = "order.paid"
ORDER_FAILURE_ROUTING_KEY = "email.failed"
ORDER_EMAIL_QUEUE = f"{PREFIX}.order.email.q"
ORDER_POINTS_QUEUE = f"{PREFIX}.order.points.q"
ORDER_FAILURE_QUEUE = f"{PREFIX}.order.email.failed.q"


def declare_order_topology(channel: BlockingChannel, *, with_points: bool = False) -> None:
    """同名、同参数可重复声明。先准备失败消息的去处，再创建工作队列。"""
    # topic 根据路由键匹配绑定规则；direct 要求路由键完全匹配。
    # durable=True 保存交换机定义，交换机本身不负责存储待处理消息。
    channel.exchange_declare(exchange=ORDER_EVENTS_EXCHANGE, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=ORDER_FAILURE_EXCHANGE, exchange_type="direct", durable=True)
    # 先建失败队列，再把它绑定到失败交换机，准备好死信的接收路径。
    channel.queue_declare(
        queue=ORDER_FAILURE_QUEUE, durable=True, arguments={"x-queue-type": "classic"}
    )
    # 这条绑定表示：失败交换机收到 email.failed 时，把消息交给失败队列。
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
            "x-dead-letter-exchange": ORDER_FAILURE_EXCHANGE,  # 被拒绝等死信的转发目标。
            "x-dead-letter-routing-key": ORDER_FAILURE_ROUTING_KEY,  # 转发时替换路由键。
        },
    )
    channel.queue_bind(
        queue=ORDER_EMAIL_QUEUE,
        exchange=ORDER_EVENTS_EXCHANGE,
        routing_key=ORDER_PAID_ROUTING_KEY,
    )
    # 第二个队列让积分业务也得到一份消息；它不会分走邮件队列里的消息。
    # 积分示例只演示订阅，没有配置死信队列。
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
    """只创建资源，不发消息；--with-points 额外创建积分订阅。"""
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
