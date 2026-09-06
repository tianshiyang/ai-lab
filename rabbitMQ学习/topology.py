"""订单练习用到的交换机、队列和绑定。"""

from pika.adapters.blocking_connection import BlockingChannel

from rabbitMQ学习.config import open_connection, resource_prefix

# PyCharm 练习开关：False 只建邮件相关资源；True 额外创建积分队列和绑定。
# 改回 False 不会删除之前建好的积分队列。
enable_points_queue = False


def declare_exchanges(channel: BlockingChannel) -> None:
    """创建两个分拣点：订单交换机和失败交换机。"""
    # topic 根据路由键匹配绑定规则；direct 要求路由键完全匹配。
    # durable=True 保存交换机定义，交换机本身不负责存储待处理消息。
    channel.exchange_declare(
        exchange=f"{resource_prefix}.order.events", exchange_type="topic", durable=True
    )
    channel.exchange_declare(
        exchange=f"{resource_prefix}.order.failure", exchange_type="direct", durable=True
    )


def declare_failure_queue(channel: BlockingChannel) -> None:
    """创建失败队列，并绑定 email.failed 路由键。"""
    # 先建失败队列，再把它绑定到失败交换机，准备好死信的接收路径。
    channel.queue_declare(
        queue=f"{resource_prefix}.order.email.failed.q",  # 邮件失败队列：保存待排查消息。
        durable=True,
        arguments={"x-queue-type": "classic"},
    )
    # 这条绑定表示：失败交换机收到 email.failed 时，把消息交给失败队列。
    channel.queue_bind(
        queue=f"{resource_prefix}.order.email.failed.q",
        exchange=f"{resource_prefix}.order.failure",  # 失败消息的分拣台。
        routing_key="email.failed",  # 接收邮件失败标签。
    )


def declare_email_queue(channel: BlockingChannel) -> None:
    """创建邮件队列，设置死信去处，再订阅 order.paid。"""
    channel.queue_declare(
        queue=f"{resource_prefix}.order.email.q",  # 邮件工作队列：等待发送通知。
        durable=True,
        arguments={
            "x-queue-type": "classic",
            "x-dead-letter-exchange": f"{resource_prefix}.order.failure",  # 被拒绝等死信的转发目标。
            "x-dead-letter-routing-key": "email.failed",  # 转发时替换路由键。
        },
    )
    channel.queue_bind(
        queue=f"{resource_prefix}.order.email.q",
        exchange=f"{resource_prefix}.order.events",  # 订单消息的分拣台。
        routing_key="order.paid",  # 邮件队列订阅订单已支付事件。
    )


def declare_points_queue(channel: BlockingChannel) -> None:
    """额外创建积分订阅；这一节只演示分发，积分队列没有死信配置。"""
    # 独立队列得到独立的一份消息，不会分走邮件队列里的消息。
    channel.queue_declare(
        queue=f"{resource_prefix}.order.points.q",  # 积分工作队列：等待处理积分。
        durable=True,
        arguments={"x-queue-type": "classic"},
    )
    channel.queue_bind(
        queue=f"{resource_prefix}.order.points.q",
        exchange=f"{resource_prefix}.order.events",
        routing_key="order.paid",
    )


def declare_order_topology(channel: BlockingChannel, *, with_points: bool = False) -> None:
    """按依赖顺序准备资源；同名、同参数可重复声明。"""
    # 这里的 * 表示 with_points 必须按名称传入，例如 with_points=True。
    # 它是普通函数参数，与启动命令中的参数无关。
    declare_exchanges(channel)
    declare_failure_queue(channel)
    declare_email_queue(channel)
    if with_points:
        declare_points_queue(channel)


def main() -> None:
    """PyCharm 运行入口：读取上方 enable_points_queue，只准备资源，不发送消息。"""
    with open_connection() as connection:
        declare_order_topology(connection.channel(), with_points=enable_points_queue)
    print(f"已创建交换机：{resource_prefix}.order.events、{resource_prefix}.order.failure")
    print(f"已创建队列：{resource_prefix}.order.email.q、{resource_prefix}.order.email.failed.q")
    if enable_points_queue:
        print(f"已创建积分队列：{resource_prefix}.order.points.q")


if __name__ == "__main__":
    main()
