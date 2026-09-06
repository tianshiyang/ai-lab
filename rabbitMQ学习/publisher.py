"""发布一条订单支付消息。"""

import argparse
import json
from datetime import UTC, datetime
from uuid import uuid4

import pika

from rabbitMQ学习.config import open_connection
from rabbitMQ学习.topology import ORDER_EVENTS_EXCHANGE, ORDER_PAID_ROUTING_KEY


def publish_order_paid(order_id: str, *, fail: bool = False) -> str:
    """发布成功后返回事件 ID；fail 只让邮件示例模拟失败，不影响发送过程。"""
    # 每次调用创建新的事件 ID；同一订单重复调用也会产生不同事件，不会自动去重。
    event_id = uuid4().hex
    event = {
        "event_id": event_id,
        "event_name": "order.paid",
        "occurred_at": datetime.now(UTC).isoformat(),  # 使用带时区的 UTC 时间。
        "order_id": order_id,
        "user_id": 10086,
        "total_amount": "99.00",  # 金额用字符串，避免二进制浮点数的表示误差。
        "simulate_failure": fail,
    }
    with open_connection() as connection:
        channel = connection.channel()
        # 等服务器确认；这不代表消费者已完成工作。
        channel.confirm_delivery()
        channel.basic_publish(
            exchange=ORDER_EVENTS_EXCHANGE,
            routing_key=ORDER_PAID_ROUTING_KEY,
            # 字典 → JSON 文本 → UTF-8 字节；ensure_ascii=False 保留可读中文。
            body=json.dumps(event, ensure_ascii=False).encode("utf-8"),
            properties=pika.BasicProperties(
                content_type="application/json",  # 声明正文格式，不会替我们校验 JSON。
                content_encoding="utf-8",  # 告诉接收方正文的编码方式。
                delivery_mode=2,  # 消息持久化，还需要目标队列本身持久化。
                message_id=event_id,  # 附加属性中的 ID 与正文一致，便于追踪。
            ),
            mandatory=True,  # 无法匹配任何队列时要求退回，不把无路由视为成功。
        )
    return event_id


def main() -> None:
    """解析 --order-id、--fail 参数，并将常见发布错误转成终端提示。"""
    parser = argparse.ArgumentParser(description="模拟订单支付成功，发出一条消息")
    parser.add_argument("--order-id", default="202609060001", help="订单号")
    parser.add_argument("--fail", action="store_true", help="让邮件消费者模拟处理失败")
    args = parser.parse_args()
    try:
        event_id = publish_order_paid(args.order_id, fail=args.fail)
    except pika.exceptions.UnroutableError:
        parser.exit(1, "发送失败：没有匹配的队列绑定，请先运行 topology。\n")
    except pika.exceptions.NackError:
        parser.exit(1, "发送失败：服务器未确认接收，请检查服务器状态。\n")
    print(f"已发送 order.paid：order_id={args.order_id}，event_id={event_id}")


if __name__ == "__main__":
    main()
