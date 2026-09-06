"""发布一条订单支付消息。"""

import argparse
import json
from datetime import UTC, datetime
from uuid import uuid4

import pika

from rabbitMQ学习.config import open_connection
from rabbitMQ学习.topology import ORDER_EVENTS_EXCHANGE, ORDER_PAID_ROUTING_KEY


def publish_order_paid(order_id: str, *, fail: bool = False) -> str:
    event_id = uuid4().hex
    event = {
        "event_id": event_id,
        "event_name": "order.paid",
        "occurred_at": datetime.now(UTC).isoformat(),
        "order_id": order_id,
        "user_id": 10086,
        "total_amount": "99.00",
        "simulate_failure": fail,
    }
    with open_connection() as connection:
        channel = connection.channel()
        # 等服务器确认；这不代表消费者已完成工作。
        channel.confirm_delivery()
        channel.basic_publish(
            exchange=ORDER_EVENTS_EXCHANGE,
            routing_key=ORDER_PAID_ROUTING_KEY,
            body=json.dumps(event, ensure_ascii=False).encode("utf-8"),
            properties=pika.BasicProperties(
                content_type="application/json",
                content_encoding="utf-8",
                delivery_mode=2,
                message_id=event_id,
            ),
            mandatory=True,
        )
    return event_id


def main() -> None:
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
