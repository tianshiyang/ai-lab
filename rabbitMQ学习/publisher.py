"""发布一条订单支付消息。"""

import json
from datetime import UTC, datetime
from uuid import uuid4

import pika

from rabbitMQ学习.config import open_connection
from rabbitMQ学习.topology import ORDER_EVENTS_EXCHANGE, ORDER_PAID_ROUTING_KEY

# 练习时改这里，再点 PyCharm 的运行按钮。False / True 是 Python 的否 / 是。
ORDER_ID = "202609060001"  # 本次发送的订单号。
SIMULATE_FAILURE = False  # True 让邮件消费者模拟失败；消息仍正常发送。
MESSAGE_COUNT = 1  # 批量练习时改成 6；大于 1 时订单号自动加 -1、-2 等后缀。


def build_order_event(order_id: str, *, fail: bool = False) -> dict:
    """只组装订单事件数据，不连接 RabbitMQ，方便独立阅读和检查。"""
    # 每次调用创建新的事件 ID；同一订单重复调用也会产生不同事件，不会自动去重。
    event_id = uuid4().hex
    return {
        "event_id": event_id,
        "event_name": "order.paid",
        "occurred_at": datetime.now(UTC).isoformat(),  # 使用带时区的 UTC 时间。
        "order_id": order_id,
        "user_id": 10086,
        "total_amount": "99.00",  # 金额用字符串，避免二进制浮点数的表示误差。
        "simulate_failure": fail,
    }


def publish_order_paid(order_id: str, *, fail: bool = False) -> str:
    """发布成功后返回事件 ID；fail 只让邮件示例模拟失败，不影响发送过程。"""
    event = build_order_event(order_id, fail=fail)
    event_id = event["event_id"]
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
    """读取本文件顶部的练习设置，发送后退出；失败用非零退出码表示。"""
    if not isinstance(MESSAGE_COUNT, int) or isinstance(MESSAGE_COUNT, bool) or MESSAGE_COUNT < 1:
        raise ValueError("MESSAGE_COUNT 必须是大于 0 的整数。")
    try:
        for index in range(MESSAGE_COUNT):
            # range 从 0 开始，所以给订单号加后缀时使用 index + 1。
            order_id = ORDER_ID if MESSAGE_COUNT == 1 else f"{ORDER_ID}-{index + 1}"
            event_id = publish_order_paid(order_id, fail=SIMULATE_FAILURE)
            print(f"已发送 order.paid：order_id={order_id}，event_id={event_id}")
    except pika.exceptions.UnroutableError:
        raise SystemExit("发送失败：没有匹配的队列绑定，请先运行 topology.py。") from None
    except pika.exceptions.NackError:
        raise SystemExit("发送失败：服务器未确认接收，请检查服务器状态。") from None


if __name__ == "__main__":
    main()
