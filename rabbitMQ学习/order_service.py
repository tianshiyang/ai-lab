"""订单业务：检查消息并模拟处理，不负责连接 RabbitMQ，也不发送 ack。"""

import json


def parse_order_event(body: bytes) -> dict:
    """把 JSON 字节转换为字典，字段不符合约定时抛出 ValueError。"""
    # json.loads 可以直接接收 bytes；非法 JSON 会抛出 ValueError 的子类。
    event = json.loads(body)
    # isinstance 用于检查对象类型，防止收到列表、数字后还把它当字典使用。
    if not isinstance(event, dict) or not event.get("event_id"):
        raise ValueError("消息必须是带有 event_id 的 JSON 对象")
    # get 读取字段，不存在时返回 None；这里要求事件类型和订单号都正确。
    if event.get("event_name") != "order.paid" or not event.get("order_id"):
        raise ValueError("消息必须是带有 order_id 的 order.paid 事件")
    return event


def handle_order_event(event: dict, queue_kind: str) -> None:
    """模拟业务成功或失败；这里只打印，不实际发邮件或修改积分。"""
    if queue_kind not in ("email", "points"):
        raise ValueError("queue_kind 只能是 email 或 points")
    # 这个开关只影响邮件，方便观察同一条消息在不同业务中独立处理。
    if queue_kind == "email" and event.get("simulate_failure"):
        raise ValueError("simulate_failure=True，模拟邮件处理失败")
    action = "发送支付通知" if queue_kind == "email" else "增加积分"
    print(f"模拟{action}成功：order_id={event['order_id']}", flush=True)
