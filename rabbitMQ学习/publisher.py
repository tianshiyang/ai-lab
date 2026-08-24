"""下单后发布事件的生产者示例。"""

import json
from datetime import UTC, datetime
from decimal import Decimal
from time import time
from uuid import uuid4

import pika

from rabbitMQ学习.config import open_connection
from rabbitMQ学习.topology import (
    ORDER_CREATED_ROUTING_KEY,
    ORDER_EVENTS_EXCHANGE,
    declare_order_topology,
)


def publish_order_created(
    order_id: str,
    user_id: int,
    total_amount: Decimal,
    *,
    event_id: str | None = None,
) -> str:
    """把已提交订单转换成可重试、可去重的领域事件，并等待 broker confirm。"""
    event_id = event_id or uuid4().hex
    body = json.dumps(
        {
            "event_id": event_id,
            "event_name": "order.created",
            "schema_version": 1,
            "occurred_at": datetime.now(UTC).isoformat(),
            "order_id": order_id,
            "user_id": user_id,
            "total_amount": str(total_amount),
        },
        ensure_ascii=False,
    ).encode()
    properties = pika.BasicProperties(
        content_type="application/json",
        content_encoding="utf-8",
        delivery_mode=pika.DeliveryMode.Persistent,
        message_id=event_id,
        correlation_id=order_id,
        timestamp=int(time()),
        headers={"schema_version": 1},
    )

    with open_connection() as connection:
        channel = connection.channel()
        declare_order_topology(channel)
        channel.confirm_delivery()
        channel.basic_publish(
            exchange=ORDER_EVENTS_EXCHANGE,
            routing_key=ORDER_CREATED_ROUTING_KEY,
            body=body,
            properties=properties,
            mandatory=True,
        )

    return event_id
