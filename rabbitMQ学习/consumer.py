"""带手动确认、固定延迟重试和失败归档的 worker 示例。"""

import json
from collections.abc import Callable
from typing import Any

import pika

from rabbitMQ学习.config import open_connection
from rabbitMQ学习.topology import (
    ORDER_CREATED_QUEUE,
    ORDER_FAILURE_EXCHANGE,
    ORDER_FAILURE_ROUTING_KEY,
    declare_order_topology,
)

MAX_RETRY_COUNT = 3


class PermanentBusinessError(Exception):
    """数据本身不合法等不可重试的业务异常。"""


def rejected_retry_count(properties: pika.BasicProperties) -> int:
    """读取 broker 写入的 x-death，得到该消息已完成的重试次数。"""
    headers = properties.headers or {}
    x_death = headers.get("x-death", [])
    return sum(
        int(item["count"])
        for item in x_death
        if item.get("queue") == ORDER_CREATED_QUEUE and item.get("reason") == "rejected"
    )


def publish_to_failure_queue(
    channel: pika.channel.Channel,
    body: bytes,
    properties: pika.BasicProperties,
) -> None:
    """先确认失败消息已被 broker 接收，调用方才能确认原消息。"""
    channel.basic_publish(
        exchange=ORDER_FAILURE_EXCHANGE,
        routing_key=ORDER_FAILURE_ROUTING_KEY,
        body=body,
        properties=properties,
        mandatory=True,
    )


def consume_order_created(handle: Callable[[dict[str, Any]], None]) -> None:
    """持续消费订单事件。

    handle 必须在自己的数据库事务中完成幂等判断、业务写入和已消费记录写入。
    """
    connection = open_connection()
    channel = connection.channel()
    declare_order_topology(channel)
    channel.confirm_delivery()
    channel.basic_qos(prefetch_count=10)

    def on_message(
        callback_channel: pika.channel.Channel,
        method: pika.spec.Basic.Deliver,
        properties: pika.BasicProperties,
        body: bytes,
    ) -> None:
        try:
            event = json.loads(body)
            if not isinstance(event, dict) or not event.get("event_id"):
                raise PermanentBusinessError("消息缺少 event_id")
            handle(event)
        except PermanentBusinessError:
            publish_to_failure_queue(callback_channel, body, properties)
            callback_channel.basic_ack(method.delivery_tag)
        except Exception:
            if rejected_retry_count(properties) >= MAX_RETRY_COUNT:
                publish_to_failure_queue(callback_channel, body, properties)
                callback_channel.basic_ack(method.delivery_tag)
            else:
                callback_channel.basic_nack(method.delivery_tag, requeue=False)
        else:
            callback_channel.basic_ack(method.delivery_tag)

    channel.basic_consume(
        queue=ORDER_CREATED_QUEUE,
        on_message_callback=on_message,
        auto_ack=False,
    )
    try:
        channel.start_consuming()
    finally:
        if connection.is_open:
            connection.close()
