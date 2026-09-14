import json
import uuid
from typing import cast

import pika

from rabbitMQ学习.实战练习.data.data import requests, templates, users
from rabbitMQ学习.实战练习.demo01.topology import channel_1, connection_1
from rabbitMQ学习.实战练习.demo01.typings import Channel, Request, Result, Template, User


def build_result_data() -> list[Result]:
    """构建响应消息"""
    request_dict: dict[str, list[Request]] = {}

    for value in requests:
        request = Request.model_validate(value)
        if request_dict.get(request.request_id) is None:
            request_dict[request.request_id] = request_dict.get(request.request_id, [])
        request_dict[request.request_id].append(request)

    template_dict: dict[str, Template] = {}
    for value in templates:
        template = Template.model_validate(value)
        template_dict[template.template_id] = template

    user_dict: dict[str, User] = {}
    for value in users:
        user = User.model_validate(value)
        user_dict[user.user_id] = user

    results: list[Result] = []
    for request_id, values in request_dict.items():
        for value in values:
            template = template_dict.get(value.template_id)
            if template is None:
                print("template不存在")
                continue
            user = user_dict.get(value.user_id)
            if user is None:
                print("user不存在")
                continue
            channels = list(set(user.channels) & set(template.allow_channels))
            for channel in channels:
                try:
                    results.append(
                        Result(
                            task_id=str(uuid.uuid4()),
                            request_id=request_id,
                            user_id=value.user_id,
                            channel=cast(Channel, channel),
                            title=template.title,
                            content=template.content.format(**value.params),
                            biz_type=template.biz_type,
                            priority=template.priority,
                            delay_seconds=value.delay_seconds,
                        )
                    )
                except Exception as e:
                    print(f"报错了：{e}")

    return results


def publish(data: Result):
    """发布消息"""
    channel_1.basic_publish(
        exchange="ai_lab.notify.events",
        routing_key=f"notify.{data.biz_type}.{data.channel}",
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type="application/json",
            content_encoding="utf-8",
        ),
        body=json.dumps(data.model_dump(), ensure_ascii=False).encode("utf-8"),
    )


if __name__ == "__main__":
    for result in build_result_data():
        publish(result)

    channel_1.close()
    connection_1.close()
