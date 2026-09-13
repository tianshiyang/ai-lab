import json
import time
import uuid
from typing import TypedDict, Literal, List, cast

import pika
from pydantic import BaseModel, ConfigDict

from rabbitMQ学习.实战练习.data.data import requests, templates, users
from rabbitMQ学习.实战练习.demo01.topology import channel_1

Channel = Literal["sms", "email", "inapp"]


class Request(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str
    note: str
    user_id: str
    template_id: str
    params: dict[str, str]
    delay_seconds: int


class Template(BaseModel):
    model_config = ConfigDict(extra="forbid")
    template_id: str
    biz_type: str
    priority: int
    allow_channels: list[Channel]
    title: str
    content: str


class User(BaseModel):
    model_config = ConfigDict(extra="forbid")
    user_id: str
    name: str
    phone: str
    email: str
    channels: list[Channel]


class Result(BaseModel):
    model_config = ConfigDict(extra="forbid")
    task_id: str
    request_id: str
    user_id: str
    channels: List[Channel]
    title: str
    content: str
    biz_type: str
    priority: int
    delay_seconds: int


def build_result_data() -> List[Result]:
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
                continue
            user = user_dict.get(value.user_id)
            if user is None:
                continue
            channels = list(set(user.channels) & set(template.allow_channels))
            results.append(
                Result(
                    task_id=str(uuid.uuid4()),
                    request_id=request_id,
                    user_id=value.user_id,
                    channels=cast(list[Channel], channels),
                    title=template.title,
                    content=template.content,
                    biz_type=template.biz_type,
                    priority=template.priority,
                    delay_seconds=value.delay_seconds,
                )
            )

    return results


def publish(data: Result):
    """发布消息"""
    for channel in data.channels:
        channel_1.basic_publish(
            exchange="ai_lab.notify.events",
            routing_key=f"notify.{data.biz_type}.{channel}",
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
                content_encoding="utf-8",
            ),
            body=json.dumps(data.model_dump()).encode("utf-8"),
        )


if __name__ == "__main__":
    for result in build_result_data():
        publish(result)

    channel_1.close()



