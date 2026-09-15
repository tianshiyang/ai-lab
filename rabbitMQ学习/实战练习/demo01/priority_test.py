"""优先级验证脚本：先压一批营销（低优先级）进 sms 队列，再补几条验证码（高优先级）。

队列先进先出，先塞低的、后塞高的，高优先级能不能"插队"一眼便知。
用法：sms 消费者关着 → 跑本脚本 → 管理台确认 sms 队列有积压 → 开消费者看消费顺序。
"""

import asyncio
import json
import random
import uuid

import aio_pika

from rabbitMQ学习.实战练习.data.data import mock_config, templates
from rabbitMQ学习.实战练习.demo01.topology import connect
from rabbitMQ学习.实战练习.demo01.typings import Result, Template


def get_template(template_id: str) -> Template:
    """按模板 ID 从 templates.json 里取模板"""
    for value in templates:
        template = Template.model_validate(value)
        if template.template_id == template_id:
            return template
    raise ValueError(f"模板不存在: {template_id}")


def build_message(template: Template, request_id: str, params: dict[str, str]) -> Result:
    """渲染模板参数，构造一条发往 sms 的消息（字段和 producer 的 Result 一致）"""
    return Result(
        task_id=str(uuid.uuid4()),
        request_id=request_id,
        user_id="U001",
        channel="sms",
        title=template.title,
        content=template.content.format(**params),
        biz_type=template.biz_type,
        priority=template.priority,
        delay_seconds=0,
    )


async def publish(data: Result, exchange: aio_pika.Exchange) -> None:
    """发布消息（和 producer.py 同一个套路）"""
    await exchange.publish(
        aio_pika.Message(
            body=json.dumps(data.model_dump(), ensure_ascii=False).encode("utf-8"),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
            content_encoding="utf-8",
            priority=data.priority,
        ),
        routing_key=f"notify.{data.biz_type}.{data.channel}",
    )


async def main() -> None:
    """先营销铺底、再验证码插队，数量从 mock_config.batch 读，不写死"""
    batch = mock_config["batch"]
    marketing_template = get_template("TPL_MARKETING_618")
    verify_template = get_template("TPL_VERIFY_CODE")

    async with connect() as (_, channel_1):
        exchange = await channel_1.get_exchange("ai_lab.notify.events")

        # request_id 每条唯一：重复的话 consumer 的幂等闸门会把后面的当重复拦掉，日志一行不打
        # 第一轮：营销先进队，低优先级占住位置，制造积压
        for i in range(batch["marketing_count"]):
            message = build_message(marketing_template, f"PRI-M-{i:03d}", {"name": "张三", "coupon": "50"})
            await publish(message, exchange)

        # 第二轮：验证码后进队，按先进先出本该排在 30 条营销后面，开优先级才插得了队
        for i in range(batch["verify_code_count"]):
            code = f"{random.randint(0, 999999):06d}"  # 每条随机，消费日志里肉眼好认
            message = build_message(verify_template, f"PRI-V-{i:03d}", {"code": code})
            await publish(message, exchange)

    print(f"发布完成：营销 {batch['marketing_count']} 条 + 验证码 {batch['verify_code_count']} 条")


if __name__ == "__main__":
    asyncio.run(main())
