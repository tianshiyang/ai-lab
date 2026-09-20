import asyncio
import json

import aio_pika
from common import RABBITMQ_URL, get_config
from db import update_refunds
from producer import RefundPlan


async def main():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)
    config = await get_config(channel)
    audit_queue: aio_pika.abc.AbstractQueue = config["audit_queue"]
    events: aio_pika.abc.AbstractExchange = config["events"]

    async for message in audit_queue.iterator():
        msg = RefundPlan.model_validate(json.loads(message.body))
        if msg.audit == "通过":
            is_success = await update_refunds(
                refund_id=msg.refund_id, old_status="待审核", status="待打款"
            )
            print(
                f"{msg.refund_id}的状态变成待打款" if is_success else f"{msg.refund_id}的状态错误"
            )
            await events.publish(
                aio_pika.Message(
                    body=msg.model_dump_json().encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    priority=9 if msg.is_vip else None,
                ),
                routing_key="refund.approve",
            )
            await message.ack()
        elif msg.audit == "拒绝":
            is_success = await update_refunds(
                refund_id=msg.refund_id, old_status="待审核", status="已拒绝"
            )
            print(
                f"{msg.refund_id}的状态变成已拒绝"
                if is_success
                else f"{msg.refund_id}的审核状态错误"
            )
            await events.publish(
                aio_pika.Message(
                    body=msg.model_dump_json().encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    priority=9 if msg.is_vip else None,
                ),
                routing_key="refund.reject",
            )
            await message.ack()
        else:
            # 等待撤销
            is_success = await update_refunds(
                refund_id=msg.refund_id, old_status="待审核", status="已撤销"
            )
            print(
                f"{msg.refund_id}的状态变成已撤销"
                if is_success
                else f"{msg.refund_id}的审核状态错误"
            )
            await message.ack()


if __name__ == "__main__":
    asyncio.run(main())
