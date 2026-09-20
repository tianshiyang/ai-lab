"""结业项目·客服审核(消费者):竞争消费审核队列,按剧本流转状态并广播结果。

两个终端各跑一份竞争消费,第二个先改下面的 WORKER 名,日志才分得清是谁。

uv run python "rabbitmq教程/09-结业项目-售后退款中心/audit_consume.py"
"""

import asyncio
import json
from datetime import datetime

import aio_pika
from common import RABBITMQ_URL, get_config
from db import update_refunds
from producer import RefundPlan

WORKER = "客服A"  # 第二个终端改成"客服B"


def now() -> str:
    return f"[{datetime.now():%H:%M:%S}]"


async def main():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)
    config = await get_config(channel)
    audit_queue: aio_pika.abc.AbstractQueue = config["audit_queue"]
    events: aio_pika.abc.AbstractExchange = config["events"]

    print(f"{now()} {WORKER} 已启动,等待审核任务")
    async for message in audit_queue.iterator():
        msg = RefundPlan.model_validate(json.loads(message.body))

        # 撤销的单:撤销是提交端的活,正常时序下它早已改库,这里流转必失败 → 跳过
        if msg.audit == "等待撤销":
            is_success = await update_refunds(
                refund_id=msg.refund_id, old_status="待审核", status="已撤销"
            )
            if is_success:  # 极小概率抢在提交端之前,替它补上,结局一致
                print(f"{now()} {WORKER} {msg.refund_id} 撤销生效(提交端未及改库)")
            else:
                print(f"{now()} {WORKER} {msg.refund_id} 已撤销,跳过")
            await message.ack()
            continue

        await asyncio.sleep(4)  # 审核一单 4 秒

        if msg.audit == "拒绝":
            is_success = await update_refunds(
                refund_id=msg.refund_id, old_status="待审核", status="已拒绝"
            )
            if not is_success:  # 状态已不是待审核:重投/重复,不广播
                print(f"{now()} {WORKER} {msg.refund_id} 状态已变,跳过(重投保护)")
                await message.ack()
                continue
            print(f"{now()} {WORKER} {msg.refund_id} 审核拒绝")
            await events.publish(
                aio_pika.Message(
                    body=json.dumps({"refund_id": msg.refund_id}).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key="refund.reject",
            )
        else:  # 通过
            is_success = await update_refunds(
                refund_id=msg.refund_id, old_status="待审核", status="待打款"
            )
            if not is_success:  # 状态已不是待审核:重投/重复,不广播
                print(f"{now()} {WORKER} {msg.refund_id} 状态已变,跳过(重投保护)")
                await message.ack()
                continue
            print(f"{now()} {WORKER} {msg.refund_id} 审核通过 → 待打款")
            await events.publish(
                aio_pika.Message(
                    body=json.dumps({"refund_id": msg.refund_id}).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    priority=9 if msg.is_vip else None,  # VIP 在打款段继续插队
                ),
                routing_key="refund.approve",
            )

        await message.ack()


if __name__ == "__main__":
    asyncio.run(main())
