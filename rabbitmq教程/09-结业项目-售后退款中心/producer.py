"""结业项目·提交端(生产者):提交退款申请、按时序撤销,剧本写死在 REFUNDS。

uv run python "rabbitmq教程/09-结业项目-售后退款中心/producer.py"
"""

import asyncio
from typing import Literal

import aio_pika
from common import AUDIT_QUEUE, RABBITMQ_URL, get_config
from db import create_refunds, update_refunds
from pydantic import BaseModel


class RefundPlan(BaseModel):
    """剧本行:一张退款单的全部既定命运"""

    refund_id: str  # 退款单号
    order_id: str  # 关联的原订单号
    amount: int  # 金额(元)
    is_vip: bool  # VIP 提交时消息带 priority=9
    audit: Literal["通过", "拒绝", "等待撤销"]  # 审核剧本
    finance: Literal["成功", "三连败"] | None  # 打款剧本;拒绝/撤销的单走不到打款,为 None


# 提交时序(需求书 §4):前 5 条 t=0 提交;R3005 在 t+2 被用户撤销;两条 VIP 在 t+3 提交
REFUNDS_NORMAL_1: list[RefundPlan] = [
    RefundPlan(
        refund_id="R3001", order_id="A2001", amount=45, is_vip=False, audit="通过", finance="成功"
    ),
    RefundPlan(
        refund_id="R3002", order_id="A2002", amount=59, is_vip=False, audit="通过", finance="成功"
    ),
    RefundPlan(
        refund_id="R3003", order_id="A2003", amount=299, is_vip=False, audit="拒绝", finance=None
    ),
    RefundPlan(
        refund_id="R3004",
        order_id="A2001",
        amount=320,
        is_vip=False,
        audit="通过",
        finance="三连败",
    ),
    RefundPlan(
        refund_id="R3005", order_id="A2003", amount=88, is_vip=False, audit="等待撤销", finance=None
    ),
]

REFUNDS_VIP_3 = [
    RefundPlan(
        refund_id="R3006", order_id="A2002", amount=120, is_vip=True, audit="通过", finance="成功"
    ),
    RefundPlan(
        refund_id="R3007", order_id="A2001", amount=30, is_vip=True, audit="通过", finance="成功"
    ),
]


async def main():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        await get_config(channel)
        for plan in REFUNDS_NORMAL_1:
            # 把数据加入到审计队列
            await create_refunds(
                refund_id=plan.refund_id,
                order_id=plan.order_id,
                amount=plan.amount,
                is_vip=plan.is_vip,
                status="待审核",
                audit_will_reject=plan.audit == "拒绝",
                pay_will_fail=plan.finance == "三连败",
            )
            result = await channel.default_exchange.publish(
                aio_pika.Message(
                    body=plan.model_dump_json().encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    priority=9 if plan.is_vip else None,
                ),
                routing_key=AUDIT_QUEUE,
            )
            print(f"创建待审核单: {plan.refund_id}, {result}")

        await asyncio.sleep(2)
        revoked = await update_refunds(refund_id="R3005", old_status="待审核", status="已撤销")
        print(f"撤销 R3005: {'成功' if revoked else '失败,状态已不是待审核'}")

        await asyncio.sleep(1)
        for plan in REFUNDS_VIP_3:
            # 把数据加入到审计队列
            await create_refunds(
                refund_id=plan.refund_id,
                order_id=plan.order_id,
                amount=plan.amount,
                is_vip=plan.is_vip,
                status="待审核",
                audit_will_reject=plan.audit == "拒绝",
                pay_will_fail=plan.finance == "三连败",
            )
            result = await channel.default_exchange.publish(
                aio_pika.Message(
                    body=plan.model_dump_json().encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    priority=9 if plan.is_vip else None,
                ),
                routing_key=AUDIT_QUEUE,
            )
            print(f"创建插队的待审核单: {plan.refund_id}, {result}")

        print("消息发送成功")


if __name__ == "__main__":
    asyncio.run(main())
