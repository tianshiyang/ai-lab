"""结业项目·通知服务(消费者):听 refund.* 四类事件,统一向用户发通知。

uv run python "rabbitmq教程/09-结业项目-售后退款中心/notify_consume.py"
"""

import asyncio
import json
from datetime import datetime

import aio_pika
from common import RABBITMQ_URL, get_config
from db import get_refunds


def now() -> str:
    return f"[{datetime.now():%H:%M:%S}]"


async def main():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)
    config = await get_config(channel)
    notify_queue: aio_pika.abc.AbstractQueue = config["notify_queue"]

    print(f"{now()} 通知服务已启动")
    async for message in notify_queue.iterator():
        refund_id = json.loads(message.body)["refund_id"]
        row = await get_refunds(refund_id)  # 通知文案要的金额也查库拿真相
        amount = row.amount if row else "?"
        text = {
            "refund.approve": f"您的退款申请 {refund_id} 已通过审核,进入打款队列",
            "refund.reject": f"您的退款申请 {refund_id} 未通过审核,如有疑问请联系客服",
            "refund.paid": f"退款成功!{refund_id} 的 {amount} 元已原路退回",
            "refund.failed": f"退款打款失败({refund_id}),已转人工处理,请留意来电",
        }.get(message.routing_key, f"未知事件 {message.routing_key}: {refund_id}")
        print(f"{now()} [通知] {text}")
        await message.ack()


if __name__ == "__main__":
    asyncio.run(main())
