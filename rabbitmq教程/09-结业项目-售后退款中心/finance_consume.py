"""结业项目·出纳打款(消费者):竞争消费打款队列,失败走 5s/15s 阶梯重试,三连败进死信。

两个终端各跑一份竞争消费,第二个先改下面的 WORKER 名。

uv run python "rabbitmq教程/09-结业项目-售后退款中心/finance_consume.py"
"""

import asyncio
import json
from datetime import datetime

import aio_pika
from common import RETRY_QUEUE_15, RETRY_QUEUE_5, RABBITMQ_URL, get_config
from db import add_attempts, get_refunds, update_refunds

WORKER = "出纳A"  # 第二个终端改成"出纳B"


def now() -> str:
    return f"[{datetime.now():%H:%M:%S}]"


async def main():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)
    config = await get_config(channel)
    finance_queue: aio_pika.abc.AbstractQueue = config["finance_queue"]
    events: aio_pika.abc.AbstractExchange = config["events"]

    print(f"{now()} {WORKER} 已启动,等待打款任务")
    async for message in finance_queue.iterator():
        refund_id = json.loads(message.body)["refund_id"]
        retry = message.headers.get("retry", 0) if message.headers else 0  # 已失败几次
        row = await get_refunds(refund_id)

        if row is None or row.status != "待打款":  # 已退款/已终结/重复 → 跳过
            status = row.status if row else "不存在"
            print(f"{now()} {WORKER} {refund_id} 状态={status},跳过")
            await message.ack()
            continue

        await add_attempts(refund_id)  # 每次开始打款 +1,成功也计
        await asyncio.sleep(3)  # 打款一笔 3 秒

        if not row.pay_will_fail:  # 打款成功
            is_success = await update_refunds(
                refund_id=refund_id, old_status="待打款", status="已退款"
            )
            if not is_success:
                print(f"{now()} {WORKER} {refund_id} 状态已变,跳过(重投保护)")
                await message.ack()
                continue
            print(f"{now()} {WORKER} {refund_id} 打款成功,{row.amount} 元原路退回")
            await events.publish(
                aio_pika.Message(
                    body=json.dumps({"refund_id": refund_id}).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key="refund.paid",
            )
            await message.ack()
        elif retry < 2:  # 失败,还有重试机会:第 1 次失败进 5s 档,第 2 次进 15s 档
            print(
                f"{now()} {WORKER} {refund_id} 打款失败(第 {retry + 1} 次),"
                f"{'5' if retry == 0 else '15'} 秒后重试"
            )
            await channel.default_exchange.publish(
                aio_pika.Message(
                    message.body,  # 原 body 原样带走
                    headers={"retry": retry + 1},  # 计数 +1,回流后从这里读
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    priority=9 if row.is_vip else None,  # 重试路上保住 VIP 优先级
                ),
                routing_key=RETRY_QUEUE_5 if retry == 0 else RETRY_QUEUE_15,
            )
            await message.ack()  # 旧消息以"重发"的方式处理完毕
        else:  # 三连败,终结:改库 → 广播失败 → 死信
            is_success = await update_refunds(
                refund_id=refund_id, old_status="待打款", status="打款失败"
            )
            if not is_success:
                print(f"{now()} {WORKER} {refund_id} 状态已变,跳过(重投保护)")
                await message.ack()
                continue
            print(f"{now()} {WORKER} {refund_id} 三连败 → 打款失败,转人工处理")
            await events.publish(
                aio_pika.Message(
                    body=json.dumps({"refund_id": refund_id}).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key="refund.failed",
            )
            await message.nack(requeue=False)  # 全系统唯一的 nack:死信 → 死信池 → watcher


if __name__ == "__main__":
    asyncio.run(main())
