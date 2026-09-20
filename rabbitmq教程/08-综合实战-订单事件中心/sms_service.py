import asyncio
import json
from datetime import datetime

import aio_pika
import db
from common import RABBITMQ_URL, SMS_QUEUE, declare_topology

sent: set[str] = set()  # 本进程已发过短信的订单,重启清零


def now() -> str:
    return f"[{datetime.now():%H:%M:%S}]"


async def main():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        topo = declare_topology(channel=channel)
        sms: aio_pika.Queue = topo["sms"]

        print(f"{now()}短信服务已启动")
        async for message in sms.iterator():
            event = json.loads(message.body)
            order_id = event["order_id"]
            order = await db.get(order_id)
            retry = message.headers.get("retry", 0) if message.headers else 0

            if order_id in sent:
                print(f"{now()}短信：{order_id} 已发过，跳过")
                await message.ack()
                continue

            if order and order.sms_will_fail:
                if retry < 2:
                    print(f"{now()} 短信:{order_id} 失败(第 {retry + 1} 次),带计数重发")
                    await channel.default_exchange.publish(
                        aio_pika.Message(
                            message.body,
                            headers={"retry": retry + 1},
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        ),≤
                        routing_key=SMS_QUEUE,
                    )
                    await message.ack()
            else:
                print(f"{now()} 短信:{order_id} 三次全败 → nack(requeue=False) → 死信")
                await message.nack(requeue=False)
            continue

        amount = order.amount if order else "?"
        print(f"{now()} 短信:【云上小店】订单 {order_id} 已支付 {amount} 元")
        sent.add(order_id)
        await message.ack()


if __name__ == "__main__":
    asyncio.run(main())
