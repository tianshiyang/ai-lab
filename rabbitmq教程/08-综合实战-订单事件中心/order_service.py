import asyncio
import json
from datetime import datetime
from pathlib import Path

import aio_pika
import db
from common import PAY_WAIT, RABBITMQ_URL, declare_topology
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")

ORDERS = [
    ("A2001", 199, False, True),
    ("A2002", 59, False, False),
    ("A2003", 299, True, True),
]


def now() -> str:
    return f"[{datetime.now():%H:%M:%S}]"


def event_message(payload: dict, **kwargs) -> aio_pika.Message:
    """业务事件统一打包:json 内容 + 持久化"""
    return aio_pika.Message(
        json.dumps(payload, ensure_ascii=False).encode(),
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        **kwargs,
    )


async def pay(
    channel: aio_pika.Channel, events: aio_pika.ExchangeType, order_id: str
) -> aio_pika.Message:
    await db.mark_paid(order_id)
    await events.publish(
        event_message({"order_id": order_id, "event": "order.paid"}), routing_key="order.paid"
    )
    print(f"{now()} 用户付款:{order_id} → 广播 order.paid")


async def main() -> None:
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        topo = await declare_topology(channel)
        events: aio_pika.ExchangeType = topo["events"]
        await db.init_db()

        for order_id, amount, sms_will_fail, _will_pay in ORDERS:
            await db.create(order_id, amount, sms_will_fail)
            # 事件一:order.created(暂无订阅方)
            await events.publish(
                event_message({"order_id": order_id, "event": "order.created"}),
                routing_key="order.created",
            )
            # 事件二:延迟炸弹,直投付款等待队列
            await channel.default_exchange.publish(
                event_message({"order_id": order_id, "event": "payment.deadline"}),
                routing_key=PAY_WAIT,
            )

            print(f"{now()} 下单:{order_id},{amount} 元,15 秒付款窗口开始")
        print(f"{now()} 模拟付款:5 秒后 A2001,8 秒后 A2003")
        await asyncio.sleep(5)
        await pay(channel, events, "A2001")
        await asyncio.sleep(3)
        await pay(channel, events, "A2003")

        print(f"{now()} 订单服务退出,消费者继续在各自终端运行")

if __name__ == "__main__":
    asyncio.run(main())
