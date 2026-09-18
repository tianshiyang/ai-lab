import asyncio
import json
import os
from datetime import datetime
from pathlib import Path

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]


def now() -> str:
    return f"[{datetime.now():%H:%M:%S}]"


async def main() -> None:
    connection = await aio_pika.connect(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        # 死信交换机
        dlx = await channel.declare_exchange("ch6.dlx", aio_pika.ExchangeType.DIRECT, durable=True)

        # 正常的等待队列
        wait = await channel.declare_queue(
            "ch6.pay.wait",
            durable=True,
            arguments={
                "x-message-ttl": 10_000,
                "x-dead-letter-exchange": "ch6.dlx",
                "x-dead-letter-routing-key": "pay.timeout",
            },
        )

        # 死信队列
        timeout_queue = await channel.declare_queue("ch6.pay.timeout", durable=True)
        await timeout_queue.bind(dlx, routing_key="pay.timeout")

        await wait.purge()
        await timeout_queue.purge()

        order = {"order_id": "D4001", "amount": 199, "status": "待付款"}
        await channel.default_exchange.publish(
            aio_pika.Message(json.dumps(order).encode("utf-8")), routing_key=wait.name
        )

        print(f"{now()} 下单: {order}")
        print(f"{now()} 消息进入等待队列,10 秒倒计时开始")

        await asyncio.sleep(5)
        print(f"{now()} 5 秒,用户仍未付款")
        await asyncio.sleep(5)
        print(f"{now()} 10 秒,TTL 到期,死信转投到点处理队列")

        async for message in timeout_queue.iterator():
            received = json.loads(message.body)
            reason = message.headers["x-death"][0]["reason"]
            print(f"{now()} 收到超时事件: 订单 {received['order_id']}")
            print(f"    死因: {reason}")
            print(f"    回查订单状态 → {received['status']} → 执行取消、释放库存")
            await message.ack()
            break

        await wait.delete()
        # delete 默认 if_unused=True:有消费者挂着会拒绝删,这里显式硬删
        await timeout_queue.delete(if_unused=False)
        await dlx.delete()


if __name__ == "__main__":
    asyncio.run(main())
