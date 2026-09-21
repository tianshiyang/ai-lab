import asyncio
import json

import aio_pika
from common import RABBITMQ_URL, get_config


async def main():
    connection = await aio_pika.connect(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        config = await get_config(channel)
        q1: aio_pika.abc.AbstractQueue = config["q1"]

        async for message in q1.iterator():
            msg = json.loads(message.body.decode())
            if msg["fail"]:
                # 失败
                retry = message.headers.get("retry", 0) + 1
                if retry >= 3:
                    # 大于三次彻底失败，进入死信队列
                    await message.nack(requeue=False)
                else:
                    # 进入延迟处理队列
                    await channel.default_exchange.publish(
                        aio_pika.Message(
                            body=json.dumps(msg).encode(),
                            headers={"retry": retry},
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        ),
                        routing_key="mq.queue.q3" if retry == 1 else "mq.queue.q4",
                    )
                    print(f"{msg['id']}重试中，第{retry}次")

                print(f"处理任务[失败]: {msg['id']}")
            else:
                # 成功 -> 广播task.done消息
                await channel.default_exchange.publish(
                    aio_pika.Message(
                        body=json.dumps(
                            {"id": msg["id"], "msg": f"{msg['id']}处理成功进入广播"}
                        ).encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key="task.done",
                )
                print(f"处理任务成功：{msg['id']}")

            await message.ack()


if __name__ == "__main__":
    asyncio.run(main())
