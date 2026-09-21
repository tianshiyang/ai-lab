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
        e1: aio_pika.abc.AbstractExchange = config["e1"]

        async for message in q1.iterator():
            msg = json.loads(message.body.decode())
            if msg["fail"]:
                # 失败
                retry = message.headers.get("retry", 0) + 1
                if retry >= 3:
                    # 大于三次彻底失败，进入死信队列
                    await message.nack(requeue=False)
                    print(f"{msg['id']}第三次后彻底失败{retry}")
                    continue
                else:
                    # 进入延迟处理队列
                    if retry == 1:
                        await channel.default_exchange.publish(
                            aio_pika.Message(
                                body=json.dumps(msg).encode(),
                                headers={"retry": retry},
                                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            ),
                            routing_key="mq.queue.q3",
                        )
                        await message.ack()
                    else:
                        await channel.default_exchange.publish(
                            aio_pika.Message(
                                body=json.dumps(msg).encode(),
                                headers={"retry": retry},
                                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            ),
                            routing_key="mq.queue.q4",
                        )
                        print(f"{msg['id']}失败，并且重试中，当前：第{retry}次")
                        await message.ack()

            else:
                # 成功 -> 广播task.done消息
                await e1.publish(
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
