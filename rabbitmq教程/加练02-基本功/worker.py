import asyncio
import json

import aio_pika
from common import DELAY_QUEUE_2, DELAY_QUEUE_3, RABBITMQ_URL, get_config


async def main():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        config = await get_config(channel)
        task_queue: aio_pika.abc.AbstractQueue = config["task_queue"]
        exchange: aio_pika.abc.AbstractExchange = config["exchange"]

        async for message in task_queue.iterator():
            task = json.loads(message.body)
            if task["fail"]:
                retry = message.headers.get("retry", 0) + 1
                if retry == 1:
                    # 第一次失败
                    await channel.default_exchange.publish(
                        aio_pika.Message(
                            json.dumps(
                                {**task, "message": f"处理task的第一次失败，{task['id']}"}
                            ).encode(),
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        ),
                        routing_key=DELAY_QUEUE_2,
                    )
                    print(f"work -> 处理task的第【一】次失败，{task['id']}")
                    await message.ack()
                elif retry == 2:
                    # 第二次失败
                    await channel.default_exchange.publish(
                        aio_pika.Message(
                            json.dumps(
                                {**task, "message": f"处理task的第二次失败，{task['id']}"}
                            ).encode(),
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        ),
                        routing_key=DELAY_QUEUE_3,
                    )
                    print(f"work -> 处理task的第【二】次失败，{task['id']}")
                    await message.ack()
                else:
                    # 第三次失败 -> 直接进入死信队列
                    await message.nack(requeue=False)
            else:
                # 成功
                await exchange.publish(
                    aio_pika.Message(
                        json.dumps({**task, "message": f"处理task成功，{task['id']}"}).encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key="task.work",
                )
                print(f"work -> 处理task成功，{task['id']}")
                await message.ack()


if __name__ == "__main__":
    asyncio.run(main())
