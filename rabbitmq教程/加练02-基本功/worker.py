import asyncio
import json

import aio_pika
from common import DEAD_ROUTING_KEY, RABBITMQ_URL, RETRY_2, RETRY_3, get_config


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
                    await exchange.publish(
                        aio_pika.Message(
                            body=json.dumps(
                                {**task, "message": f"处理task的第一次失败,等2秒，{task['id']}"}
                            ).encode(),
                            headers={**message.headers, "retry": retry},
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        ),
                        routing_key=RETRY_2,
                    )
                    print(f"work -> 处理task的第【一】次失败，{task['id']}")
                    await message.ack()
                elif retry == 2:
                    # 第二次失败
                    await exchange.publish(
                        aio_pika.Message(
                            body=json.dumps(
                                {**task, "message": f"处理task的第二次失败，等3秒，{task['id']}"}
                            ).encode(),
                            headers={**message.headers, "retry": retry},
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        ),
                        routing_key=RETRY_3,
                    )
                    print(f"work -> 处理task的第【二】次失败，{task['id']}")
                    await message.ack()
                else:
                    # 第三次失败 -> 直接进入死信队列
                    # await exchange.publish(
                    #     aio_pika.Message(
                    #         body=json.dumps(
                    #             {
                    #                 **task,
                    #                 "message": f"第三次重试未成功，彻底回退，{task['id']}，{task['id']}",
                    #             }
                    #         ).encode(),
                    #         headers={**message.headers, "retry": retry},
                    #         delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    #     ),
                    #     routing_key=DEAD_ROUTING_KEY,
                    # )
                    await message.nack(requeue=False)
            else:
                # 成功
                await exchange.publish(
                    aio_pika.Message(
                        body=json.dumps(
                            {**task, "message": f"work -> 处理task成功，{task['id']}，{task['id']}"}
                        ).encode(),
                        headers={**message.headers},
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key="task.done",
                )
                print(f"work -> 处理task成功，{task['id']}")
                await message.ack()


if __name__ == "__main__":
    asyncio.run(main())
