import asyncio
import json

import aio_pika
from common import RABBITMQ_URL, get_config

TASKS = [
    {"id": "T1", "fail": False},  # 一次成功:observer 看到 run + done
    {"id": "T2", "fail": True},  # 全链路:躺 5 秒回来、再躺 5 秒回来、三连败进死信池
]


async def main():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        config = await get_config(channel)
        exchange: aio_pika.abc.AbstractExchange = config["exchange"]

        for task in TASKS:
            await exchange.publish(
                aio_pika.Message(
                    json.dumps(
                        {**task, "message": f"创建任务成功 -> 任务ID: {task['id']}"}
                    ).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key="task.work",
            )


if __name__ == "__main__":
    asyncio.run(main())
