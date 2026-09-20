"""加练·worker:消费工作队列 q1,一进三出(成功 / 延迟重试 / 放弃死信)。

uv run python "rabbitmq教程/加练-消息流转基本功/worker.py"
"""

import asyncio
import json
from datetime import datetime

import aio_pika
from common import E1, Q3, RABBITMQ_URL, TASK_DONE, declare


def now() -> str:
    return f"[{datetime.now():%H:%M:%S}]"


async def main() -> None:
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        topo = await declare(channel)
        e1: aio_pika.Exchange = topo["e1"]  # type: ignore[assignment]

        print(f"{now()} worker 已启动,等任务")
        async for message in topo["q1"].iterator():  # type: ignore[attr-defined]
            task = json.loads(message.body)  # ④ 解 body 的 fail
            task_id = task["id"]
            retry = message.headers.get("retry", 0) if message.headers else 0  # 解 header 的次数

            if not task["fail"]:  # 成功 → ⑤ 广播完成事件
                print(f"{now()} {task_id} 处理成功 → 广播 {TASK_DONE}")
                await e1.publish(
                    aio_pika.Message(message.body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                    routing_key=TASK_DONE,
                )
                await message.ack()

            elif retry < 2:  # 失败还有救 → ⑥ 直投等待队列,躺 5 秒后自动回到本队列
                print(f"{now()} {task_id} 失败(第 {retry + 1} 次),5 秒后重试")
                await channel.default_exchange.publish(
                    aio_pika.Message(
                        message.body,  # body 原样带走
                        headers={"retry": retry + 1},  # 次数 +1,回流后还在
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key=Q3,  # 键 = q3 的队列名,直投
                )
                await message.ack()  # 旧消息以"重发"的方式处理完毕

            else:  # 三连败 → ⑧ 全系统唯一一次 nack,死信机制接管送进 q4
                print(f"{now()} {task_id} 三连败,放弃 → nack 进死信池")
                await message.nack(requeue=False)


asyncio.run(main())
