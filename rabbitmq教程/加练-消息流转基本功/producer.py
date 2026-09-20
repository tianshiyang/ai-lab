"""加练·生产者:发出两条任务消息,跑完就退出。

uv run python "rabbitmq教程/加练-消息流转基本功/producer.py"
"""

import asyncio
import json
from datetime import datetime

import aio_pika
from common import E1, RABBITMQ_URL, TASK_RUN, declare


def now() -> str:
    return f"[{datetime.now():%H:%M:%S}]"


# 剧本:两条任务,写死在这里
TASKS = [
    {"id": "T1", "fail": False},  # 一次成功:worker 广播 done,observer 看到 run + done
    {"id": "T2", "fail": True},  # 全链路:躺 5 秒回来,再躺 5 秒回来,三连败进死信池
]


async def main() -> None:
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        topo = await declare(channel)
        e1: aio_pika.Exchange = topo["e1"]  # type: ignore[assignment]

        for task in TASKS:
            body = json.dumps(task, ensure_ascii=False).encode()
            receipt = await e1.publish(
                aio_pika.Message(body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                routing_key=TASK_RUN,  # ① 发任务
            )
            print(f"{now()} 发出 {task['id']}(fail={task['fail']}),broker 回执 {type(receipt).__name__}")

    print(f"{now()} producer 退出,worker/observer/watcher 继续在各自终端跑")


asyncio.run(main())
