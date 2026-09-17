import asyncio
import json
import os
from datetime import datetime
from pathlib import Path

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]

PREFETCH = 1
WORKER = "李四"


def now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


async def main():
    """主函数"""
    connection = await aio_pika.connect(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        # 限流
        await channel.set_qos(prefetch_count=PREFETCH)
        queue = await channel.declare_queue("ch2.tasks", durable=True)

        print(f"[{now()}] {WORKER} 上岗(prefetch={PREFETCH}),开始接活……")

        async for message in queue.iterator():
            task = json.loads(message.body)
            print(
                f"[{now()}] [{WORKER}] 接单: {task['name']},干 {task['seconds']} 秒"
                f"{'  ← 这条是崩过后重新投递的' if message.redelivered else ''}"
            )
            await asyncio.sleep(task["seconds"])
            if "会出事" in task["name"]:
                print(f"[{now()}] [{WORKER}] 处理到一半进程崩溃!没 ack,broker 会把消息收回去")
                os._exit(1)  # ④ 硬退出,不走任何清理,连接直接断
            await message.ack()
        print(f"[{now()}] [{WORKER}] 干完并确认: {task['name']}")


if __name__ == "__main__":
    asyncio.run(main())
