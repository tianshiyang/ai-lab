import asyncio

import aio_pika
from common import RABBITMQ_URL

# 队列参数改了以后旧队列声明不进来，先按名字硬删（不能先 declare 再删，参数不一致直接 PRECONDITION_FAILED）
QUEUES = [
    "mq.queue.q1",
    "mq.queue.q2",
    "mq.queue.q3",
    "mq.queue.q4",
    "mq.dead.q1",
]

EXCHANGES = [
    "mq.exchange.e1",
    "mq.dead",
]


async def main():
    """清空练习拓扑：先删队列（自动解绑），再删交换机。"""
    connection = await aio_pika.connect(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        for name in QUEUES:
            await channel.queue_delete(name)
            print(f"已删除队列：{name}")
        for name in EXCHANGES:
            await channel.exchange_delete(name)
            print(f"已删除交换机：{name}")
        print("清理完成，重新跑任意脚本即可重建拓扑。")


if __name__ == "__main__":
    asyncio.run(main())
