# 重置：按名字硬删本加练的全部队列与交换机，回到初始状态，方便反复演练。
# 队列参数（TTL、死信参数）在演练中常改，旧队列声明不进来，先 declare 再删会直接
# PRECONDITION_FAILED，所以不走 get_config()，按名字删最稳（删不存在的队列也不报错）。
#
# uv run python "rabbitmq教程/加练02-基本功/reset.py"

import asyncio

import aio_pika
from common import (
    DEAD_EXCHANGE,
    DEAD_TASK_QUEUE,
    DELAY_QUEUE_2,
    DELAY_QUEUE_3,
    EXCHANGE,
    NOTIFY_QUEUE,
    RABBITMQ_URL,
    TASK_QUEUE,
)

QUEUES = [
    TASK_QUEUE,
    NOTIFY_QUEUE,
    DELAY_QUEUE_2,
    DELAY_QUEUE_3,
    DEAD_TASK_QUEUE,
]

EXCHANGES = [
    EXCHANGE,
    DEAD_EXCHANGE,
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
