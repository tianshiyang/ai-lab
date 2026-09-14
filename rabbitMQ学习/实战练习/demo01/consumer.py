"""消费通道队列：aio-pika 全异步版，一个进程并发消费三个通道。

想只开单个通道（练习四多实例对比时），把 CHANNELS 改成如 ["sms"] 再各开一个运行窗口。
"""

import asyncio
import json
from datetime import datetime

import aio_pika

from rabbitMQ学习.实战练习.data.data import mock_config
from rabbitMQ学习.实战练习.demo01.store import (
    create_failed_record,
    create_gateway_call,
    create_send_record,
)
from rabbitMQ学习.实战练习.demo01.topology import connect
from rabbitMQ学习.实战练习.demo01.typings import Channel, FailedResult, GatewayResult, Result

# 消费的通道列表；单开某通道时改成一个元素
CHANNELS: list[Channel] = ["sms", "email", "inapp"]
prefetch_count = mock_config["consumer"]["prefetch_count"]
pause_before_ack_seconds = mock_config["consumer"]["pause_before_ack_seconds"]


async def handle(channel: Channel, message: aio_pika.Message) -> None:
    """处理一条消息：幂等检查、模拟调网关、落库、确认（练习二正在改的业务逻辑）。"""
    data = Result.model_validate(json.loads(message.body))
    # 模拟调用第三方网关的耗时。异步代码里必须用 asyncio.sleep：
    # time.sleep 会卡住整个事件循环，三个通道并发跑在同一个循环里，一个 sleep 冻住全部
    send_seconds = mock_config["gateway"][channel]["send_seconds"]

    record = await create_send_record(data)
    if not record:
        await create_failed_record(
            FailedResult(
                request_id=data.request_id,
                channel=data.channel,
                reason="数据重复异常",
                error_type="data_exist_error",
                attempts=0,
                permanent=True,
                raw_message=None,
                created_at=datetime.now(),
            )
        )
        await message.ack()
        return

    await asyncio.sleep(send_seconds)
    await create_gateway_call(
        GatewayResult(
            request_id=data.request_id,
            channel=data.channel,
            success=True,
            error_type=None,
            error_msg=None,
            called_at=datetime.now(),
        )
    )
    print(f"[{channel}] {data.request_id} {data.user_id} {data.content}")

    # 验收 3 的钩子：调完网关后停 N 秒再确认，制造"发了但没确认"的窗口（默认 0 不停）
    await asyncio.sleep(pause_before_ack_seconds)
    # 业务处理完成后才确认；中途退出的消息会被重新投递
    await message.ack()


async def consume(channel: Channel, connection: aio_pika.Connection) -> None:
    """消费指定通道的队列，直到连接关闭。"""
    ch = await connection.channel()
    # 一次只推一条，ack 了才推下一条；对 channel 全局生效，设一次就够
    await ch.set_qos(prefetch_count=prefetch_count)
    queue = await ch.declare_queue(channel, durable=True)
    print(f"[{channel}] 开始消费，Ctrl+C 退出")
    async for message in queue.iterator():
        await handle(channel, message)


async def main() -> None:
    """三个通道并发消费。"""
    async with connect() as (connection_1, _):
        await asyncio.gather(*(consume(ch, connection_1) for ch in CHANNELS))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
