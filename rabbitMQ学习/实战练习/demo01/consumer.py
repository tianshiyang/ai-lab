"""消费通道队列：幂等、调网关，临时错误按档位延迟重试，重试耗尽归档留证据。

想只开单个通道（练习四多实例对比时），把 CHANNELS 改成如 ["sms"] 再各开一个运行窗口。
"""

import asyncio
import json
from datetime import datetime

import aio_pika

from rabbitMQ学习.实战练习.data.data import mock_config
from rabbitMQ学习.实战练习.demo01 import gateway
from rabbitMQ学习.实战练习.demo01.gateway import PermanentError, TemporaryError
from rabbitMQ学习.实战练习.demo01.store import (
    call_count,
    create_failed_record,
    create_send_record,
    is_sent,
)
from rabbitMQ学习.实战练习.demo01.topology import connect
from rabbitMQ学习.实战练习.demo01.typings import Channel, FailedResult, Result

# 消费的通道列表；单开某通道时改成一个元素
CHANNELS: list[Channel] = ["sms", "email", "inapp"]
prefetch_count = mock_config["consumer"]["prefetch_count"]
pause_before_ack_seconds = mock_config["consumer"]["pause_before_ack_seconds"]
# 重试阶梯 [5, 15, 60]：首次 + 3 次重试，最多试 4 次
backoff_seconds: list[int] = mock_config["retry"]["backoff_seconds"]


async def archive(
    channel: Channel,
    message: aio_pika.Message,
    failed_exchange: aio_pika.Exchange,
    request_id: str,
    reason: str,
    error_type: str | None,
    attempts: int,
    permanent: bool,
) -> None:
    """归档一条消息：转投 failed 交换机留证据 + 落失败表 + ack。"""
    # 先转投再确认：要是先 ack，转投前崩溃，这条消息就真没了
    await failed_exchange.publish(
        aio_pika.Message(
            body=message.body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key=channel,
    )
    await create_failed_record(
        FailedResult(
            request_id=request_id,
            channel=channel,
            reason=reason,
            error_type=error_type,
            attempts=attempts,
            permanent=permanent,
            raw_message=message.body.decode("utf-8", errors="replace"),
            created_at=datetime.now(),
        )
    )
    await message.ack()


async def handle(
    channel: Channel,
    message: aio_pika.Message,
    retry_exchanges: dict[int, aio_pika.Exchange],
    failed_exchange: aio_pika.Exchange,
) -> None:
    """处理一条消息：幂等检查、调网关、临时错误转投重试、耗尽或永久错误归档。"""
    try:
        data = Result.model_validate(json.loads(message.body))
    except Exception:
        # 消息体本身不合法，重试一万次结果也一样：直接归档，不进重试。
        # request_id 尽力从原始体里取，取不到就空着
        try:
            request_id = json.loads(message.body).get("request_id", "")
        except Exception:
            request_id = ""
        await archive(
            channel,
            message,
            failed_exchange,
            request_id,
            reason="消息体解析失败",
            error_type="parse_error",
            attempts=0,
            permanent=True,
        )
        return

    # 幂等闸门：send_record 只在成功后写，这里拦下的必然是"已成功过"的重复提交；
    # 重试中的消息还没写 send_record，不会被误拦——这正是"先发送、后落库"的原因
    if await is_sent(data.request_id, data.channel):
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
        # 重复消息也必须处理完毕（留档 + ack），这条不能动；要改的是别闷声干：
        # 不打日志的话，重放/重投的消息被无声吞掉，看着就像消费者死了（这次就踩了）
        print(f"[{channel}] {data.request_id} 重复消息，已留档并确认")
        await message.ack()
        return

    try:
        await gateway.send(channel, data)
    except TemporaryError as e:
        # 已试几次（数 gateway_call 行数，含刚失败的这次）
        attempt = await call_count(data.request_id, data.channel)
        if attempt <= len(backoff_seconds):
            # 还有档位：发到对应档位的交换机（fanout 进队列不看 key）。
            # routing_key 必须原样保留业务 key：死信转发沿用的就是它，改了就回不了业务队列
            seconds = backoff_seconds[attempt - 1]
            await retry_exchanges[seconds].publish(
                aio_pika.Message(
                    body=message.body,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    priority=message.priority,  # 回 sms 队列时继续参与优先级排序
                ),
                routing_key=message.routing_key,
            )
            await message.ack()
            print(f"[{channel}] {data.request_id} 第 {attempt} 次尝试失败（临时），{seconds} 秒后重试：{e}")
            return
        # 档位用光：归档留证据，不再重试
        print(f"[{channel}] {data.request_id} 第 {attempt} 次尝试仍失败，重试耗尽，归档")
        await archive(
            channel,
            message,
            failed_exchange,
            data.request_id,
            reason=f"重试耗尽：{e}",
            error_type="temporary_error_exhausted",
            attempts=attempt,
            permanent=False,
        )
        return
    except PermanentError as e:
        # 永久错误不重试，直接归档
        print(f"[{channel}] {data.request_id} 永久错误，归档：{e}")
        await archive(
            channel,
            message,
            failed_exchange,
            data.request_id,
            reason=f"永久错误：{e}",
            error_type="permanent_error",
            attempts=0,
            permanent=True,
        )
        return

    # 成功才写 send_record：先发送、后落库
    await create_send_record(data)
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
    arguments = {"x-max-priority": 10} if channel == "sms" else None
    queue = await ch.declare_queue(channel, durable=True, arguments=arguments)
    # 转投用的交换机：重试每档一个（发到哪个档位就进哪个队列）、归档按通道进
    retry_exchanges = {
        seconds: await ch.get_exchange(f"ai_lab.notify.retry.{seconds}s")
        for seconds in backoff_seconds
    }
    failed_exchange = await ch.get_exchange("ai_lab.notify.failed")
    print(f"[{channel}] 开始消费，Ctrl+C 退出")
    async for message in queue.iterator():
        await handle(channel, message, retry_exchanges, failed_exchange)


async def main() -> None:
    """三个通道并发消费。"""
    async with connect() as (connection_1, _):
        await asyncio.gather(*(consume(ch, connection_1) for ch in CHANNELS))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
