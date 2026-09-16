"""模拟调第三方网关：按 fail_rules 注入故障，成败都写一行 gateway_call。"""

import asyncio
from datetime import datetime

from rabbitMQ学习.实战练习.data.data import mock_config
from rabbitMQ学习.实战练习.demo01.store import call_count, create_gateway_call
from rabbitMQ学习.实战练习.demo01.typings import Channel, GatewayResult, Result


class TemporaryError(Exception):
    """临时错误：这次失败，隔一会儿再试可能就好（网关超时、连接被拒）。"""


class PermanentError(Exception):
    """永久错误：重试一万次结果也一样（数据本身的问题）。"""


async def send(channel: Channel, data: Result) -> None:
    """模拟调一次网关：sleep 出耗时，按 fail_rules 定成败，写流水，失败抛异常。"""
    send_seconds = mock_config["gateway"][channel]["send_seconds"]
    await asyncio.sleep(send_seconds)

    # 本次是第几次调用：数 gateway_call 行数，跨进程重启不丢
    attempt = await call_count(data.request_id, data.channel) + 1

    rule = next(
        (
            r
            for r in mock_config["fail_rules"]
            if r["request_id"] == data.request_id and r["channel"] == channel
        ),
        None,
    )
    fail = rule is not None and (rule["mode"] == "fail_always" or attempt <= rule["times"])

    error_type = error_msg = None
    if fail:
        # 规则里的 error 形如 "SMTPConnectError: connection refused"，冒号前是类型、后是详情
        error_type, _, error_msg = rule["error"].partition(":")
        error_type, error_msg = error_type.strip(), error_msg.strip()

    await create_gateway_call(
        GatewayResult(
            request_id=data.request_id,
            channel=channel,
            success=not fail,
            error_type=error_type,
            error_msg=error_msg,
            called_at=datetime.now(),
        )
    )
    if fail:
        raise TemporaryError(f"第 {attempt} 次调用失败: {rule['error']}")
