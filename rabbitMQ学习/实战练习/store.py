"""数据库操作封装：消费者和 producer 要用的读写都收在这，全是 async 函数。

函数只做"一条 SQL 能说清的事"；业务判断（重试几档、要不要进归档）留在调用方。
消费者是 aio-pika 全异步，直接 await 调用。
直接运行是自检：uv run python -m rabbitMQ学习.实战练习.store
"""

import asyncio

from sqlalchemy import delete, func, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from rabbitMQ学习.实战练习.db import SessionLocal
from rabbitMQ学习.实战练习.models import FailedRecord, GatewayCall, SendRecord

# 自检用的请求 ID，跑完即删，不跟真实数据混
_SELFTEST_REQUEST_ID = "SELFTEST-REQ-1"


async def is_sent(request_id: str, channel: str) -> bool:
    """查询这对 (request_id, channel) 是否已发送成功。"""
    async with SessionLocal() as session:
        stmt = (
            select(func.count())
            .select_from(SendRecord)
            .where(SendRecord.request_id == request_id, SendRecord.channel == channel)
        )
        return (await session.scalar(stmt) or 0) > 0


async def mark_sent(task: dict, consumer_id: str | None = None) -> bool:
    """写入发送成功留档，返回是否真的插进去了（False=重复，被唯一约束拦下）。"""
    async with SessionLocal() as session:
        async with session.begin():
            stmt = (
                pg_insert(SendRecord)
                .values(
                    task_id=task["task_id"],
                    request_id=task["request_id"],
                    user_id=task["user_id"],
                    channel=task["channel"],
                    title=task["title"],
                    content=task["content"],
                    biz_type=task["biz_type"],
                    priority=task["priority"],
                    consumer_id=consumer_id,
                )
                .on_conflict_do_nothing(index_elements=["request_id", "channel"])
            )
            result = await session.execute(stmt)
            return result.rowcount == 1


async def log_call(
    request_id: str,
    channel: str,
    success: bool,
    error_type: str | None = None,
    error_msg: str | None = None,
) -> int:
    """写入一行网关调用流水，返回这对 (request_id, channel) 的累计调用次数。"""
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(
                insert(GatewayCall).values(
                    request_id=request_id,
                    channel=channel,
                    success=success,
                    error_type=error_type,
                    error_msg=error_msg,
                )
            )
            stmt = (
                select(func.count())
                .select_from(GatewayCall)
                .where(GatewayCall.request_id == request_id, GatewayCall.channel == channel)
            )
            return await session.scalar(stmt) or 0


async def call_count(request_id: str, channel: str) -> int:
    """统计这对 (request_id, channel) 已调用过几次网关。"""
    async with SessionLocal() as session:
        stmt = (
            select(func.count())
            .select_from(GatewayCall)
            .where(GatewayCall.request_id == request_id, GatewayCall.channel == channel)
        )
        return await session.scalar(stmt) or 0


async def mark_failed(
    request_id: str,
    channel: str | None,
    reason: str,
    attempts: int,
    permanent: bool,
    raw_message: dict | None = None,
) -> None:
    """写入最终失败留档（永久错误或重试耗尽）。"""
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(
                insert(FailedRecord).values(
                    request_id=request_id,
                    channel=channel,
                    reason=reason,
                    attempts=attempts,
                    permanent=permanent,
                    raw_message=raw_message,
                )
            )


async def _cleanup_selftest() -> None:
    """清理自检数据，保证重复跑结果一致。"""
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(
                delete(GatewayCall).where(GatewayCall.request_id == _SELFTEST_REQUEST_ID)
            )
            await session.execute(
                delete(SendRecord).where(SendRecord.request_id == _SELFTEST_REQUEST_ID)
            )
            await session.execute(
                delete(FailedRecord).where(FailedRecord.request_id == _SELFTEST_REQUEST_ID)
            )


async def _selftest() -> None:
    """跑一遍五个函数，验证幂等和计数是否符合预期。"""
    task = {
        "task_id": "selftest-task-1",
        "request_id": _SELFTEST_REQUEST_ID,
        "user_id": "U001",
        "channel": "sms",
        "title": "自检",
        "content": "自检消息",
        "biz_type": "verify_code",
        "priority": 9,
    }

    await _cleanup_selftest()
    print("is_sent 首次（应 False）:", await is_sent(_SELFTEST_REQUEST_ID, "sms"))
    print("mark_sent 第一次（应 True）:", await mark_sent(task, consumer_id="test"))
    print("mark_sent 重复（应 False）:", await mark_sent(task))
    print("is_sent 再查（应 True）:", await is_sent(_SELFTEST_REQUEST_ID, "sms"))
    print(
        "log_call 第 1 次（应 1）:",
        await log_call(_SELFTEST_REQUEST_ID, "sms", success=True),
    )
    print(
        "log_call 第 2 次（应 2）:",
        await log_call(_SELFTEST_REQUEST_ID, "sms", success=False, error_type="GatewayTimeout"),
    )
    print("call_count（应 2）:", await call_count(_SELFTEST_REQUEST_ID, "sms"))
    await mark_failed(
        _SELFTEST_REQUEST_ID,
        "sms",
        "自检：模拟重试耗尽",
        attempts=2,
        permanent=False,
        raw_message=task,
    )
    await _cleanup_selftest()
    print("自检完成，测试数据已清理")


if __name__ == "__main__":
    asyncio.run(_selftest())
