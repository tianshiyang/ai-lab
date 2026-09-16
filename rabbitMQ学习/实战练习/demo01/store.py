from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from rabbitMQ学习.实战练习.db import SessionLocal
from rabbitMQ学习.实战练习.demo01.typings import FailedResult, GatewayResult, Result
from rabbitMQ学习.实战练习.models import FailedRecord, GatewayCall, SendRecord


async def create_send_record(data: Result) -> bool:
    """创建发送成功留档"""
    async with SessionLocal() as session:
        # 表里没有 delay_seconds 列，落库时剔除
        stmt = (
            insert(SendRecord)
            .values(**data.model_dump(exclude={"delay_seconds"}))
            .on_conflict_do_nothing(index_elements=["request_id", "channel"])
        )
        result = await session.execute(stmt)
        await session.commit()
        return result.rowcount == 1


async def create_failed_record(data: FailedResult) -> FailedRecord:
    """创建最终失败留档"""
    async with SessionLocal() as session:
        record = FailedRecord(**data.model_dump())
        session.add(record)
        await session.commit()
        await session.refresh(record)
        return record


async def create_gateway_call(data: GatewayResult) -> GatewayCall:
    """创建网关调用流水"""
    async with SessionLocal() as session:
        record = GatewayCall(**data.model_dump())
        session.add(record)
        await session.commit()
        await session.refresh(record)
        return record


async def call_count(request_id: str, channel: str) -> int:
    """数这对 (request_id, channel) 已调过几次网关；行数即次数，天然跨进程重启。"""
    async with SessionLocal() as session:
        result = await session.execute(
            select(func.count())
            .select_from(GatewayCall)
            .where(GatewayCall.request_id == request_id, GatewayCall.channel == channel)
        )
        return int(result.scalar_one())


async def is_sent(request_id: str, channel: str) -> bool:
    """查这对 (request_id, channel) 是否已成功发送过（幂等闸门）。"""
    async with SessionLocal() as session:
        result = await session.execute(
            select(func.count())
            .select_from(SendRecord)
            .where(SendRecord.request_id == request_id, SendRecord.channel == channel)
        )
        return result.scalar_one() > 0
