"""练习二的数据库读写：消费者当前用的四个函数。"""

from sqlalchemy import select

from rabbitMQ学习.实战练习.db import SessionLocal
from rabbitMQ学习.实战练习.demo01.typings import FailedResult, GatewayResult, Result
from rabbitMQ学习.实战练习.models import FailedRecord, GatewayCall, SendRecord


async def get_send_record(data: Result) -> SendRecord | None:
    """获取发送成功留档"""
    async with SessionLocal() as session:
        stmt = select(SendRecord).where(SendRecord.request_id == data.request_id)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()


async def create_send_record(data: Result) -> SendRecord:
    """创建发送成功留档"""
    async with SessionLocal() as session:
        # 表里没有 delay_seconds 列，落库时剔除
        record = SendRecord(**data.model_dump(exclude={"delay_seconds"}))
        session.add(record)
        await session.commit()
        await session.refresh(record)
        return record


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
