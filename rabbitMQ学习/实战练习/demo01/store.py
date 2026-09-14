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
