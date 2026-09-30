import asyncio
import time

from sqlalchemy import select
from sqlalchemy.exc import DBAPIError

from 锁教程.model import Order, Session, Sku, reset_lab


async def unsafe_reserve(name: str) -> str:
    """事故现场"""
    async with Session.begin() as session:
        sku = await session.get(Sku, 1)
        await asyncio.sleep(2)  # 模拟读写之间业务计算
        sku.stock -= 1
        await session.commit()
        return f"{name}: 读到 {sku.stock + 1}, 写成 {sku.stock}"


async def reserve_with_row_lock(name: str) -> str:
    """悲观锁 -> 等"""
    async with Session.begin() as session:
        sku = (
            await session.execute(select(Sku).where(Sku.id == 1).with_for_update())
        ).scalar_one()

        await asyncio.sleep(1)

        if sku.stock < 1:
            return f"{name}: 售罄"
        sku.stock -= 1
        return f"{name}: 扣减后 stock={sku.stock}"


async def reserve_with_row_lock_no_wait(name: str) -> str:
    """悲观锁 -> 不等"""
    try:
        async with Session.begin() as session:
            sku = (
                await session.execute(
                    select(Sku).where(Sku.id == 1).with_for_update(nowait=True)
                )
            ).scalar_one()
            await asyncio.sleep(1)
            sku.stock -= 1
            return f"{name}: 扣减后 stock={sku.stock}"
    except DBAPIError:
        return f"{name}: 锁被占用，立即失败"


async def claim_one(worker: str) -> str | None:
    """多worker领任务"""
    async with Session.begin() as session:
        order = (
            await session.execute(
                select(Order)
                .where(Order.status == "PENDING")
                .order_by(Order.id)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
        ).scalar_one_or_none()
        await asyncio.sleep(1)
        if order is None:
            return None

        order.status = "PROCESSING"
        return f"{worker} 领取 {order.id}"


async def main():
    await reset_lab()
    prev = time.perf_counter()
    # 事故现场
    # result = await asyncio.gather(unsafe_reserve("请求A"), unsafe_reserve("请求B"))
    # result = await asyncio.gather(reserve_with_row_lock("请求A"), reserve_with_row_lock("请求B"))
    # result = await asyncio.gather(
    #     reserve_with_row_lock_no_wait("请求A"), reserve_with_row_lock_no_wait("请求B")
    # )
    result = await asyncio.gather(claim_one("请求A"), claim_one("请求B"))
    print(f"执行了{time.perf_counter() - prev}s, 结果{result}")


if __name__ == "__main__":
    asyncio.run(main())
