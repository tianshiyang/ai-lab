import asyncio
import time

from sqlalchemy import select

from 锁教程.model import Order, Product, Session


async def reserve_with_row_lock():
    """乐观锁 -> 等"""
    async with Session.begin() as session:
        product = (
            await session.execute(select(Product).where(Product.id == 1).with_for_update())
        ).scalar_one()

        await asyncio.sleep(2)

        if product.available < 1:
            return False
        product.available -= 1
        return True


async def reserve_with_row_lock_no_wait():
    """乐观锁 -> 不等"""
    async with Session.begin() as session:
        product = (
            await session.execute(
                select(Product).where(Product.id == 1).with_for_update(nowait=True)
            )
        ).scalar_one()

        if product.available < 1:
            return False
        product.available -= 1
        return True


async def claim_one(worker_id: str) -> int | None:
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
        if order is None:
            return None

        order.status = "PROCESSING"
        print(worker_id, "领取", order.id)
        return order.id


async def main():
    prev = time.perf_counter()
    # result = await asyncio.gather(reserve_with_row_lock(), reserve_with_row_lock())
    result = await asyncio.gather(reserve_with_row_lock_no_wait(), reserve_with_row_lock_no_wait())
    print(f"执行了{time.perf_counter() - prev}s, 结果{result}")


if __name__ == "__main__":
    asyncio.run(main())
