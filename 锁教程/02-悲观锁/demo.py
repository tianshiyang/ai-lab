import asyncio
import time

from sqlalchemy import select
from sqlalchemy.exc import DBAPIError

from 锁教程.model import Order, OrderItem, Product, ProductSku, Session, reset_lab


async def unsafe_reserve(name: str) -> str:
    """事故现场"""
    async with Session.begin() as session:
        sku = await session.get(ProductSku, 1)
        await asyncio.sleep(2)  # 模拟读写之间业务计算
        sku.stock -= 1
        await session.commit()
        return f"{name}: 读到 {sku.stock + 1}, 写成 {sku.stock}"


async def reserve_with_row_lock(name: str) -> str:
    """悲观锁 -> 等"""
    async with Session.begin() as session:
        sku = (
            await session.execute(select(ProductSku).where(ProductSku.id == 1).with_for_update())
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
                    select(ProductSku).where(ProductSku.id == 1).with_for_update(nowait=True)
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


async def place_order(request_id: str) -> str:
    async with Session.begin() as session:
        product = await session.scalar(select(Product).where(Product.id == 1).with_for_update())
        sku = await session.scalar(select(ProductSku).where(ProductSku.id == 1).with_for_update())
        if sku.stock < 1:
            return "SOLD_OUT"
        sku.stock -= 1
        product.sold_count += 1
        order = Order(user_id="u-1", request_id=request_id, total_amount=sku.price)
        session.add(order)
        await session.flush()
        session.add(OrderItem(order_id=order.id, sku_id=sku.id, quantity=1, unit_price=sku.price))
    return "SUCCESS"


async def biz(name: str, sku_first: bool) -> str:
    try:
        async with Session.begin() as session:
            lock_sku = select(ProductSku).where(ProductSku.id == 1).with_for_update()
            lock_product = select(Product).where(Product.id == 1).with_for_update()
            if sku_first:
                await session.execute(lock_sku)
                await asyncio.sleep(0.3)
                await session.execute(lock_product)
            else:
                await session.execute(lock_product)
                await asyncio.sleep(0.3)
                await session.execute(lock_sku)
            return f"{name}: 两行都锁到手"
    except DBAPIError as e:
        return f"{name}: 数据库杀掉了我({type(e.orig.__cause__).__name__})"


async def main():
    await reset_lab()
    prev = time.perf_counter()
    # 事故现场
    # result = await asyncio.gather(unsafe_reserve("请求A"), unsafe_reserve("请求B"))
    # result = await asyncio.gather(reserve_with_row_lock("请求A"), reserve_with_row_lock("请求B"))
    # result = await asyncio.gather(
    #     reserve_with_row_lock_no_wait("请求A"), reserve_with_row_lock_no_wait("请求B")
    # )
    # result = await asyncio.gather(claim_one("请求A"), claim_one("请求B"))
    # result = await asyncio.gather(place_order("请求A"), place_order("请求B"))
    result = await asyncio.gather(
        biz("事务1(先锁商品)", False),
        biz("事务2(先锁SKU)", True),
    )
    # result = await asyncio.gather(
    #     biz("事务1(先锁商品)", False),
    #     biz("事务2(先锁商品)", False),
    # )
    print(f"执行了{time.perf_counter() - prev}s, 结果{result}")


if __name__ == "__main__":
    asyncio.run(main())
