import asyncio
import time

from sqlalchemy import select, update

from 锁教程.model import ProductSku, Session, reset_lab


async def reserve_once(name: str) -> str:
    async with Session() as session:
        sku = (await session.execute(select(ProductSku).where(ProductSku.id == 1))).scalar_one()
        await asyncio.sleep(1)
        if sku.stock < 1:
            return f"{name}: SOLD_OUT"
        result = await session.execute(
            update(ProductSku)
            .where(ProductSku.id == 1, ProductSku.version == sku.version)
            .values(stock=sku.stock - 1, version=sku.version + 1)
        )

        await session.commit()
        return f"{name}: SUCCESS" if result.rowcount == 1 else f"{name}: CONFLICT"


async def main():
    await reset_lab()
    prev = time.perf_counter()
    results = await asyncio.gather(reserve_once("请求A"), reserve_once("请求B"))
    print(f"耗时 {time.perf_counter() - prev:.2f}s")
    print(results)
    async with Session() as session:
        sku = await session.get(ProductSku, 1)
        print(f"终态: stock={sku.stock}, version={sku.version}")

if __name__ == "__main__":
    asyncio.run(main())