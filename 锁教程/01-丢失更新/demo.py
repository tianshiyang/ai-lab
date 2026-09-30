import asyncio

from sqlalchemy import update

from 锁教程.model import ProductSku, Session, reset_lab


async def unsafe_reserve() -> None:
    """错误示例"""
    async with Session() as session:
        sku = await session.get(ProductSku, 1)
        await asyncio.sleep(1)
        print(sku.stock)
        sku.stock -= 1
        await session.commit()


async def safe_reserve() -> None:
    """正确示例"""
    async with Session() as session:
        result = await session.execute(
            update(ProductSku)
            .where(ProductSku.id == 1, ProductSku.stock > 0)
            .values(stock=ProductSku.stock - 1)
        )
        await session.commit()
        return result.rowcount == 1


async def main():
    """主函数"""
    await reset_lab()
    # await asyncio.gather(unsafe_reserve(), unsafe_reserve())
    results = await asyncio.gather(safe_reserve(), safe_reserve())
    print(results)


if __name__ == "__main__":
    asyncio.run(main())
