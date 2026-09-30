import asyncio

from sqlalchemy import update

from 锁教程.model import Product, Session


async def unsafe_reserve() -> None:
    """错误示例"""
    async with Session() as session:
        product = await session.get(Product, 1)
        await asyncio.sleep(1)
        print(product.available)
        product.available -= 1
        await session.commit()


async def safe_reserve() -> None:
    """正确示例"""
    async with Session() as session:
        result = await session.execute(
            update(Product)
            .where(Product.id == 1, Product.available > 0)
            .values(available=Product.available - 1)
        )
        await session.commit()
        return result.rowcount == 1


async def main():
    """主函数"""
    # await asyncio.gather(unsafe_reserve(), unsafe_reserve())
    results = await asyncio.gather(safe_reserve(), safe_reserve())
    print(results)


if __name__ == "__main__":
    asyncio.run(main())
