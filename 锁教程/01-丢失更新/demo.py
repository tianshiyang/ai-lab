import asyncio

from sqlalchemy import update

from 锁教程.model import Session, Sku


async def unsafe_reserve() -> None:
    """错误示例"""
    async with Session() as session:
        sku = await session.get(Sku, 1)
        await asyncio.sleep(1)
        print(sku.stock)
        sku.stock -= 1
        await session.commit()


async def safe_reserve() -> None:
    """正确示例"""
    async with Session() as session:
        result = await session.execute(
            update(Sku)
            .where(Sku.id == 1, Sku.stock > 0)
            .values(stock=Sku.stock - 1)
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
