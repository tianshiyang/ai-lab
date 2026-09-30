import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import ForeignKey, String, delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

load_dotenv(Path(__file__).resolve().parents[1] / ".env")


class Base(DeclarativeBase):
    pass


class Product(Base):
    __tablename__ = "lock_lab_product"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(100))
    sold_count: Mapped[int] = mapped_column(default=0)  # 累计销量,下单成功 +1
    summary_done: Mapped[bool] = mapped_column(default=False)  # 每日汇总是否已执行


class Item(Base):
    __tablename__ = "lock_lab_item"

    id: Mapped[int] = mapped_column(primary_key=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("lock_lab_product.id"))
    spec: Mapped[str] = mapped_column(String(50))  # 班型,如 "weekend"


class Sku(Base):
    __tablename__ = "lock_lab_sku"

    id: Mapped[int] = mapped_column(primary_key=True)
    item_id: Mapped[int] = mapped_column(ForeignKey("lock_lab_item.id"))
    name: Mapped[str] = mapped_column(String(50))  # 票档,如 "early-bird"
    price: Mapped[int] = mapped_column()  # 单位: 分
    stock: Mapped[int] = mapped_column()
    version: Mapped[int] = mapped_column(default=0)


class Order(Base):
    __tablename__ = "lock_lab_order"

    id: Mapped[int] = mapped_column(primary_key=True)
    sku_id: Mapped[int] = mapped_column(ForeignKey("lock_lab_sku.id"))
    user_id: Mapped[str] = mapped_column(String(50))
    request_id: Mapped[str] = mapped_column(String(80), unique=True)
    status: Mapped[str] = mapped_column(String(20), default="PENDING")


engine = create_async_engine(os.environ["DATABASE_URL"])
Session = async_sessionmaker(engine, expire_on_commit=False)


async def reset_lab() -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    async with Session.begin() as session:
        await session.execute(delete(Order))
        await session.execute(delete(Sku))
        await session.execute(delete(Item))
        await session.execute(delete(Product))

        session.add(Product(id=1, title="python-async-course", sold_count=0, summary_done=False))
        await session.flush()  # 外键依赖父表:逐级落库,先插父行再插子行
        session.add(Item(id=1, product_id=1, spec="weekend"))
        await session.flush()
        session.add_all(
            [
                Sku(id=1, item_id=1, name="early-bird", price=49900, stock=3, version=0),
                Sku(id=2, item_id=1, name="standard", price=69900, stock=5, version=0),
            ]
        )
        await session.flush()
        session.add_all(
            Order(sku_id=1, user_id=f"u-{i}", request_id=f"seed-{i:03d}")
            for i in range(1, 6)
        )


if __name__ == "__main__":
    asyncio.run(reset_lab())
