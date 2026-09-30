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
    sku: Mapped[str] = mapped_column(String(50), unique=True)
    available: Mapped[int]
    version: Mapped[int] = mapped_column(default=0)


class Order(Base):
    __tablename__ = "lock_lab_order"

    id: Mapped[int] = mapped_column(primary_key=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("lock_lab_product.id"))
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
        await session.execute(delete(Product))
        session.add(Product(id=1, sku="python-async", available=3, version=0))
        await session.flush()  # 先落库商品行，订单的外键才有父行可指
        session.add_all(
            Order(product_id=1, user_id=f"u-{i}", request_id=f"seed-{i:03d}")
            for i in range(1, 6)
        )


if __name__ == "__main__":
    asyncio.run(reset_lab())
