import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import String, Update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

load_dotenv(Path(__file__).parents[2] / ".env")
DATABASE_URL = os.environ["DATABASE_URL"]


class Base(DeclarativeBase):
    pass


class Order(Base):
    __tablename__ = "orders"

    order_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    amount: Mapped[int]
    status: Mapped[str] = mapped_column(default="待付款")
    cancel_reason: Mapped[str | None]
    points_granted: Mapped[bool] = mapped_column(default=False)  # 积分服务的幂等标记
    sms_will_fail: Mapped[bool] = mapped_column(default=False)  # 演示剧本开关


engin = create_async_engine(DATABASE_URL)
Session = async_sessionmaker(engin, expire_on_commit=False)


async def init_db():
    async with engin.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get(order_id: str) -> Order | None:
    async with Session() as session:
        return await session.get(Order, order_id)


async def create(order_id: str, amount: int, sms_will_fail: bool) -> None:
    async with Session() as session:
        session.add(Order(order_id=order_id, amount=amount, sms_will_fail=sms_will_fail))
        await session.commit()


async def mark_paid(order_id: str) -> None:
    async with Session() as session:
        await session.execute(
            Update(Order).where(Order.order_id == order_id).values(status="已付款")
        )
        await session.commit()


async def grant_points(order_id: str) -> bool:
    async with Session() as session:
        result = await session.execute(
            Update(Order)
            .where(Order.order_id == order_id, Order.points_granted.is_(False))
            .values(points_granted=True)
        )
        await session.commit()
        return result.rowcount == 1


async def cancel(order_id: str, reason: str) -> None:
    async with Session() as session:
        await session.execute(
            Update(Order)
            .where(Order.order_id == order_id)
            .values(status="已取消", cancel_reason=reason)
        )
        await session.commit()
