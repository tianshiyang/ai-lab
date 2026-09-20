import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import String, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

load_dotenv(Path(__file__).parents[2] / ".env")
DATABASE_URL = os.environ["DATABASE_URL"]

engine = create_async_engine(DATABASE_URL)


class Base(DeclarativeBase):
    pass


class Refund(Base):
    __tablename__ = "refunds"

    refund_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    order_id: Mapped[str] = mapped_column(String(20))  # 关联的原订单号
    amount: Mapped[int]  # 退款金额(元)
    is_vip: Mapped[bool] = mapped_column(default=False)  # VIP 标记,提交端读它决定消息 priority
    status: Mapped[str] = mapped_column(
        default="待审核"
    )  # 待审核/待打款/已退款/已拒绝/已撤销/打款失败
    attempts: Mapped[int] = mapped_column(default=0)  # 打款尝试次数,每开始打一次款 +1
    reject_reason: Mapped[str | None]  # 审核拒绝原因,可空
    audit_will_reject: Mapped[bool] = mapped_column(default=False)  # 审核剧本开关,True=客服将拒绝
    pay_will_fail: Mapped[bool] = mapped_column(default=False)  # 打款剧本开关,True=每次打款都失败


@lru_cache(maxsize=1)
def get_session() -> async_sessionmaker[AsyncSession]:
    """获取session"""
    return async_sessionmaker(engine=engine, expire_on_commit=False)


@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession]:
    """获取数据库会话"""
    async with get_session()() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_db() -> None:
    """建表。create_all 幂等——和拓扑声明一个脾气,谁先启动谁建。"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def create_refunds(
    refund_id: str,
    order_id: str,
    amount: int,
    is_vip: bool,
    status: str,
    audit_will_reject: bool,
    pay_will_fail: bool,
) -> None:
    """创建退款"""
    async with get_db_session() as session:
        session.add(
            Refund(
                refund_id=refund_id,
                order_id=order_id,
                amount=amount,
                status=status,
                is_vip=is_vip,
                audit_will_reject=audit_will_reject,
                pay_will_fail=pay_will_fail,
            )
        )


async def update_refunds(refund_id: str, old_status: str, status: str):
    async with get_db_session() as session:
        stmt = (
            update(Refund)
            .where(Refund.refund_id == refund_id, Refund.status == old_status)
            .values(status=status)
        )
        result = await session.execute(stmt)
        return result.rowcount >= 1
