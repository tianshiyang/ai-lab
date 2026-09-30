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
    __table_args__ = {"comment": "商品表"}

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(100), comment="商品名称")
    sold_count: Mapped[int] = mapped_column(default=0, comment="累计销量,下单成功加购买数量")
    summary_done: Mapped[bool] = mapped_column(default=False, comment="每日汇总是否已执行")


class ProductSku(Base):
    __tablename__ = "lock_lab_product_sku"
    __table_args__ = {"comment": "商品SKU表(规格单位,库存挂在这层)"}

    id: Mapped[int] = mapped_column(primary_key=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("lock_lab_product.id"), comment="所属商品 id")
    name: Mapped[str] = mapped_column(String(50), comment="规格名,如 early-bird / standard")
    price: Mapped[int] = mapped_column(comment="售价,单位: 分")
    stock: Mapped[int] = mapped_column(comment="剩余库存")
    version: Mapped[int] = mapped_column(default=0, comment="乐观锁版本号")


class Order(Base):
    __tablename__ = "lock_lab_order"
    __table_args__ = {"comment": "订单表(订单头)"}

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[str] = mapped_column(String(50), comment="下单用户标识")
    request_id: Mapped[str] = mapped_column(String(80), unique=True, comment="业务请求唯一标识,幂等键")
    total_amount: Mapped[int] = mapped_column(comment="订单总金额,单位: 分")
    status: Mapped[str] = mapped_column(String(20), default="PENDING", comment="PENDING/PROCESSING/DONE")


class OrderItem(Base):
    __tablename__ = "lock_lab_order_item"
    __table_args__ = {"comment": "订单明细表(订单行)"}

    id: Mapped[int] = mapped_column(primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("lock_lab_order.id"), comment="所属订单 id")
    sku_id: Mapped[int] = mapped_column(ForeignKey("lock_lab_product_sku.id"), comment="所购 SKU id")
    quantity: Mapped[int] = mapped_column(comment="购买数量")
    unit_price: Mapped[int] = mapped_column(comment="下单时单价快照,单位: 分")


engine = create_async_engine(os.environ["DATABASE_URL"])
Session = async_sessionmaker(engine, expire_on_commit=False)


async def reset_lab() -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    async with Session.begin() as session:
        await session.execute(delete(OrderItem))
        await session.execute(delete(Order))
        await session.execute(delete(ProductSku))
        await session.execute(delete(Product))

        session.add(Product(id=1, title="python-async-course", sold_count=0, summary_done=False))
        await session.flush()  # 外键依赖父表:逐级落库,先插父行再插子行
        session.add_all(
            [
                ProductSku(id=1, product_id=1, name="early-bird", price=49900, stock=3, version=0),
                ProductSku(id=2, product_id=1, name="standard", price=69900, stock=5, version=0),
            ]
        )
        await session.flush()
        orders = [
            Order(user_id=f"u-{i}", request_id=f"seed-{i:03d}", total_amount=49900)
            for i in range(1, 6)
        ]
        session.add_all(orders)  # 5 笔已支付待处理的订单,第 2 讲 SKIP LOCKED 的队列
        await session.flush()
        session.add_all(
            OrderItem(order_id=order.id, sku_id=1, quantity=1, unit_price=49900)
            for order in orders
        )


if __name__ == "__main__":
    asyncio.run(reset_lab())
