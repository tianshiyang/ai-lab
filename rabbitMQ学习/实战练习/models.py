"""表结构声明：三张 notify_ 表的 ORM 模型。

只有"表长什么样"；怎么连库在 db.py，怎么读写在 store.py。
改了模型记得走 alembic：revision --autogenerate → 人工审 → upgrade head。
"""

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """所有模型的基类"""


class SendRecord(Base):
    """发送成功留档。UNIQUE(request_id, channel) 就是幂等的抓手"""

    __tablename__ = "notify_send_record"
    __table_args__ = (UniqueConstraint("request_id", "channel", name="uq_send_request_channel"),)

    task_id: Mapped[str] = mapped_column(String(64), primary_key=True)  # 消息里的 task_id
    request_id: Mapped[str] = mapped_column(String(32), index=True)  # 请求 ID
    user_id: Mapped[str] = mapped_column(String(16))  # 用户 ID
    channel: Mapped[str] = mapped_column(String(8), index=True)  # 通道
    title: Mapped[str] = mapped_column(String(128))  # 标题
    content: Mapped[str] = mapped_column(String(512))  # 渲染后的正文
    biz_type: Mapped[str] = mapped_column(String(32))  # 业务类型
    priority: Mapped[int] = mapped_column(Integer)  # 优先级
    consumer_id: Mapped[str | None] = mapped_column(
        String(16), default=None
    )  # 练习四：哪个实例发的
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )  # 发送时间


class GatewayCall(Base):
    """网关调用流水。一行 = 真的调了一次网关；行数 = 尝试次数，跨重启天然成立"""

    __tablename__ = "notify_gateway_call"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    request_id: Mapped[str] = mapped_column(String(32), index=True)  # 请求 ID
    channel: Mapped[str] = mapped_column(String(8), index=True)  # 通道
    success: Mapped[bool] = mapped_column(Boolean)  # 这次调用成功了吗
    error_type: Mapped[str | None] = mapped_column(String(64), default=None)  # 失败时的错误类型
    error_msg: Mapped[str | None] = mapped_column(String(256), default=None)  # 失败时的错误信息
    called_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )  # 调用时刻，练习四的限速统计就查它


class FailedRecord(Base):
    """最终失败留档：永久错误（attempts=0）或重试耗尽"""

    __tablename__ = "notify_failed_record"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    request_id: Mapped[str] = mapped_column(String(32), index=True)  # 请求 ID
    channel: Mapped[str | None] = mapped_column(
        String(8), default=None
    )  # 永久错误发生在进队列前，可能没有通道
    reason: Mapped[str] = mapped_column(String(64))  # 一句话原因，如"模板不存在"
    error_type: Mapped[str | None] = mapped_column(String(64), default=None)  # 错误类型
    attempts: Mapped[int] = mapped_column(Integer)  # 尝试了几次；永久错误为 0
    permanent: Mapped[bool] = mapped_column(Boolean)  # True=永久错误，False=重试耗尽
    raw_message: Mapped[dict | None] = mapped_column(
        JSONB, default=None
    )  # 练习三：原始消息体，留证据
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )  # 失败时间
