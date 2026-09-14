from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

Channel = Literal["sms", "email", "inapp"]


class Request(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str
    note: str
    user_id: str
    template_id: str
    params: dict[str, str]
    delay_seconds: int


class Template(BaseModel):
    model_config = ConfigDict(extra="forbid")
    template_id: str
    biz_type: str
    priority: int
    allow_channels: list[Channel]
    title: str
    content: str


class User(BaseModel):
    model_config = ConfigDict(extra="forbid")
    user_id: str
    name: str
    phone: str
    email: str
    channels: list[Channel]


class Result(BaseModel):
    model_config = ConfigDict(extra="forbid")
    task_id: str
    request_id: str
    user_id: str
    channel: Channel
    title: str
    content: str
    biz_type: str
    priority: int
    delay_seconds: int


class FailedResult(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str
    channel: str | None
    reason: str
    error_type: str | None
    attempts: int
    permanent: bool
    raw_message: str | None
    created_at: datetime


class GatewayResult(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str
    channel: str
    success: bool
    error_type: str | None
    error_msg: str | None
    called_at: datetime
