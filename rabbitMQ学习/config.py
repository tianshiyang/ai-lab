"""读取项目根目录的 .env，创建 RabbitMQ 连接。"""

import os
from pathlib import Path
from typing import cast

import pika
from dotenv import load_dotenv

# __file__ 是当前文件路径；向上两级找到项目根目录，避免依赖终端的所在目录。
ROOT = Path(__file__).resolve().parents[1]
# 默认不覆盖终端里已有的环境变量；没有设置时才使用 .env 的值。
load_dotenv(ROOT / ".env")
# 前缀只用于区分练习资源，不是 RabbitMQ 的权限隔离；权限隔离由 vhost 管理。
PREFIX = os.getenv("RABBITMQ_PREFIX", "ai_lab.learn")
HELLO_QUEUE = f"{PREFIX}.hello.q"


def open_connection() -> pika.BlockingConnection:
    """限制连接等待时间，不在日志里输出包含密码的 URL。"""
    url = os.getenv("RABBITMQ_URL")
    # cast 只帮助类型检查，不会补出缺失的值，因此先做运行时检查。
    if not url:
        raise ValueError("请在项目根目录 .env 中填写 RABBITMQ_URL。")
    # 将 amqp://用户名:密码@地址:端口/vhost 解析成 Pika 的连接参数。
    parameters = pika.URLParameters(cast(str, url))
    parameters.heartbeat = 60  # 心跳协商值（秒），帮助检测断开的连接。
    parameters.socket_timeout = 5  # 建立底层网络连接的超时秒数。
    parameters.stack_timeout = 10  # 整个连接建立过程（含协议握手）的超时秒数。
    parameters.connection_attempts = 1  # 只尝试一次，失败后交给调用方处理。
    parameters.blocked_connection_timeout = 15  # 服务器因资源告警阻塞连接的最长等待。
    # BlockingConnection 会等待连接建立成功才返回；连接失败会抛出异常。
    return pika.BlockingConnection(parameters)
