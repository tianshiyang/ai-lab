"""读取项目根目录的 .env，创建 RabbitMQ 连接。"""

import os
from pathlib import Path

import pika
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
PREFIX = os.getenv("RABBITMQ_PREFIX", "ai_lab.learn")
HELLO_QUEUE = f"{PREFIX}.hello.q"


def open_connection() -> pika.BlockingConnection:
    """限制连接等待时间，不在日志里输出包含密码的 URL。"""
    url = os.getenv("RABBITMQ_URL")
    if not url:
        raise ValueError("请在项目根目录 .env 中设置 RABBITMQ_URL，见 README。")
    parameters = pika.URLParameters(url)
    parameters.heartbeat = 60
    parameters.socket_timeout = 5
    parameters.stack_timeout = 10
    parameters.connection_attempts = 1
    parameters.blocked_connection_timeout = 15
    return pika.BlockingConnection(parameters)
