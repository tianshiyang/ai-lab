"""RabbitMQ 连接配置。"""

import os

import pika
from dotenv import load_dotenv

load_dotenv()


def open_connection() -> pika.BlockingConnection:
    """创建一个用于短生命周期发布或 worker 的 AMQP 连接。"""
    url = os.environ["RABBITMQ_URL"]
    parameters = pika.URLParameters(url)
    parameters.heartbeat = 30
    parameters.blocked_connection_timeout = 30
    return pika.BlockingConnection(parameters)
