import os
from pathlib import Path
from typing import cast

import dotenv
import pika

# 加载环境变量
project_root = Path(__file__).resolve().parents[3]
dotenv.load_dotenv(project_root / ".env")

# 声明Connection
connection_1 = pika.BlockingConnection(pika.URLParameters(cast(str, os.getenv("RABBITMQ_URL"))))

# 声明channel
channel_1 = connection_1.channel()

# 声明交换机
channel_1.exchange_declare(exchange="ai_lab.notify.events", exchange_type="topic", durable=True)

# 声明队列
channel_1.queue_declare(queue="sms", durable=True)
channel_1.queue_declare(queue="email", durable=True)
channel_1.queue_declare(queue="inapp", durable=True)

# 声明绑定
channel_1.queue_bind(queue="sms", exchange="ai_lab.notify.events", routing_key="notify.*.sms")
channel_1.queue_bind(queue="email", exchange="ai_lab.notify.events", routing_key="notify.*.email")
channel_1.queue_bind(queue="inapp", exchange="ai_lab.notify.events", routing_key="notify.*.inapp")