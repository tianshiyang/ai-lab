from pathlib import Path

import dotenv
import pika
import os
from typing import cast

project_root = Path(__file__).resolve().parents[3]
# 默认不覆盖终端里已有的环境变量；没有设置时才使用 .env 的值。
dotenv.load_dotenv(project_root / ".env")


hello_world_connection = pika.BlockingConnection(
    pika.URLParameters(cast(str, cast(str, os.getenv("RABBITMQ_URL"))))
)

hello_world_channel = hello_world_connection.channel()