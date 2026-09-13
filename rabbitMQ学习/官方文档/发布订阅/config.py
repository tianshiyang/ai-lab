import os
from pathlib import Path
from typing import cast

import dotenv
import pika


project_root = Path(__file__).resolve().parents[3]
# 默认不覆盖终端里已有的环境变量；没有设置时才使用 .env 的值。
dotenv.load_dotenv(project_root / ".env")


mitt_connection = pika.BlockingConnection(
    pika.URLParameters(cast(str, cast(str, os.getenv("RABBITMQ_URL"))))
)
mitt_channel = mitt_connection.channel()
