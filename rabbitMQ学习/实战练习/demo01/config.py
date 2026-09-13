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

