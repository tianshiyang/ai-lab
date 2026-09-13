import os
from pathlib import Path
from typing import cast

import dotenv
import pika

project_root = Path(__file__).resolve().parents[3]
# 默认不覆盖终端里已有的环境变量；没有设置时才使用 .env 的值。
dotenv.load_dotenv(project_root / ".env")

# 声明connection
router_connection = pika.BlockingConnection(
    pika.URLParameters(cast(str, os.getenv("RABBITMQ_URL")))
)

# 声明channel
router_channel = router_connection.channel()

# 声明交换机
router_channel.exchange_declare(
    exchange="direct_logs",
    exchange_type="direct",
)

# 声明队列
result = router_channel.queue_declare(queue="", exclusive=True)

router_queue_name = result.method.queue

