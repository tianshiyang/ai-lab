"""工作队列的公共初始化：连接 + channel + 声明 task_queue。"""
import os
from pathlib import Path
from typing import cast

import dotenv
import pika


project_root = Path(__file__).resolve().parents[3]
# 默认不覆盖终端里已有的环境变量；没有设置时才使用 .env 的值。
dotenv.load_dotenv(project_root / ".env")

work_connection = pika.BlockingConnection(pika.URLParameters(cast(str, os.getenv("RABBITMQ_URL"))))

work_channel = work_connection.channel()

# 队列声明放在公共 config 里：生产者(new_task)和消费者(worker)都 import 这份配置，
# 天然保证两边声明的参数完全一致（声明是幂等的，重复执行无害；参数不一致 broker 会直接报错）。
channel = work_channel.queue_declare(
    queue="task_queue", durable=True, arguments={"x-queue-type": "quorum"}
)
# queue_declare 返回 Queue.DeclareOk，能拿到队列名、积压消息数、在线消费者数，
# 这里只是接住返回值，没用到。
