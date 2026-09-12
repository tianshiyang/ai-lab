import sys

import pika

from rabbitMQ学习.官方文档.工作队列.work_config import work_channel

message = " ".join(sys.argv[1:]) or "Hello World!"

work_channel.basic_publish(
    exchange="",
    routing_key="task_queue",
    body=message,
    properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
)

print(f"[x] Sent {message}")
work_channel.close()