"""生产者：发布任务消息。

用法：uv run python -m rabbitMQ学习.官方文档.工作队列.new_task 任务内容
消息里每个 "." 代表 1 秒工作量（worker 那边按点的个数 sleep），用来模拟耗时任务。
"""
import sys

import pika

from rabbitMQ学习.官方文档.工作队列.work_config import work_channel

# 从命令行参数拼出消息，没传就用默认值
message = " ".join(sys.argv[1:]) or "Hello World!"

# properties 是消息的"元数据"：
#   delivery_mode=Persistent —— 把消息标记为持久化（写盘），配合 durable 队列，
#   RabbitMQ 重启后消息仍然在；Transient(1) 则是纯内存、重启即失。
#   （quorum 队列的消息本来就总是持久化的，这里照官方教程的写法保留，不影响结果）
work_channel.basic_publish(
    exchange="",
    routing_key="task_queue",
    body=message,
    properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
)

print(f"[x] Sent {message}")
work_channel.close()
