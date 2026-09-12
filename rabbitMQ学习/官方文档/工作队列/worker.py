"""消费者（worker）：从 task_queue 取任务并处理。

工作队列的核心就在这：可以同时启动多个 worker 订阅同一条队列，
消息会被分摊给它们并行处理。worker 本身不感知彼此，大家只面对队列。
"""
import time

from rabbitMQ学习.官方文档.工作队列.work_config import work_channel


# 模拟耗时任务：消息里每个 "." 睡 1 秒，"task..." 就是干 3 秒活
def callback(ch, method, properties, body):
    print(f" [x] Received {body.decode()}")
    time.sleep(body.count(b"."))
    print(f"[x] Done")
    # 手动 ack：告诉 broker"这条消息处理完了，可以删了"。
    # delivery_tag 是这次投递在本 channel 上的编号（随消息一起送来，在 method 里）。
    # 消费者没 ack 就挂掉/断开时，broker 会把消息重新投递给其他 worker——消息不丢。
    # 注意：忘了写这行，消息会一直卡在 unacked 状态（broker 越占越多内存），也不会再投给任何人。
    # 排查命令：rabbitmqctl list_queues name messages_ready messages_unacknowledged
    ch.basic_ack(delivery_tag=method.delivery_tag)


# 公平分发：prefetch_count=1 —— 这个 worker 最多"预支"1 条未 ack 的消息，
# ack 上一条之前 broker 不会再派新活给它。
# 没有它，broker 只会机械地轮询：worker1 拿到耗时任务还没干完，新消息照样轮流塞给它，
# 而先干完的 worker2 只能干等着，分配完全看运气。
work_channel.basic_qos(prefetch_count=1)

# 不传 auto_ack（默认 False）就是手动 ack 模式：消费者必须自己调 basic_ack 确认
work_channel.basic_consume(queue="task_queue", on_message_callback=callback)

work_channel.start_consuming()
