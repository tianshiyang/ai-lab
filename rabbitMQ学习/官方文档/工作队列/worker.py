import time

from rabbitMQ学习.官方文档.工作队列.work_config import work_channel


def callback(ch, method, properties, body):
    print(f" [x] Received {body.decode()}")
    time.sleep(body.count(b"."))
    print(f"[x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


work_channel.basic_qos(prefetch_count=1)
work_channel.basic_consume(queue="task_queue", on_message_callback=callback)

work_channel.start_consuming()