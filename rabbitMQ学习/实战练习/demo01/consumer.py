import json
import time

from rabbitMQ学习.实战练习.data.data import mock_config
from rabbitMQ学习.实战练习.demo01.topology import channel_1


def callback_1(ch, method, properties, body):
    time.sleep(mock_config["gateway"]["sms"]["send_seconds"])
    print(body)
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel_1.basic_consume(queue="sms", on_message_callback=callback_1)

channel_1.start_consuming()