import json
import time

from rabbitMQ学习.实战练习.data.data import mock_config
from rabbitMQ学习.实战练习.demo01.topology import channel_1, connection_1

# 改这里切换通道：sms / email / inapp，开三个运行窗口各跑一个
CHANNEL = "sms"

# 模拟调用第三方网关的耗时
SEND_SECONDS = mock_config["gateway"][CHANNEL]["send_seconds"]


def callback_1(ch, method, properties, body):
    data = json.loads(body)

    # 模拟调网关发送
    time.sleep(SEND_SECONDS)

    print(f"[{CHANNEL}] {data['request_id']} {data['user_id']} {data['content']}")

    # 业务处理完成后才确认；中途退出的消息会被重新投递
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel_1.basic_consume(queue=CHANNEL, on_message_callback=callback_1)

print(f"[{CHANNEL}] 开始消费，停止按钮退出")

try:
    channel_1.start_consuming()
except KeyboardInterrupt:
    channel_1.stop_consuming()
    connection_1.close()
