"""第一条消息：只用一个队列和默认交换机。"""

import argparse

import pika

from rabbitMQ学习.config import HELLO_QUEUE, open_connection


def main() -> None:
    parser = argparse.ArgumentParser(description="发送一条简单的文本消息")
    parser.add_argument("message", nargs="?", default="你好，RabbitMQ！")
    args = parser.parse_args()
    with open_connection() as connection:
        channel = connection.channel()
        channel.queue_declare(
            queue=HELLO_QUEUE, durable=True, arguments={"x-queue-type": "classic"}
        )
        channel.confirm_delivery()
        channel.basic_publish(
            exchange="",  # 默认交换机：用队列名作为路由键。
            routing_key=HELLO_QUEUE,
            body=args.message.encode("utf-8"),
            properties=pika.BasicProperties(delivery_mode=2),
            mandatory=True,
        )
    print(f"已发送：{args.message}")
    print(f"目标队列：{HELLO_QUEUE}")


if __name__ == "__main__":
    main()
