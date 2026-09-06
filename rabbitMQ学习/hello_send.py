"""第一条消息：只用一个队列和默认交换机。"""

import argparse

import pika

from rabbitMQ学习.config import HELLO_QUEUE, open_connection


def main() -> None:
    """从命令行取一段文字，发布到 hello 队列后退出。"""
    parser = argparse.ArgumentParser(description="发送一条简单的文本消息")
    # nargs="?" 表示文字参数可省略；不传就使用 default。
    parser.add_argument("message", nargs="?", default="你好，RabbitMQ！")
    args = parser.parse_args()
    with open_connection() as connection:
        # 队列声明和消息发布都通过通道执行，with 结束时自动关闭连接。
        channel = connection.channel()
        # 队列不存在就创建；已存在则核对参数。同名、同参数可重复声明。
        # durable 保存队列定义；classic 是本教程使用的经典队列类型。
        channel.queue_declare(
            queue=HELLO_QUEUE, durable=True, arguments={"x-queue-type": "classic"}
        )
        channel.confirm_delivery()  # 等服务器确认发布结果，不是等消费者处理完成。
        channel.basic_publish(
            exchange="",  # 默认交换机：用队列名作为路由键。
            routing_key=HELLO_QUEUE,
            body=args.message.encode("utf-8"),  # 消息正文发送的是字节。
            properties=pika.BasicProperties(delivery_mode=2),  # 2 表示消息需要持久化。
            mandatory=True,  # 无匹配队列时退回；确认模式下 Pika 会抛出路由异常。
        )
    print(f"已发送：{args.message}")
    print(f"目标队列：{HELLO_QUEUE}")


if __name__ == "__main__":
    main()
