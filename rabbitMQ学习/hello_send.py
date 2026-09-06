"""第一条消息：只用一个队列和默认交换机。"""

import pika

from rabbitMQ学习.config import open_connection, resource_prefix

# 练习时只改这段文字，然后在 PyCharm 点运行；不需要填写启动参数。
message_text = "你好，RabbitMQ！"


def main() -> None:
    """读取上方 message_text，发布到 hello 队列后退出。"""
    with open_connection() as connection:
        # 队列声明和消息发布都通过通道执行，with 结束时自动关闭连接。
        channel = connection.channel()
        # 队列不存在就创建；已存在则核对参数。同名、同参数可重复声明。
        # durable 保存队列定义；classic 是本教程使用的经典队列类型。
        channel.queue_declare(
            queue=f"{resource_prefix}.hello.q", durable=True, arguments={"x-queue-type": "classic"}
        )
        channel.confirm_delivery()  # 等服务器确认发布结果，不是等消费者处理完成。
        channel.basic_publish(
            exchange="",  # 默认交换机：用队列名作为路由键。
            routing_key=f"{resource_prefix}.hello.q",
            body=message_text.encode("utf-8"),  # 消息正文发送的是字节。
            properties=pika.BasicProperties(delivery_mode=2),  # 2 表示消息需要持久化。
            mandatory=True,  # 无匹配队列时退回；确认模式下 Pika 会抛出路由异常。
        )
    print(f"已发送：{message_text}")
    print(f"目标队列：{resource_prefix}.hello.q")


if __name__ == "__main__":
    main()
