"""接收文本消息，打印后确认。"""

from rabbitMQ学习.config import open_connection, resource_prefix


def main() -> None:
    """持续接收文本；每条打印完成后再确认，Ctrl+C 退出。"""
    with open_connection() as connection:
        channel = connection.channel()
        # 接收端也声明相同队列，因此可以先启动接收端，再启动发送端。
        channel.queue_declare(
            queue=f"{resource_prefix}.hello.q", durable=True, arguments={"x-queue-type": "classic"}
        )
        channel.basic_qos(prefetch_count=1)  # 最多持有一条尚未确认的消息。

        def on_message(ch, method, properties, body):
            """Pika 每收到一条消息就调用此函数；body 是正文，properties 是附加属性。"""
            # decode 把字节还原成文字；flush=True 让终端立即显示，便于观察。
            print(f"收到：{body.decode('utf-8', errors='replace')}", flush=True)
            # delivery_tag 是本次投递在当前通道的编号，必须通过原通道确认。
            ch.basic_ack(delivery_tag=method.delivery_tag)

        # 注册订阅和回调；auto_ack=False 要求业务代码主动发送 ack。
        channel.basic_consume(
            queue=f"{resource_prefix}.hello.q", on_message_callback=on_message, auto_ack=False
        )
        print(f"等待 {resource_prefix}.hello.q 的消息，按 Ctrl+C 退出。", flush=True)
        try:
            channel.start_consuming()  # 进入接收循环；没有消息时等待，不是程序卡住。
        except KeyboardInterrupt:
            print("\n已停止接收。")


if __name__ == "__main__":
    main()
