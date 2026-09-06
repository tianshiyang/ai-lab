"""接收文本消息，打印后确认。"""

from rabbitMQ学习.config import HELLO_QUEUE, open_connection


def main() -> None:
    with open_connection() as connection:
        channel = connection.channel()
        channel.queue_declare(
            queue=HELLO_QUEUE, durable=True, arguments={"x-queue-type": "classic"}
        )
        channel.basic_qos(prefetch_count=1)

        def on_message(ch, method, properties, body):
            print(f"收到：{body.decode('utf-8', errors='replace')}", flush=True)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_consume(queue=HELLO_QUEUE, on_message_callback=on_message, auto_ack=False)
        print(f"等待 {HELLO_QUEUE} 的消息，按 Ctrl+C 退出。", flush=True)
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            print("\n已停止接收。")


if __name__ == "__main__":
    main()
