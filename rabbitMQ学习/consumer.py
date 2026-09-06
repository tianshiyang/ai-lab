"""完成模拟业务后手动确认；邮件处理失败时拒绝并进入死信流程。"""

import pika
from pika.adapters.blocking_connection import BlockingChannel

from rabbitMQ学习.config import open_connection
from rabbitMQ学习.order_service import handle_order_event, parse_order_event
from rabbitMQ学习.topology import ORDER_EMAIL_QUEUE, ORDER_POINTS_QUEUE

# PyCharm 练习设置：修改并保存后重新运行。已启动的进程不会自动读取修改。
QUEUE_KIND = "email"  # email 处理邮件队列；points 处理积分队列。
DELAY_SECONDS = 0  # 改成 20 就模拟处理 20 秒，方便观察 Unacked。
STOP_AFTER_ONE = False  # True 处理一条就退出；False 持续等待。空队列时两者都等待。


def consume(*, queue_kind: str = "email", delay: float = 0, once: bool = False) -> None:
    """消费指定业务队列；delay 模拟耗时，once 表示处理一条后停止。"""
    if queue_kind not in ("email", "points"):
        raise ValueError('QUEUE_KIND 只能是 "email" 或 "points"。')
    if not 0 <= delay <= 60:
        raise ValueError("DELAY_SECONDS 必须在 0～60 秒之间。")
    queue_name = ORDER_EMAIL_QUEUE if queue_kind == "email" else ORDER_POINTS_QUEUE
    with open_connection() as connection:
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)  # 限制未确认数量，不是启动一个工作线程。

        # 回调放在这里，可以使用本次连接 connection 和本次设置 delay、once。
        # 业务检查和处理已拆到 order_service.py，回调只协调接收、处理、确认。
        def on_message(
            ch: BlockingChannel,
            method: pika.spec.Basic.Deliver,
            properties: pika.BasicProperties,
            body: bytes,
        ) -> None:
            """回调由 Pika 调用：ch 是通道，method 是投递信息，properties 是消息属性。"""
            # redelivered=True 表示重新投递；不代表业务一定已执行过。
            print(
                f"收到消息：redelivered={method.redelivered}，body={body.decode('utf-8', errors='replace')}",
                flush=True,
            )
            try:
                event = parse_order_event(body)  # 将格式检查交给业务模块。
                if delay:
                    print(f"模拟处理 {delay:g} 秒；现在可以观察 Unacked。", flush=True)
                    # Pika 的 sleep 会保持心跳通信。
                    connection.sleep(delay)
                handle_order_event(event, queue_kind)  # 返回表示成功；抛出异常表示失败。
            # 这里只处理数据错误和模拟业务错误；连接异常继续抛出，避免误当业务失败。
            except (ValueError, KeyError, TypeError) as exc:
                if queue_kind == "email":
                    print(f"处理失败：{exc}；发送 nack，按死信配置转交失败队列。", flush=True)
                else:
                    print(
                        f"处理失败：{exc}；积分练习队列没有死信配置，本条消息将被丢弃。", flush=True
                    )
                # 不回原队列：邮件走死信配置，积分因没有死信配置会丢弃。
                # nack 不是失败队列的收件确认；默认死信转发仍有失败风险。
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            else:
                # 只有业务成功才确认；ack 之前连接断开，消息可能重新投递。
                ch.basic_ack(delivery_tag=method.delivery_tag)
                print("已发送 ack。", flush=True)
            if once:
                # 停止订阅循环后离开 with，连接随之关闭。
                ch.stop_consuming()

        # 必须先运行 topology 创建队列；这里订阅已有队列，不自动创建。
        channel.basic_consume(queue=queue_name, on_message_callback=on_message, auto_ack=False)
        print(f"等待队列 {queue_name} 的消息，按 Ctrl+C 退出。", flush=True)
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            print("\n消费者已停止；尚未确认的消息在连接关闭后会重新入队。", flush=True)


def main() -> None:
    """PyCharm 点击运行时，使用上方三个练习设置。"""
    consume(queue_kind=QUEUE_KIND, delay=DELAY_SECONDS, once=STOP_AFTER_ONE)


if __name__ == "__main__":
    main()
