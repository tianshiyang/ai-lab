"""完成模拟业务后手动确认；邮件处理失败时拒绝并进入死信流程。"""

import argparse
import json

import pika
from pika.adapters.blocking_connection import BlockingChannel

from rabbitMQ学习.config import open_connection
from rabbitMQ学习.topology import ORDER_EMAIL_QUEUE, ORDER_POINTS_QUEUE


def consume(*, queue_kind: str = "email", delay: float = 0, once: bool = False) -> None:
    """消费指定业务队列；delay 模拟耗时，once 表示处理一条后停止。"""
    queue_name = ORDER_EMAIL_QUEUE if queue_kind == "email" else ORDER_POINTS_QUEUE
    with open_connection() as connection:
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)  # 限制未确认数量，不是启动一个工作线程。

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
                event = json.loads(body)  # JSON 字节还原成 Python 对象，再验证业务字段。
                if not isinstance(event, dict) or not event.get("event_id"):
                    raise ValueError("消息必须是带有 event_id 的 JSON 对象")
                if event.get("event_name") != "order.paid" or not event.get("order_id"):
                    raise ValueError("消息必须是带有 order_id 的 order.paid 事件")
                if delay:
                    print(f"模拟处理 {delay:g} 秒；现在可以观察 Unacked。", flush=True)
                    # Pika 的 sleep 会保持心跳通信。
                    connection.sleep(delay)
                if queue_kind == "email" and event.get("simulate_failure"):
                    raise ValueError("按 --fail 参数模拟邮件处理失败")
                action = "发送支付通知" if queue_kind == "email" else "增加积分"
                print(f"模拟{action}成功：order_id={event['order_id']}", flush=True)
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
    """解析命令行选项；限制模拟耗时，方便在管理页面观察 Unacked。"""
    parser = argparse.ArgumentParser(description="消费订单消息（只打印，不实际发邮件或加积分）")
    parser.add_argument("--queue", choices=["email", "points"], default="email")
    parser.add_argument("--delay", type=float, default=0, help="模拟耗时秒数，范围 0～60")
    parser.add_argument("--once", action="store_true", help="处理一条后退出；没有消息时仍等待")
    args = parser.parse_args()
    if not 0 <= args.delay <= 60:
        parser.error("--delay 必须在 0～60 秒之间")
    consume(queue_kind=args.queue, delay=args.delay, once=args.once)


if __name__ == "__main__":
    main()
