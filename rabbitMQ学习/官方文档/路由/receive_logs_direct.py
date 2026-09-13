import pika

from rabbitMQ学习.官方文档.路由.config import router_channel, router_queue_name

severities = ["warning", "info", "danger"]

for severity in severities:
    router_channel.queue_bind(exchange="direct_logs", queue=router_queue_name, routing_key=severity)


def callback(ch, method, properties, body):
    print(f"[x] {method.routing_key}:{body}")


router_channel.basic_consume(queue=router_queue_name, on_message_callback=callback, auto_ack=True)

router_channel.start_consuming()