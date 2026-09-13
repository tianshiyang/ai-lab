from rabbitMQ学习.官方文档.发布订阅.config import mitt_channel

result = mitt_channel.queue_declare(queue="", exclusive=True)

queue_name = result.method.queue

mitt_channel.queue_bind(exchange="logs", queue=queue_name)

print(" [*] Waiting for logs. To exit press CTRL+C")


def callback(ch, method, properties, body):
    print(f" [x] {body}")


mitt_channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

mitt_channel.start_consuming()