from rabbitMQ学习.官方文档.hello_world.config import hello_world_channel


def callback(ch, method, properties, body):
    print(f'[x] Received {body}')


hello_world_channel.basic_consume(queue="hello", auto_ack=True, on_message_callback=callback)

hello_world_channel.start_consuming()