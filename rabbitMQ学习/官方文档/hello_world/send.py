from rabbitMQ学习.官方文档.hello_world.config import hello_world_channel

hello_world_channel.queue_declare(queue="hello", durable=True, arguments={"x-queue-type": "quorum"})


# 发送消息
hello_world_channel.basic_publish(exchange="", routing_key="hello", body="Hello World")

print("已发送 'Hello World'")
hello_world_channel.close()