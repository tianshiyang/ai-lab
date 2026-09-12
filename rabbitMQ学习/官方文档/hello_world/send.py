"""生产者：声明队列，发一条消息，完事。"""
from rabbitMQ学习.官方文档.hello_world.config import hello_world_channel

# queue_declare：声明队列。幂等——参数相同重复声明无害；
# 但如果队列已存在而这次声明参数不同（比如 durable 不一致），
# broker 会报 406 PRECONDITION_FAILED 并关掉 channel，所以生产/消费两边的参数必须完全一致。
#   durable=True：队列的定义在 RabbitMQ 重启后仍保留（经典队列还要配消息持久化才不丢消息）。
#   x-queue-type: quorum：法定人数队列，基于 Raft 在节点间复制，比经典队列更安全，
#   是官方推荐（取代旧的镜像队列）。注意 quorum 队列强制要求 durable=True。
hello_world_channel.queue_declare(queue="hello", durable=True, arguments={"x-queue-type": "quorum"})


# basic_publish：消息是发给 exchange（交换机）的，再由交换机按规则路由到队列。
#   exchange="" 即"默认交换机"：routing_key 直接当队列名用，消息原样进同名队列，
#   是不引入交换机概念时最简单的写法（到发布/订阅那章才会真正用上交换机）。
#   body 可以是 str（pika 按 UTF-8 编码成 bytes）或直接给 bytes。
hello_world_channel.basic_publish(exchange="", routing_key="hello", body="Hello World")

print("已发送 'Hello World'")
hello_world_channel.close()
