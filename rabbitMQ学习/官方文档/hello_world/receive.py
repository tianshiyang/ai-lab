"""消费者：订阅队列，阻塞等消息，来一条处理一条。"""
from rabbitMQ学习.官方文档.hello_world.config import hello_world_channel


# 回调签名固定是 (ch, method, properties, body)：
#   ch         —— 收到消息的那条 channel，可以在回调里继续调它的 API（比如 basic_ack）
#   method     —— 本次"投递"的元信息（Basic.Deliver）：delivery_tag（这条消息在本 channel
#                 上的唯一编号，ack 时要用它）、routing_key、redelivered（是否被重新投递过）等
#   properties —— 发送方设置的消息属性（BasicProperties），如 delivery_mode、headers 等
#   body       —— 消息体，bytes 类型
def callback(ch, method, properties, body):
    print(f'[x] Received {body}')


# basic_consume：订阅队列，之后 broker 每投递一条消息就调用一次 on_message_callback。
# auto_ack=True：broker 把消息发出去就视为"已确认"、立刻删掉（fire-and-forget）——
# 消费者要是处理到一半挂了，这条消息就永远丢了。工作队列一章会改成手动 ack 来解决。
hello_world_channel.basic_consume(queue="hello", auto_ack=True, on_message_callback=callback)

# start_consuming：进入阻塞循环，把收到的消息分发给回调，并维持心跳。
# 会一直运行，直到连接关闭或 Ctrl+C。
hello_world_channel.start_consuming()
