from rabbitMQ学习.官方文档.发布订阅.config import mitt_channel, mitt_connection

mitt_channel.exchange_declare(exchange="logs", exchange_type="fanout")

message = "info:hello world"
mitt_channel.basic_publish(exchange="logs", routing_key="logs", body=message)

print(f"[x] Sent {message}")
mitt_connection.close()