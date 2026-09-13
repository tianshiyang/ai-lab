import asyncio
from time import sleep

from rabbitMQ学习.官方文档.路由.config import router_channel, router_connection


async def main():
    for routing_key in ["info", "warning", "danger"]:
        sleep(2)
        router_channel.basic_publish(
            exchange="direct_logs", routing_key=routing_key, body=f"hello world: {routing_key}"
        )


if __name__ == "__main__":
    asyncio.run(main())
    router_connection.close()