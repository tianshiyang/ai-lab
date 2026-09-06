"""只检查 AMQP 登录和建立通道，不创建队列，也不收发消息。"""

import sys

import pika

from rabbitMQ学习.config import open_connection


def main() -> None:
    try:
        with open_connection() as connection:
            channel = connection.channel()
            print(f"AMQP 连接成功，通道编号：{channel.channel_number}")
            print("已通过 AMQP 登录；创建、读写队列的权限在后续练习中验证。")
    except pika.exceptions.ProbableAuthenticationError:
        print("登录失败：请检查 .env 中的用户名和密码。", file=sys.stderr)
        sys.exit(1)
    except pika.exceptions.ProbableAccessDeniedError:
        print("无权访问 vhost：请检查该用户对 / 的权限。", file=sys.stderr)
        sys.exit(1)
    except (pika.exceptions.AMQPError, OSError, ValueError) as exc:
        print(
            f"连接失败（{type(exc).__name__}）：请按 README 排查 URL、5672 端口和服务状态。",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
