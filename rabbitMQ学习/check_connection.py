"""只检查 AMQP 登录和建立通道，不创建队列，也不收发消息。"""

import sys

import pika

from rabbitMQ学习.config import open_connection


def main() -> None:
    """验证账号能否通过 AMQP 登录；不代表已经具备所有队列操作权限。"""
    try:
        # with 结束时关闭连接；channel 是在连接内部开出的操作通道。
        with open_connection() as connection:
            channel = connection.channel()
            print(f"AMQP 连接成功，通道编号：{channel.channel_number}")
            print("已通过 AMQP 登录；创建、读写队列的权限在后续练习中验证。")
    except pika.exceptions.ProbableAuthenticationError:
        # 登录信息错误与 vhost 权限错误分开提示，方便知道该查哪里。
        print("登录失败：请检查 .env 中的用户名和密码。", file=sys.stderr)
        sys.exit(1)
    except pika.exceptions.ProbableAccessDeniedError:
        print("无权访问 vhost：请检查该用户对 / 的权限。", file=sys.stderr)
        sys.exit(1)
    except (pika.exceptions.AMQPError, OSError, ValueError) as exc:
        # 只输出异常类型，避免异常详情意外带出连接密码。
        print(
            f"连接失败（{type(exc).__name__}）：请按 README 排查 URL、5672 端口和服务状态。",
            file=sys.stderr,
        )
        sys.exit(1)  # 非零退出码表示检查失败，方便终端或脚本判断结果。


if __name__ == "__main__":
    main()
