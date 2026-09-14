import json
import time
from datetime import datetime

from rabbitMQ学习.实战练习.data.data import mock_config
from rabbitMQ学习.实战练习.demo01.store import (
    create_failed_record,
    create_gateway_call,
    create_send_record,
    get_send_record,
)
from rabbitMQ学习.实战练习.demo01.topology import channel_1, connection_1
from rabbitMQ学习.实战练习.demo01.typings import FailedResult, GatewayResult

# 改这里切换通道：sms / email / inapp，开三个运行窗口各跑一个
CHANNEL = "sms"

# 模拟调用第三方网关的耗时
SEND_SECONDS = mock_config["gateway"][CHANNEL]["send_seconds"]


async def callback_1(ch, method, properties, body):
    data = json.loads(body)

    record = await get_send_record(data)
    if record is not None:
        await create_failed_record(
            FailedResult(
                request_id=record.request_id,
                channel=record.channel,
                reason="数据重复异常",
                error_type="data_exist_error",
                attempts=0,
                permanent=True,
                raw_message=None,
                created_at=datetime.now(),
            )
        )
    record = await create_send_record(data)

    # 模拟调网关发送
    time.sleep(SEND_SECONDS)
    await create_gateway_call(
        GatewayResult(
            request_id=record.request_id,
            channel=record.channel,
            success=True,
            error_type=None,
            error_msg=None,
            called_at=datetime.now(),
        )
    )

    print(f"[{CHANNEL}] {data['request_id']} {data['user_id']} {data['content']}")

    ch.basic_qos(prefetch_count=1)

    # 业务处理完成后才确认；中途退出的消息会被重新投递
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel_1.basic_consume(queue=CHANNEL, on_message_callback=callback_1)

print(f"[{CHANNEL}] 开始消费，停止按钮退出")

try:
    channel_1.start_consuming()
except KeyboardInterrupt:
    channel_1.stop_consuming()
    connection_1.close()
