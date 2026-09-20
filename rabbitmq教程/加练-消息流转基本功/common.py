"""加练公共零件:连接地址 + 对象名 + 拓扑声明(声明部分由你来写)。

每个服务脚本启动时都调一遍 declare,谁先启动谁建。
"""

import os
from pathlib import Path

import aio_pika
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")
RABBITMQ_URL = os.environ["RABBITMQ_URL"]

# 对象名(照练习.md 的代号)
E1 = "e1"  # 事件交换机(topic)
D1 = "d1"  # 死信交换机(direct)
Q1 = "q1"  # 工作队列,worker 消费
Q2 = "q2"  # 观察队列,observer 消费
Q3 = "q3"  # 等待队列:消息统一活 5 秒,无人消费,到期回流 q1
Q4 = "q4"  # 死信池,watcher 消费

# 事件键
TASK_RUN = "task.run"  # 新任务
TASK_DONE = "task.done"  # 任务完成


async def declare(channel: aio_pika.Channel) -> dict[str, object]:
    """声明全套拓扑(交换机、队列、绑定),返回 {"e1":..., "d1":..., "q1":..., ...} 供调用方取用。

    属性要求见练习.md 的对象属性表,翻译成 declare 的类型和 arguments 是这次的练习本体。
    """
    # TODO 1: e1 —— topic 交换机,durable
    # TODO 2: d1 —— direct 交换机,durable
    # TODO 3: q1 —— 工作队列;durable;消息被 nack 放弃时,改键 "dead" 投给 d1
    # TODO 4: q2 —— 观察队列;durable;绑 e1 / task.*
    # TODO 5: q3 —— 等待队列;durable;无人消费;所有消息统一活 5 秒;
    #          到期死信改键 "q1" 投给默认交换机(直落回工作队列)
    # TODO 6: q4 —— 死信池;durable;绑 d1 / dead
    raise NotImplementedError("把拓扑声明补在 common.py 的 declare 里,删掉这一行")
