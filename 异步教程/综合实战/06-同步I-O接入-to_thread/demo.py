import asyncio
import time


def blocking_work():
    time.sleep(2)
    print("结果返回了")


async def heartbeat():
    for i in range(4):
        print(f"心跳{i}")
        await asyncio.sleep(1)


async def main():
    task_1 = asyncio.to_thread(blocking_work)
    await asyncio.gather(task_1, heartbeat())


if __name__ == "__main__":
    asyncio.run(main())
