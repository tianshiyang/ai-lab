import asyncio
from asyncio import CancelledError


async def demo_semaphore():
    semaphore = asyncio.Semaphore(2)

    async def task(i: int):
        async with semaphore:
            print(f"任务{i}进入")
            await asyncio.sleep(1)
            print(f"任务{i}离开")

    await asyncio.gather(*(task(i) for i in range(5)))


async def demo_timeout():
    try:
        async with asyncio.timeout(1):
            await asyncio.sleep(2)
    except TimeoutError:
        print("timeout 超时")


async def demo_wait_for():
    async def task():
        try:
            await asyncio.sleep(2)
        except CancelledError:
            print("取消?")
            raise
        except TimeoutError:
            print("超时？")
            raise

    try:
        await asyncio.wait_for(task(), timeout=1)
    except TimeoutError:
        print("超时了")


if __name__ == "__main__":
    # asyncio.run(demo_semaphore())
    # asyncio.run(demo_timeout())
    asyncio.run(demo_wait_for())
