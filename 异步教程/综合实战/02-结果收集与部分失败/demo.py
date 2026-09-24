import asyncio
import time


async def task_a():
    await asyncio.sleep(1)
    return "A"


async def task_b():
    await asyncio.sleep(2)
    raise ValueError("B")


async def task_c():
    await asyncio.sleep(3)
    return "C"


async def task_1():
    before = time.perf_counter()
    result = await asyncio.gather(task_a(), task_b(), task_c(), return_exceptions=True)
    print(result)
    print(f"总耗时：{time.perf_counter() - before}")


async def task_2():
    before = time.perf_counter()
    for item in asyncio.as_completed([task_a(), task_b(), task_c()]):
        try:
            result = await item
            print(result)
        except ValueError as e:
            print(f"ValueError: {e}")

    print(f"总耗时：{time.perf_counter() - before}")


if __name__ == "__main__":
    # asyncio.run(task_1())
    asyncio.run(task_2())
