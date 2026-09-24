import asyncio
import time


async def fetch_user():
    await asyncio.sleep(2)
    return "用户"


async def fetch_score():
    await asyncio.sleep(3)
    return "积分"


async def bingxing():
    before = time.perf_counter()
    user = await fetch_user()
    score = await fetch_score()

    print(f"并行 -> 用户：{user}, 积分：{score}, 耗时{time.perf_counter() - before}")


async def chuanxing():
    before = time.perf_counter()
    user, score = await asyncio.gather(fetch_user(), fetch_score())
    print(f"串行 -> 用户：{user}, 积分：{score}, 耗时{time.perf_counter() - before}")


async def main():
    await bingxing()
    await chuanxing()


if __name__ == "__main__":
    asyncio.run(main())
