import asyncio


async def task_fast():
    await asyncio.sleep(1)
    print("执行了快")
    return "快"


async def task_slow():
    await asyncio.sleep(3)
    print("执行了慢")
    return "慢"


async def main():
    done, pending = await asyncio.wait(
        {asyncio.create_task(task_fast()), asyncio.create_task(task_slow())},
        return_when=asyncio.FIRST_COMPLETED,
    )

    winner = next(iter(done))
    print(f"获胜：{winner.result()}")

    for task in pending:
        await task
        # task.cancel()
    # result = await asyncio.gather(*pending, return_exceptions=True)
    # print(result)


if __name__ == "__main__":
    asyncio.run(main())
