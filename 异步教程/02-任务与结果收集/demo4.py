import asyncio


async def demo_as_completed() -> None:
    async def work(name: str, delay: float) -> str:
        await asyncio.sleep(delay)
        return name

    tasks = [asyncio.create_task(work("慢", 0.2)), asyncio.create_task(work("快", 0.1))]
    for completed in asyncio.as_completed(tasks):
        print("先收到：", await completed)


async def main() -> None:
    await demo_as_completed()


if __name__ == "__main__":
    asyncio.run(main())
