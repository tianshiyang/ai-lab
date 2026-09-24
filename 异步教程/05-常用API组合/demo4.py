import asyncio


async def demo_wait_first_completed() -> None:
    async def work(name: str, delay: float) -> None:
        await asyncio.sleep(delay)
        return name

    tasks = {asyncio.create_task(work("慢", 2)), asyncio.create_task(work("快", 1))}
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    for task in pending:
        task.cancel()

    await asyncio.gather(*pending, return_exceptions=True)
    print(next(iter(done)).result())


async def main() -> None:
    await demo_wait_first_completed()


if __name__ == "__main__":
    asyncio.run(main())
