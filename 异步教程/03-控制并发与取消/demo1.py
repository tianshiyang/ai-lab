import asyncio


async def demo_semaphore() -> None:
    limit = asyncio.Semaphore(2)

    async def work(index: int) -> None:
        async with limit:
            print(f"开始 {index}")
            await asyncio.sleep(index)
            print(f"结束 {index}")

    await asyncio.gather(*(work(index) for index in range(5)))

async def main() -> None:
    await demo_semaphore()


if __name__ == "__main__":
    asyncio.run(main())
