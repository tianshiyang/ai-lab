import asyncio


async def demo_gather_exceptions() -> None:
    async def work(fail: bool) -> str:
        await asyncio.sleep(0.1)
        if fail:
            raise RuntimeError("失败了")
        else:
            return "成功了"

    results = await asyncio.gather(work(False), work(True), return_exceptions=True)
    for result in results:
        print(type(result).__name__, result)

    print(results)


async def main() -> None:
    await demo_gather_exceptions()


if __name__ == "__main__":
    asyncio.run(main())
