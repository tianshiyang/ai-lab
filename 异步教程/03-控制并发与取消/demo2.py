import asyncio


async def demo_timeout() -> None:
    async def slow() -> None:
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            print("slow收到取消")
            raise

    try:
        async with asyncio.timeout(0.1):
            await slow()
    except TimeoutError:
        print("外层收到 TimeoutError")


async def main() -> None:
    await demo_timeout()


if __name__ == "__main__":
    asyncio.run(main())
