import asyncio
import time


async def demo_to_thread() -> None:
    def blocking_io() -> str:
        time.sleep(1.5)
        return "同步工作结束"

    async def hearbeat() -> None:
        for index in range(5):
            print(f"心跳{index}")
            await asyncio.sleep(1)

    _, result = await asyncio.gather(asyncio.to_thread(blocking_io),hearbeat())
    print(result)


async def main() -> None:
    await demo_to_thread()


if __name__ == "__main__":
    asyncio.run(main())
