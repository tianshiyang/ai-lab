import asyncio


async def demo_sleep_zero() -> None:
    async def worker(name: str) -> None:
        print(f"{name}: 第一步")
        await asyncio.sleep(0)
        print(f"{name}: 第二步")

    await asyncio.gather(worker("A"), worker("B"))


async def main() -> None:
    await demo_sleep_zero()


if __name__ == "__main__":
    asyncio.run(main())
