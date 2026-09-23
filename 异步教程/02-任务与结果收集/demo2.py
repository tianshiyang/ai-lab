import asyncio


async def demo_gather_order() -> None:
    async def work(name: str, delay: float) -> None:
        await asyncio.sleep(delay)
        print(f"完成{name}")
        return name

    results = await asyncio.gather(work("慢", 0.2), work("快", 0.1))
    print(results)


async def main() -> None:
    await demo_gather_order()


if __name__ == "__main__":
    asyncio.run(main())
