import asyncio


async def demo_wait_for() -> None:
    try:
        result = await asyncio.wait_for(asyncio.sleep(1, result="完成"), timeout=1.1)
        print(result)
    except TimeoutError:
        print("await for 超时")


async def main() -> None:
    await demo_wait_for()


if __name__ == "__main__":
    asyncio.run(main())
