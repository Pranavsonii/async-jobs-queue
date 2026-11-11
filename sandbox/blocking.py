import asyncio

queue = asyncio.Queue()


async def customer(name):
    print(f"{name} Line me aaya")
    await queue.put(name)


async def counter_worker():
    while True:
        # koi nahi hai to wait karo yehi brpop hai
        person = await queue.get()
        print(f"serving: {person}")
        await asyncio.sleep(1)


async def main():
    await asyncio.gather(customer("rahul"), customer("priya"), counter_worker())
