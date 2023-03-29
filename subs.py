import asyncio
import aiofiles
import redis.asyncio as redis
import time

async def pubsub_test(node):
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    print(f"Ping successful: {r.ping()}")
    async with aiofiles.open(f"{node}.txt",'a') as f:
        async with r.pubsub() as ps:
            await ps.subscribe(node)
            #ps.subscribe("user-*")
            print(f"subscribed to {node}")

            while True:
                message = await ps.get_message(ignore_subscribe_messages=True)
                if message is not None:
                    stream = (message['channel'], message['data'])
                    print(stream)
                    await f.writelines(f"{stream[1]}\n")

async def main():
    print(f"started at {time.strftime('%X')}")
    await asyncio.gather(pubsub_test("node-1"),
                         pubsub_test("node-2"),
                         pubsub_test("node-3"))
    print(f"finished at {time.strftime('%X')}")

if __name__ == "__main__":
    asyncio.run(main())
