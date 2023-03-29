# r = redis.Redis(
#    host='redis-12983.c293.eu-central-1-1.ec2.cloud.redislabs.com',
#    port=12983,
#    password='jurCPpNghHXZOwjNUwxDqSeianTBrA8B',
#    decode_responses=True
# )
import asyncio
import redis.asyncio as redis
import time
import pandas as pd
import numpy as np


def sync_exec(coro):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(coro)


def csv_read():
    df = pd.read_csv("T1.csv")
    df = df.drop(columns=["Theoretical_Power_Curve (KWh)"])
    df.rename(columns={'LV ActivePower (kW)': 'Actualpower'}, inplace=True)
    df.rename(columns={'Wind Speed (m/s)': 'Windspeed'}, inplace=True)
    df.rename(columns={'Wind Direction (°)': 'Winddir'}, inplace=True)
    df.rename(columns={'Date/Time': 'DateTime'}, inplace=True)

    data = np.array([
        df['Actualpower'].to_numpy(),
        df['Windspeed'].to_numpy(),
        df['Winddir'].to_numpy(),
        df['DateTime'].to_numpy()
    ])
    return data


async def pubsub_test(data, node):
    r = redis.Redis(host='localhost', port=6379, db=0)
    print(node)
    print(f"Ping successful: {await r.ping()}")
    for i in data:
        await r.publish(node, i)
        await asyncio.sleep(1)
    await r.close()


async def main(data):
    print(f"started at {time.strftime('%X')}")
    await asyncio.gather(pubsub_test(data[0], "node-1"),
                         pubsub_test(data[1], "node-2"),
                         pubsub_test(data[2], "node-3"))
    print(f"finished at {time.strftime('%X')}")

if __name__ == "__main__":
    data = csv_read()
    row, col = data.shape
    print(row, col)
    asyncio.run(main(data))
