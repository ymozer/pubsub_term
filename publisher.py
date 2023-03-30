import asyncio
from asyncio import Future 
import redis.asyncio as redis
import time
import pandas as pd
import numpy as np

def csv_read():
	df = pd.read_csv("T1.csv")
	# Drop the specified column because homework doesn't mention it 
	df = df.drop(columns=["Theoretical_Power_Curve (KWh)"])
	# Renaming columns for convenience
	df.rename(columns={'LV ActivePower (kW)': 'Actualpower'}, inplace=True)
	df.rename(columns={'Wind Speed (m/s)': 'Windspeed'}, inplace=True)
	df.rename(columns={'Wind Direction (°)': 'Winddir'}, inplace=True)
	df.rename(columns={'Date/Time': 'DateTime'}, inplace=True)

	# Return data composed as Tuple
	data = np.array([
		df['Actualpower'].to_numpy(),
		df['Windspeed'].to_numpy(),
		df['Winddir'].to_numpy(),
		df['DateTime'].to_numpy()
	])
	return data


async def publisherAgent(data, node, delay):
	r = redis.Redis(host='localhost', port=6379, db=0)
	print(f"{node} ping successful: {await r.ping()}")
	if type(data) == str and  data == "STOP":
		count=0
		while count<100:
			print(f"[{time.strftime('%X')}][{count}]: Sending stop signal for {node}. ")
			await r.publish(node, data) # send stop signal 
			count+=1
		await r.close()

	for i in data:
		# publish data to specified node
		await r.publish(node, i)
		# Sleep async for specified seconds
		await asyncio.sleep(delay)
	await r.close()


async def main(data,delay):
	print(f"started at {time.strftime('%X')}")
	# TODO : Don't publish just one column, publish all columns for row
	# Need to compare this data if they have same date
	await asyncio.gather(	publisherAgent(data[0], "node-1", delay),
							publisherAgent(data[1], "node-2", delay),
							publisherAgent(data[2], "node-3", delay)
	)
	print(f"finished at {time.strftime('%X')}")
	await publisherAgent(("STOP"), "node-1", delay)
	await publisherAgent(("STOP"), "node-2", delay)
	await publisherAgent(("STOP"), "node-3", delay)


if __name__ == "__main__":
	# dataset parse
	data = csv_read()
	# delay in seconds
	delay = 1
	asyncio.run(main(data,delay))
