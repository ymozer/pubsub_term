import asyncio
from asyncio import Future 
import redis.asyncio as redis
import time
import pandas as pd
import numpy as np

def csv_read():
	df = pd.read_csv("T1.csv")
	# Renaming columns for convenience
	df.rename(columns={'Date/Time': 'DateTime'}, inplace=True)
	df.rename(columns={'LV ActivePower (kW)': 'ActivePower'}, inplace=True)
	df.rename(columns={'Wind Speed (m/s)': 'Windspeed'}, inplace=True)
	df.rename(columns={'Wind Direction (°)': 'Winddir'}, inplace=True)
	df.rename(columns={'Theoretical_Power_Curve (KWh)': 'TheoreticalPC'}, inplace=True)
	
	# Create seperate dataframes for sendinfg to different subs
	# Each consist the data/time column
	df_ActivePower 		= df.drop(columns=["Windspeed","Winddir","TheoreticalPC"])
	df_WindSpeed 			= df.drop(columns=["ActivePower","Winddir","TheoreticalPC"])
	df_WindDir 				= df.drop(columns=["Windspeed","ActivePower","TheoreticalPC"])
	df_TheoreticalPC	= df.drop(columns=["Windspeed","Winddir","ActivePower"])

	# Return seperate dataframes as tuples
	data = [
		df_ActivePower,
		df_WindSpeed,
		df_WindDir,
		df_TheoreticalPC
	]
	return data

async def publisherAgent(data:pd.DataFrame, node:str, delay:int):
	# Connection to redis machine
	r = redis.Redis(host='localhost', port=6379, db=0)
	print(f"{node} ping successful: {await r.ping()}")
	# Sending 'STOP' signal to subs if file ends
	if type(data) == str and  data == "STOP":
		count=0
		while count<100:
			print(f"[{time.strftime('%X')}][{count}]: Sending stop signal for {node}. ")
			await r.publish(node, data) # send stop signal 
			count+=1
		await r.close()

	# Data publishing
	for index, row in data.iterrows():
		to_be_published_str = f"{row[0]}\n{row[1]}"
		# publish dataframe to specified node
		await r.publish(node, to_be_published_str)
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
