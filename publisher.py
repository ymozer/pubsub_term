import asyncio
import time
import argparse
from asyncio import Future 
import redis.asyncio as redis

import pandas as pd
import numpy as np

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class Publisher:
	@classmethod
	async def create(cls,data, name, delay):
		self = Publisher()
		self.data=data # type: ignore
		self.name=name # type: ignore
		self.delay=delay # type: ignore

		print(f"{bcolors.HEADER}{self.name} started at {time.strftime('%X')}{bcolors.ENDC}")
		await self.publisherAgent(self.data, self.name, self.delay)
		print(f"finished at {time.strftime('%X')}")
		await self.publisherAgent(("STOP"), self.name, self.delay)
		return self
	
	async def publisherAgent(self,data:pd.DataFrame, node:str, delay:int):
		# Connection to redis machine
		r = redis.Redis(host='localhost', port=6379, db=0)
		print(f"{node} ping successful: {await r.ping()}")
		# Sending 'STOP' signal to subs if file ends
		if type(data) == str and  data == "STOP":
			count=0
			while count<100:
				print(f"{bcolors.HEADER}[{time.strftime('%X')}][{count}]: Sending stop signal for {node}.{bcolors.ENDC}")
				await r.publish(node, data) # send stop signal 
				count+=1
			await r.close()
			return

		# Data publishing
		for index, row in data.iterrows():
			to_be_published_str = f"{row[0]}\n{row[1]}"
			# publish dataframe to specified node
			await r.publish(node, to_be_published_str)
			# Sleep async for specified seconds
			await asyncio.sleep(delay)
		await r.close()
'''END OF CLASS'''

def my_csv_read(data):
		df=pd.read_csv(data,sep=';')

		# Create seperate dataframes for sending to different subs
		# Each consist the data/time column
		df_ActivePower   = df[['Date/Time','LV ActivePower (kW)']]
		df_WindSpeed     = df[['Date/Time','Wind Speed (m/s)']]
		df_WindDir       = df[['Date/Time','Wind Direction (°)']]
		df_TheoreticalPC = df[["Date/Time","Theoretical_Power_Curve (KWh)"]]

		# Return seperate dataframes as tuples
		df_list = [
			df_ActivePower,
			df_WindSpeed,
			df_WindDir,
			df_TheoreticalPC
		]
		return df_list


async def main():
	# Read csv file
	filename = "T1_small.csv"
	delay = 0.5
	data=my_csv_read(filename)

	# Create publishers
	await asyncio.gather(
		Publisher.create(data[0],'node-1',delay),
		Publisher.create(data[1],'node-2',delay),
		Publisher.create(data[2],'node-3',delay)
		)


if __name__ == "__main__":
	try:
		loop = asyncio.get_running_loop()
	except RuntimeError:  # 'RuntimeError: There is no current event loop...'
		loop = None

	if loop and loop.is_running():
		print('Async event loop already running. Adding coroutine to the event loop.')
		tsk = loop.create_task(main())
		# https://docs.python.org/3/library/asyncio-task.html#task-object
		# Optionally, a callback function can be executed when the coroutine completes
		tsk.add_done_callback(
			lambda t: print(f'Task done with result={t.result()}  << return val of main()'))
	else:
		print('Starting new event loop')
		result = asyncio.run(main())