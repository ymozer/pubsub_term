import asyncio
from asyncio import Future 
import redis.asyncio as redis
import time
import pandas as pd
import numpy as np

class Publisher:
	@classmethod
	async def create(cls,data, name, delay):
		self = Publisher()
		self.data=data
		self.name=name
		self.delay=delay

		print(f"started at {time.strftime('%X')}")
		asyncio.run(await self.publisherAgent(self.data, self.name, self.delay))
		print(f"finished at {time.strftime('%X')}")
		asyncio.run(await self.publisherAgent(("STOP"), self.name, self.delay))
		return self
	async def publisherAgent(self,data:pd.DataFrame, node:str, delay:int):
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
'''END OF CLASS'''

def csv_read(data):
		df = pd.read_csv(data)
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
		df_list = [
			df_ActivePower,
			df_WindSpeed,
			df_WindDir,
			df_TheoreticalPC
		]
		return df_list


async def main():
	# Read csv file
	filename = "T1.csv"
	delay = 1
	data=csv_read(filename)
	# Create publishers
	await asyncio.gather(Publisher.create(data[0],'node-1',delay),Publisher.create(data[1],'node-2',delay),Publisher.create(data[2],'node-3',delay))


if __name__ == "__main__":
	asyncio.run(main())
