import os
import sys
import time
import asyncio
import aiofiles
import argparse
import redis.asyncio as redis
from redis.exceptions import ConnectionError
from sklearn.model_selection import train_test_split
import pandas as pd
 
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
    
class ManagerAgent:
	@classmethod
	async def create(cls,data, name, delay):
		self = ManagerAgent()
		self.data=data # type: ignore
		self.name=name # type: ignore
		self.delay=delay # type: ignore

		print(f"{bcolors.HEADER}{self.name} started at {time.strftime('%X')}{bcolors.ENDC}")
		asyncio.run(await self.ManagerAgent(self.data, self.name, self.delay))
		print(f"Finished at {time.strftime('%X')}")
		asyncio.run(await self.ManagerAgent(("STOP"), self.name, self.delay))
		return self
	
	async def ManagerAgent(self,data:pd.DataFrame, node:str, delay:int):
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

		# Data publishing
		for index, row in data.iterrows():
			to_be_published_str = f"{row[0]}\n{row[1]}"
			# publish dataframe to specified node
			await r.publish(node, to_be_published_str)
			# Sleep async for specified seconds
			await asyncio.sleep(delay)
		await r.close()
'''END OF CLASS'''

class Subscriber:
	def __init__(self,*args, **kwargs):
		self.pubs=kwargs.get('KNOWN_PUBS')
		self.dataset_name=kwargs.get('DATASET_NAME')

	async def subAgent(self,node: str):
		# split argument input to match publisher's node's:
		# node-1_ActivePower --> ['node-1', 'ActivePower']
		split_str = node.split('_')  # ('NODE_NAME','VALUE_NAME')

		# Connection to redis machine
		pool = redis.ConnectionPool(host='localhost', port=6379, db=0, decode_responses=True)
		r = redis.Redis(connection_pool=pool)
		try:
			await r.ping()
		except ConnectionError as e:
			print(f"[{time.strftime('%X')}]: Cannot connect to redis server!")
			# print(e)
			sys.exit(1)

		# get other node names
		hosts = self.pubs.copy()
		if split_str[0] in hosts:
			index = hosts.index(split_str[0])
			hosts.pop(index)

		# subscribing to specified node
		async with r.pubsub() as ps:
			# subscribe to own channel
			await ps.subscribe(split_str[0])
			# print(f"[{time.strftime('%X')}]: Subscribed to {split_str[0]}")
			while True:
				message = await ps.get_message(ignore_subscribe_messages=True, timeout=3)
				# if message NOT empty
				if message is not None:
					# If incoming message, break loop and finish agent
					if message['data'] == "STOP":
						print(f"[{time.strftime('%X')}]: EOF")
						break
					stream = (message['channel'], message['data'])
					# print(stream)
					time.sleep(0.001)  # be nice to the system :)
					return stream, hosts
				else:
					#print(f"[{time.strftime('%X')}-{split_str[0]}]: Cannot communicate with Publisher!")
					continue


	def parse(self,data):
		'''Parse incoming data'''
		value = data[1]  # date value
		value = value.split('\n')
		return value


	async def main(self):
		'''Compose subscribers for async data gathering
			and !!! DATASET CREATION !!!
		'''
		print(f"{bcolors.HEADER}Started at {time.strftime('%X')}{bcolors.ENDC}")
		while True:
			results = await asyncio.gather(self.subAgent("node-1_ActivePower"),
											self.subAgent("node-2_WindSpeed"),
											self.subAgent("node-3_WindDir"))
			if results[0][0][1] == "STOP" and results[1][0][1] == "STOP" and results[2][0][1] == "STOP":
				break
			node_1_value = self.parse(results[0][0])
			node_2_value = self.parse(results[1][0])
			node_3_value = self.parse(results[2][0])

			mismatch_count=0
			filename = f"{self.dataset_name}.csv"
			# Check if all nodes have same date
			if node_1_value[0] == node_2_value[0] == node_3_value[0]:
				print(f"{bcolors.OKGREEN}[{time.strftime('%X')}] Dates are same:\n{bcolors.ENDC}{bcolors.OKBLUE}node-1: {node_1_value[1]}\tnode-2: {node_2_value[1]}\tnode-3: {node_3_value[1]}{bcolors.ENDC}\n")

				# Check if file exist because we want to write to first row
				# as column names (headers)
				flag = False
				if os.path.exists(filename) == False:
					flag = True
				
				'''
					I am opening and closing each time value incomes. I want to read
					file in realtime(even though it is not performant), otherwise
					can't read file because of the lock
				'''
				async with aiofiles.open(filename, 'a+', encoding='utf-8') as f:
					if flag:
						await f.write(f"Execution time,Date/Time,ActivePower,WindSpeed,WindDir\n")
					await f.write(f"{time.strftime('%X')},{node_1_value[0]},{node_1_value[1]},{node_2_value[1]},{node_3_value[1]}\n")
			else: # dates don't match
				print("Dates don't match")
				async with aiofiles.open(filename, 'a+', encoding='utf-8') as f:
					await f.write(f"{time.strftime('%X')},Lossed Data (Mismatch of dates)\n")
				mismatch_count+=1
				await asyncio.sleep(1)
				if mismatch_count>10: 
					break
		print(f"{bcolors.HEADER}Finished at {time.strftime('%X')}{bcolors.ENDC}")

	async def publish_test(self, df, pub: str):
		'''Publish test data to redis'''
		# Connection to redis machine
		pool = redis.ConnectionPool(host='localhost', port=6379, db=0, decode_responses=True)
		r = redis.Redis(connection_pool=pool)
		try:
			await r.ping()
		except ConnectionError as e:
			print(f"[{time.strftime('%X')}]: Cannot connect to redis server!")
			# print(e)
			sys.exit(1)
		# publish test data
		for index, row in df.iterrows():
			await r.publish(pub, f"{row['Date/Time']},{row['ActivePower']}")
			await asyncio.sleep(0.001)
		await r.publish(pub, "STOP")

async def manage():
	# Read splitted data
	x_test = pd.read_csv(os.path.join("splits","x_test.csv"))
	y_test = pd.read_csv(os.path.join("splits","y_test.csv"))
	# Send splitted data to ML Agent for prediction
	ml_agent= await ManagerAgent.create(x_test, "manager",0.5)

if __name__ == "__main__":
	parser=argparse.ArgumentParser(
		prog='PubSub Term',
		description='Wind Turbine Power Estimation'
	)
	parser.add_argument('-p','--predict',nargs='+',help='Predict power for given wind speed and direction')
	parser.add_argument('infile', nargs='?',
		      	type=argparse.FileType('a'),
             	default=sys.stdin,
		     	help='Input file (default: stdin). Use it if dataset already available.'
		     	)
	parser.set_defaults(infile="merged.csv")
	args = parser.parse_args()

	sub=Subscriber()
	if len(sys.argv) > 1:
		# Check if file exist
		if os.path.exists("merged.csv"):
			num_lines = sum(1 for line in open('merged.csv'))
			print(f"Dataset num of lines: {bcolors.HEADER}{num_lines}{bcolors.ENDC}")
			if num_lines < 50000:
				print("File is missing values!")
				asyncio.run(sub.main())
			else:
				print("Dataset merged exists")
				sys.exit(0)
		else:
			print("File does not exist! Exiting...")
			sys.exit(1)
	# if no args supplied
	else:
		if not os.path.exists("merged.csv"):
			# Create dataset
			asyncio.run(sub.main())
		else:
			# Send splitted data to ML Agent for prediction
			# Check if folder exist
			if not os.path.exists(os.path.join("splits","x_test.csv")):
				print("start ml_agent first")
				sys.exit(1)
			else:
				asyncio.run(manage())
			

# Eğer olustuturulan dataset var olup da "split" halleri yoksa onları olustur
# Eğer olusturulan dataset yoksa olustur
			

