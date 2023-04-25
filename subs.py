import os
import sys
import time
import asyncio
import aiofiles
import argparse
import redis.asyncio as redis
 
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
    
class Subscriber:
	def __init__(self,*args, **kwargs):
		self.pubs=kwargs.get('KNOWN_PUBS')

	async def subAgent(self,node: str):
		# split argument input to match publisher's node's:
		# node-1_ActivePower --> ['node-1', 'ActivePower']
		split_str = node.split('_')  # ('NODE_NAME','VALUE_NAME')

		# Connection to redis machine
		r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

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
		'''Compose subscribers for async data gathering'''
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
			filename = "combined.csv"
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
	args = parser.parse_args()
	if len(sys.argv) > 1:
		# Manager Agent Part
		print(args.infile.name, args.infile.mode)
	else:
		KNOWN_PUBS = ["node-1", "node-2", "node-3"]
		sub=Subscriber(KNOWN_PUBS=KNOWN_PUBS)
		asyncio.run(sub.main())
