import asyncio
import aiofiles
import redis.asyncio as redis
import time
import os
 
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
				message = await ps.get_message(ignore_subscribe_messages=True, timeout=7)
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
					continue


	def parse(self,data):
		'''Parse incoming data'''
		value = data[1]  # date value
		value = value.split('\n')
		return value


	async def main(self):
		'''Compose subscribers for async data gathering'''
		print(f"started at {time.strftime('%X')}")
		while True:
			results = await asyncio.gather(self.subAgent("node-1_ActivePower"),
											self.subAgent("node-2_WindSpeed"),
											self.subAgent("node-3_WindDir"))
			if results[0][0][1] == "STOP" and results[1][0][1] == "STOP" and results[2][0][1] == "STOP":
				break
			node_1_value = self.parse(results[0][0])
			node_2_value = self.parse(results[1][0])
			node_3_value = self.parse(results[2][0])

			# Check if all nodes have same date
			if node_1_value[0] == node_2_value[0] == node_3_value[0]:
				print(f"[{time.strftime('%X')}] Dates are same:\nnode-1: {node_1_value[1]}\tnode-2: {node_2_value[1]}\tnode-3: {node_3_value[1]}\n")
				# Write to file
				filename = "combined.txt"

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
						await f.write(f"Execution time\tDate/Time\tActivePower\tWindSpeed\tWindDir\n")
					await f.write(f"{time.strftime('%X')}\t{node_1_value[0]}\t{node_1_value[1]}\t{node_2_value[1]}\t{node_3_value[1]}\n")
		print(f"finished at {time.strftime('%X')}")

if __name__ == "__main__":
	KNOWN_PUBS = ["node-1", "node-2", "node-3"]
	sub=Subscriber(KNOWN_PUBS=KNOWN_PUBS)
	asyncio.run(sub.main())
