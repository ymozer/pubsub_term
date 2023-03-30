import asyncio
import aiofiles
import redis.asyncio as redis
import time


async def subAgent(node: str):
	split_str = node.split('_')  # ('NODE_NAME','VALUE_NAME')
	r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
	print(f"[{time.strftime('%X')}]: {split_str[0]} ping successful: {await r.ping()}")
	async with r.pubsub() as ps:
		await ps.subscribe(split_str[0])
		print(f"[{time.strftime('%X')}]: Subscribed to {split_str[0]}")
		while True:
			# Save stream of data to txt file
			# I want to see update of file realtime in vscode
			# so i open and close each loop (even though not performant)
			async with aiofiles.open(f"{node}.txt", 'a+') as f:
				message = await ps.get_message(ignore_subscribe_messages=True,timeout=5)
				if message is not None:
					if message['data'] == "STOP":
						print(f"[{time.strftime('%X')}]: EOF")
						break
					stream = (message['channel'], message['data'])
					# print(stream)
					# Write values to files per row
					await f.writelines(f"{stream[1]}\n")
					await f.close()
				else:
					print(f"[{time.strftime('%X')}]: {split_str[0]} Message empty. Check connection with publisher.")
			time.sleep(0.001)  # be nice to the system :)



async def main():
	'''Compose subscribers for async data gathering'''
	print(f"started at {time.strftime('%X')}")
	await asyncio.gather(subAgent("node-1_ActualPower"),
						subAgent("node-2_WindSpeed"),
						subAgent("node-3_WindDir"))
	print(f"finished at {time.strftime('%X')}")

if __name__ == "__main__":
	asyncio.run(main())
