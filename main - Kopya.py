import threading
import time
import queue

class EventChannel:
    def __init__(self):
        self.subscriber_queues = {
            "subscriber1": queue.Queue(),
            "subscriber2": queue.Queue()
        }
        self.locks = {
            "subscriber1": threading.Lock(),
            "subscriber2": threading.Lock()
        }
    
    def publish(self, message):
        words = message.split()
        for word in words:
            self.subscriber_queues["subscriber1"].put(word)
            self.subscriber_queues["subscriber2"].put(word)
    
    def subscribe(self, subscriber_name):
        return SubscriberAgent(self.subscriber_queues[subscriber_name], self.locks[subscriber_name])

class SubscriberAgent:
    def __init__(self, queue, lock):
        self.queue = queue
        self.lock = lock
        
    def read_message(self):
        self.lock.acquire()
        try:
            word1 = self.queue.get(block=True, timeout=None)
            word2 = self.queue.get(block=True, timeout=None)
            word3 = self.queue.get(block=True, timeout=None)
            word4 = self.queue.get(block=True, timeout=None)

        finally:
            self.lock.release()
        return (word1, word2, word3,word4)

# Example usage:
channel = EventChannel()
subscriber1 = channel.subscribe("subscriber1")
subscriber2 = channel.subscribe("subscriber2")
publisher = threading.Thread(target=channel.publish, args=("Hello Agent Oriented Programming",))
publisher.start()
print("Subscriber-1:")
for i in subscriber1.read_message():
    i=i.strip()
    if i == 'Hello' or i=='Oriented' : 
        print(i)
print("\nSubscriber-2:")
for i in subscriber2.read_message():
    if i == 'Agent' or i=='Programming' : 
        print(i)
    
publisher.join()
