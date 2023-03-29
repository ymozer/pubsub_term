#  File "C:\Users\ymeti\OneDrive\Masaüstü\Yapay Zeka\TermProj\Lib\site-packages\spade\behaviour.py", line 63, in set_agent 
# REMOVED asyncio.Queue(loop=self.agent.loop) --> asyncio.Queue()
# jabbim servers is slow so timeout is long (40 sec)
import time
import datetime

from spade_pubsub import PubSubMixin

from spade.agent import Agent
from spade.behaviour import CyclicBehaviour, PeriodicBehaviour
from spade.message import Message
from spade.template import Template


class SenderAgent(PubSubMixin, Agent):
    class InformBehav(PeriodicBehaviour):
        async def run(self):
            
            print(f"PeriodicSenderBehaviour running at {datetime.datetime.now().time()}: {self.counter}\n")
            msg1 = Message(to=self.get("receiver_jid_1"))  # Instantiate the message
            msg1.body = "Hello Agent"  # Set the message content
            msg1.set_metadata("performative", "inform")
            await self.agent.pubsub.publish(self.get("receiver_jid_1"), "node1", msg1)
            print("Message1 sent!\n")

            msg2 = Message(to=self.get("receiver_jid_2"))  # Instantiate the message
            msg2.body = "Oriented Programming"  # Set the message content
            msg2.set_metadata("performative", "inform")
            await self.agent.pubsub.publish(self.get("receiver_jid_2"), "node2", msg2)
            print("Message2 sent!\n")

            if self.counter == 15:
                self.kill()
            self.counter += 1

        async def on_end(self):
            # stop agent from behaviour
            await self.agent.pubsub.delete(self.get("receiver_jid_1"), "node1")
            await self.agent.pubsub.delete(self.get("receiver_jid_2"), "node2")
            await self.agent.stop()

        async def on_start(self):
            self.counter = 0

    async def setup(self):
        print(f"PeriodicSenderAgent started at {datetime.datetime.now().time()}")
        await self.agent.pubsub.create("sendermetin@jabbim.com", "node1")
        await self.agent.pubsub.create("sendermetin@jabbim.com", "node2")
        list_of_nodes = await self.agent.pubsub.get_nodes("sendermetin@jabbim.com")
        print("All nodes: ",list_of_nodes)
        start_at = datetime.datetime.now() + datetime.timedelta(seconds=5)
        b = self.InformBehav(period=2, start_at=start_at)
        self.add_behaviour(b)

class receiver(PubSubMixin, Agent):
    class RecvBehav(CyclicBehaviour):

        async def run(self):
            print("RecvBehav running")
            await self.agent.pubsub.subscribe("sendermetin@jabbim.com", "node1")
            msg1=await self.agent.pubsub.get_items("sendermetin@jabbim.com", "node1", timeout= 40)
            await self.agent.pubsub.subscribe("sendermetin@jabbim.com", "node2")
            msg2=await self.agent.pubsub.get_items("sendermetin@jabbim.com", "node2", timeout= 40)

            if msg1:
                print("Message1 received with content: {}".format(msg1.body))
            else:
                print("Did not received any message1 after 40 seconds")

            if msg2:
                print("Message2 received with content: {}".format(msg2.body))
            else:
                print("Did not received any message2 after 40 seconds")

            await self.agent.stop()

    async def setup(self):
        print("receiver started")
        b = self.RecvBehav()
        template = Template()
        template.set_metadata("performative", "inform")
        self.add_behaviour(b, template)



if __name__ == "__main__":
    senderagent = SenderAgent("sendermetin@jabbim.com", "112233")
    senderagent.set("receiver_jid_1","metin@jabbim.com")
    senderagent.set("receiver_jid_2","yusufmetin@jabbim.com")
    print(f"Knowladge Base of Sender:\n*{senderagent.get('receiver_jid_1')}\n*{senderagent.get('receiver_jid_2')}")
    senderagent.start(auto_register=True)

    node1_receiver= receiver("metin@jabbim.com", "112233")
    future = node1_receiver.start(auto_register=True)
    future.result()

    node2_receiver= receiver("yusufmetin@jabbim.com", "112233")
    future2 = node2_receiver.start(auto_register=True)
    future2.result() 


    while node1_receiver.is_alive() and node2_receiver.is_alive():
        try:
            time.sleep(3)
        except KeyboardInterrupt:
            senderagent.stop()
            node1_receiver.stop()
            node2_receiver.stop()
            break
    print("Agents finished")
