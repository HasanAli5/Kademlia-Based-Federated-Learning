import kademlia.network

from network import Network as BaseNetwork
from asyncio import Lock
import json

class Network(BaseNetwork):
    # this adapts the base broadcasting system for the aggregation network
    # we want to stop relaying for pairing.
    # where we want to relay only if already paired to reduce network traffic.

    def __init__(self, node:kademlia.network.Server, port:int,
                 buffer_length:int, messages_length:int,ignores_length:int,model_transfer_port):
        super().__init__(node, port,
                         buffer_length, messages_length, ignores_length,
                         model_transfer_port)
        
        self.paired = False
        self.paired_lock = Lock()
        

    async def relay(self, message:str, sender_ip = None):
        msg:dict = json.loads(message)
        source_node = int(msg.get("source_node_id"))
        is_owner = source_node == self.node.node.long_id

        if is_owner:
            await super().relay(message,sender_ip)
            return

        paired = await self.get_status()
        is_pairing_request = msg.get("syn")=="pair"
        leader = await self.is_leading_peer(self.node.node.long_id,source_node)
        
        if is_pairing_request and not leader and not paired:
            # is selfish to make sure that it is able to take this request
            print("[a_network.relay] keeping the request to itself")
            return
        else:
            await super().relay(message,sender_ip)
            return
    
    async def set_status(self,value:bool):
        async with self.paired_lock:
            self.paired = value

    async def get_status(self):
        async with self.paired_lock:
            return self.paired
