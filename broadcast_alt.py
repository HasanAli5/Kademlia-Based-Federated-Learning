from sympy import false

from broadcast import Broadcast as BaseBroadcast
from asyncio import Lock
import json

class Broadcast(BaseBroadcast):
    # this adapts the base broadcasting system for the aggregation network
    # we want to stop relaying for pairing.
    # where we want to relay only if already paired to reduce network traffic.

    def __init__(self, node, port, max_length):
        super().__init__(node, port, max_length)
        self.paired = False
        self.lock = Lock()
        

    async def relay(self, message:str):
        msg:dict = json.loads(message)
        
        paired = await self.get_status()
        is_pairing_request = msg.get("request")=="pair"
        
        source_node = msg.get("source_node_id")
        if source_node:
            is_owner = source_node == self.node.node.long_id
        else:
            is_owner = False

        if is_pairing_request and not paired and not is_owner:
            print(f"[a_broadcast.relay] node is being selfish. Pair Request {is_pairing_request} ,Paired : {paired}, Own message : {is_owner}")
            return
        else:
            print(f"[a_broadcast.relay] Pair Request {is_pairing_request} ,Paired : {paired}, Own message : {is_owner}")
            await super().relay(message)
    
    async def set_status(self,value:bool):
        async with self.lock:
            self.paired = value

    async def get_status(self):
        async with self.lock:
            return self.paired
