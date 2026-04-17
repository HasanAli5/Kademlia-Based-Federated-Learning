from broadcast import Broadcast as BaseBroadcast
from asyncio import Lock

class Broadcast(BaseBroadcast):
    # this adapts the base broadcasting system for the aggregation network
    # we want to stop relaying for pairing.
    # where we want to relay only if already paired to reduce network traffic.

    async def __init__(self, node, port, max_length):
        self.paired = False
        self.lock = Lock()
        super().__init__(node, port, max_length)

    async def relay(self, message):
        with self.lock:
            if self.paired == True:
                return await super().relay(message)
    
    async def set_status(self,value:bool):
        with self.lock:
            self.paired = value

