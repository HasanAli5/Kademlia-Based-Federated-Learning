import asyncio
from kademlia.network import Server
from broadcast import *
from network import get_host
import json

class Aggregate_Stage():

    def __init__(self):
        self.aggregated = False
        self.peer = None
        self.lock = asyncio.Lock()

    async def aggregate_request(self,node:Server,broadcast:Broadcast):
        nodes = node.protocol.router.find_neighbors(node.node)
        for n in nodes:
            data = {
                'source_ip':f'{get_host()}',
                'request':'aggregate',
                'relay':True
            }
            try:
                await broadcast.send(n.ip,json.dumps(data))
            except:
                print(f"[aggregate_request] aggregate requests failed to send to {n.ip}")
        print("[aggregate_request] aggregate requests sent")

    async def await_aggregate_response(self,broadcast:Broadcast):
        ticker = 30
        while ticker > 0:
            print("[await_aggregate_response] waiting for aggregate response")
            messages = await broadcast.get_messages()
            for message in messages:
                msg = json.loads(message)
                if 'response' in msg.keys() and msg["response"] == "deny_aggregate":
                    print("[await_aggregate_response] deny was sent")
                    return True
            await asyncio.sleep(5)
            ticker = ticker - 5
            print(f"[await_aggregate_response] time left {ticker} seconds")
        return False
    
    async def is_leader(ns_number):
        # this send a leader request
        # we then await for other people who want to be leader and decide who shall
        # based on node id.
        # this is required when multiple nodes finish last at the same time so the timeout should be small like 30 seconds.
        pass

    async def wait_for_leader():
        # this will read the broadcast buffer and check if there is a connect request
        pass