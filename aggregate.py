import asyncio
from kademlia.network import Server
import math
from broadcast import *
from network import get_host
import json

class Aggregate_Stage():

    def __init__(self):
        self.aggregated = False
        self.dropped = False
        self.peer = None
        self.lock = asyncio.Lock()

    async def aggregate_request(self,broadcast:Broadcast):
        data = {
            'source_ip':f'{get_host()}',
            'source_port':f'{broadcast.port}',
            'request':'aggregate',
            'relay':True
        }
        try:
            await broadcast.relay(json.dumps(data))
        except:
            print(f"[aggregate_request] aggregate requests failed to send")
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
    
    async def send_leader_request(self,node:Server,broadcast:Broadcast):
        data = {
            'source_ip':f'{get_host()}',
            'source_port':f'{broadcast.port}',
            'request':'leader_request',
            'node_id':f"{node.node.long_id}",
            'relay':True
        }
        try:
            await broadcast.relay(json.dumps(data))
        except:
            print("[is_leader] leader request not sent")

    async def deny_leader_request(self,node:Server,broadcast:Broadcast,ns_number):
        try:
            while True:
                messages = await broadcast.get_messages()
                for message in messages:
                    msg = json.loads(message)
                    if 'request' in msg.keys() and msg["request"] == "leader_request":
                        print("[is_leader] found leader request")
                        ip = msg["source_ip"]
                        port = msg["source_port"]
                        # if this node has better node id (closer to ns_number)
                        print(f"{node.node.long_id} vs {msg["node_id"]}")
                        if abs(ns_number-node.node.long_id)<abs(ns_number-int(msg["node_id"])):
                            data = {
                                'source_ip':f'{get_host()}',
                                'source_port':f'{broadcast.port}',
                                'destination_ip':f"{ip}",
                                'destination_port':f"{port}",
                                'response':'deny_leader',
                                'relay':False
                            }
                            await broadcast.send(ip,port,json.dumps(data))
                            await broadcast.ignore_message(message)
                            await broadcast.delete_message(message)
                            print(f"[deny_leader_request] leader request from {ip}:{port} is denied")
                        else:
                            print("[is_leader] other node id is closer to ns_number")
                            data = {
                                'source_ip':f'{get_host()}',
                                'source_port':f'{broadcast.port}',
                                'destination_ip':f"{ip}",
                                'destination_port':f"{port}",
                                'response':'accept_leader',
                                'relay':False
                            }
                            await broadcast.send(ip,port,json.dumps(data))
                            await broadcast.ignore_message(message)
                            await broadcast.delete_message(message)
                            print(f"[deny_leader_request] leader request from {ip}:{port} is accepted")
                await asyncio.sleep(2)
        except:
            print("[is_leader] deny request failed")

    async def await_leader_response(self,broadcast:Broadcast):
        ticker = 30
        while ticker > 0:
            print("[await_leader_response] waiting for leader response")
            messages = await broadcast.get_messages()
            for message in messages:
                msg = json.loads(message)
                if 'response' in msg.keys() and msg["response"] == "deny_leader":
                    print("[await_leader_response] deny was recieved")
                    return False
                elif 'response' in msg.keys() and msg["response"] == "accept_leader":
                    print("[await_leader_response] accept was recieved")
                    # add extra time to ticker
                    ticker = ticker + 15
                    # add to ignore list
                    await broadcast.ignore_message(message)
                    await broadcast.delete_message(message)
            await asyncio.sleep(5)
            ticker = ticker - 5
            print(f"[await_leader_response] time left {ticker} seconds")
        # will default to true on timeout
        return True

    async def is_leader(self,node:Server,broadcast:Broadcast,ns_number):
        # this is required when multiple nodes finish last at the same time so the timeout should be small like 30 seconds.

        loop = asyncio.get_event_loop()

        # this send a leader request
        await self.send_leader_request(node,broadcast)

        # start deny if this node is closer to ns_number
        deny_task = loop.create_task(self.deny_leader_request(node,broadcast,ns_number))

         # we then await for other people who want to be leader and decide who shall
        is_leader = await self.await_leader_response(broadcast)
        deny_task.cancel()
        try: await deny_task
        except asyncio.CancelledError: print("[is_leader] Stopped waiting for deny_task to cancel")

        return is_leader
    
    async def send_join_request(self,broadcast:Broadcast,port):
        data = {
            'source_ip':f'{get_host()}',
            'source_port':f"{broadcast.port}",
            'request':'join_request',
            'leader_ip':f"{get_host()}",
            'leader_port':f"{port}",
            'relay':True
        }
        try:
            await broadcast.relay(json.dumps(data))
        except:
            print("[send_join_request] join request not sent")
        
    async def wait_for_leader(self,broadcast:Broadcast):
        # this will read the broadcast buffer and check if there is a connect request and exit
        while True:
            messages = await broadcast.get_messages()
            for message in messages:
                msg = json.loads(message)
                if 'request' in msg.keys() and msg['request'] == 'join_request':
                    print("[wait_for_leader] found join request")
                    leader_ip = msg["leader_ip"]
                    leader_port = msg["leader_port"]
                    return leader_ip,leader_port
            await asyncio.sleep(3)


    async def aggregation(self,aggregation_node):
        while not self.dropped:
            #pairing procedure

            #then send/recieve model.

            #if recieved model then aggregate

            #if sent then get ready to recieve

            #drop or continue to next step.
            pass
        pass

    async def pairing(self):
        # send request to all nodes (advertise)
        # get response (we can or not)
        # if ns_number and nodeid is far then you will get the most favourable response
        # must be picked from furthest first from requests.
        # if the response is denied then continue pairing cycle.
        pass

    async def send_model(self):
        # ns_number indicates who drops out or not of the pair. (furthest drops)
        # closes stays in network so sends first then recieves

        pass

    async def recieve_model(self):
        # ns_number indicates who drops out or not of the pair. (furthest drops)
        # furthest does the aggregation so recieves first then sends
        pass

    async def FedAvg(self,model1,model2):
        #not real
        avg = (model1+model2)/2
        pass