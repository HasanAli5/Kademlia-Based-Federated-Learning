import asyncio
from kademlia.network import Server
from broadcast import *
from broadcast_alt import Broadcast as aBroadcast
from network import get_host
import json

class Aggregate_Stage():

    def __init__(self,a_broadcast):
        # we use this to relay back to a peer if finished or dropped from aggregration
        # along with keeping track with the aggregation loop
        states = ["aggregate","backup","done"]
        self.paired = False
        self.state = states[0]
        # we can keep track of last peer to check health of (if they have disconnected or aggregated)
        self.peer = None
        loop = asyncio.get_event_loop()
        self.pair_denial = loop.create_task(self.deny_pairing_requests(a_broadcast))
        self.lock = asyncio.Lock()

    def set_paired(self,value:bool):
        with self.lock:
            self.paired = value

    def get_paired(self):
        with self.lock:
            return self.paired
        
    def set_peer(self,value:tuple):
        with self.lock:
            self.peer = value

    def get_peer(self):
        with self.lock:
            return self.peer
    
    ## pre-aggregation steps

    async def aggregate_request(self,broadcast:Broadcast):
        data = {
            'request':'aggregate'
        }
        data = broadcast.make_message(broadcast,relay=True,extra_data=data)
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
                msg:dict = await broadcast.convert_message(message)
                if msg.get("response") == "deny_aggregate":
                    print("[await_aggregate_response] deny was recieved")
                    return True
            await asyncio.sleep(5)
            ticker = ticker - 5
            print(f"[await_aggregate_response] time left {ticker} seconds")
        return False
    
    async def send_leader_request(self,broadcast:Broadcast):
        data = {
            'request':'leader_query'
        }
        data = broadcast.make_message(broadcast,relay=True,extra_data=data)
        try:
            await broadcast.relay(json.dumps(data))
        except:
            print("[is_leader] leader request not sent")

    async def deny_leader_request(self,broadcast:Broadcast,ns_number):
        try:
            while True:
                messages = await broadcast.get_messages()
                for message in messages:
                    msg:dict = await broadcast.convert_message(message)
                    if msg.get('request') == "leader_query":
                        print("[is_leader] found leader request")
                        ip = msg.get('source_ip')
                        port = msg.get('source_port')
                        # if this node has better node id (closer to ns_number)
                        print(f"{broadcast.node.node.long_id} vs {msg.get('node_id')}")
                        if abs(ns_number-broadcast.node.node.long_id)<abs(ns_number-int(msg.get('node_id'))):
                            data = {
                                'response':'deny_leader'
                            }
                            data = broadcast.make_message(relay=False,extra_data=data)
                            await broadcast.send(ip,port,json.dumps(data))
                            await broadcast.delete_and_ignore_message(message)
                            print(f"[deny_leader_request] leader request from {ip}:{port} is denied")
                        else:
                            print("[is_leader] other node id is closer to ns_number")
                            data = {
                                'response':'accept_leader',
                            }
                            data = broadcast.make_message(relay=False,extra_data=data)
                            await broadcast.send(ip,port,json.dumps(data))
                            await broadcast.delete_and_ignore_message(message)
                            print(f"[deny_leader_request] leader request from {ip}:{port} is accepted")
                await asyncio.sleep(2)
        except:
            print("[is_leader] deny request failed")

    async def await_leader_response(self,broadcast:Broadcast):
        ticker = 30
        freq = 5
        while ticker > 0:
            print("[await_leader_response] waiting for leader response")
            messages = await broadcast.get_messages()
            for message in messages:
                msg:dict = await broadcast.convert_message(message)
                if msg.get('response') == "deny_leader":
                    print("[await_leader_response] deny was recieved")
                    return False
                elif msg.get('response') == "accept_leader":
                    print("[await_leader_response] accept was recieved")
                    # add extra time to ticker
                    ticker = ticker + 15
                    # add to ignore list
                    await broadcast.delete_and_ignore_message(message)
            await asyncio.sleep(freq)
            ticker = ticker - freq
            print(f"[await_leader_response] time left {ticker} seconds")
        # will default to true on timeout
        return True

    async def is_leader(self,broadcast:Broadcast,ns_number):
        # this is required when multiple nodes finish last at the same time so the timeout should be small like 30 seconds.

        loop = asyncio.get_event_loop()

        # this send a leader request
        await self.send_leader_request(broadcast)

        # start deny if this node is closer to ns_number
        deny_task = loop.create_task(self.deny_leader_request(broadcast,ns_number))

         # we then await for other people who want to be leader and decide who shall
        is_leader = await self.await_leader_response(broadcast)
        deny_task.cancel()
        try: await deny_task
        except asyncio.CancelledError: print("[is_leader] Stopped waiting for deny_task to cancel")

        return is_leader

    ## starting aggregation network
    
    async def send_join_request(self,broadcast:Broadcast,aggregation_port):
        #require port parameter
        data = {
            'request':'join_aggregation',
            'aggregation_port':f"{aggregation_port}",
        }
        data = broadcast.make_message(relay=True,extra_data=data)
        try:
            await broadcast.relay(json.dumps(data))
        except:
            print("[send_join_request] join request not sent")
        
    async def wait_for_leader(self,broadcast:Broadcast):
        freq = 3
        # this will read the broadcast buffer and check if there is a connect request and exit
        while True:
            messages = await broadcast.get_messages()
            for message in messages:
                msg:dict = await broadcast.convert_message(message)
                if msg.get('request') == 'join_aggregation':
                    print("[wait_for_leader] found join request")
                    leader_ip = msg.get('source_ip')
                    leader_port = msg.get('aggregation_port')
                    return leader_ip,leader_port
            await asyncio.sleep(freq)

    ## main aggregation steps
    # uses aggregation network (aBroadcast)

    async def send_pair_request(self,broadcast:aBroadcast):
        # broadcasts pair request to all nodes
        data = {
            'request':'pair',
        }
        data = await broadcast.make_message(relay=True,extra_data=data)
        try:
            await broadcast.relay(json.dumps(data))
        except:
            print("[pairing] pairing request not sent")

    async def send_pair_response(self,broadcast:aBroadcast,temp_peer):
        # only response if interested
        ip,port,node_id = temp_peer
        data = {
            'response':"pair"
        }
        data = await broadcast.make_message(relay=False,extra_data=data)
        try:
            await broadcast.send(ip,port,data)
        except:
            print("[send_pair_response] response not sent")

    async def accept_pair_response(self,broadcast:aBroadcast,temp_peer):
        # informs the pair that the pairing has been accepted by the peer
        # both node are now paired once sent for aggregation
        ip,port,node_id = temp_peer
        data = {
            'response':"accept_pair"
        }
        data = await broadcast.make_message(relay=False,extra_data=data)
        try:
            await broadcast.send(ip,port,data)
        except:
            print("[accept_pair_response] response not sent")

    async def deny_pair_response(self,broadcast:aBroadcast,temp_peer):
        # this denies pair response so they can move unlock from peer
        ip,port,node_id = temp_peer
        data = {
            'response':"deny_pair",
        }
        data = await broadcast.make_message(relay=False,extra_data=data)
        try:
            await broadcast.send(ip,port,data)
        except:
            print("[deny_pair_response] response not sent")

    def is_leading_peer(self,node_id,peer_node_id,ns_number):
        # leading peer is closest to ns_number
        distance = abs(ns_number-node_id)
        peer_distance = abs(ns_number-peer_node_id)
        is_leading_peer = False
        # tie breaker term
        if distance == peer_distance:
            # default to greater number wins leader
            is_leading_peer = node_id > peer_node_id
        else:
            # leader if closer to ns_number
            is_leading_peer = peer_distance > distance
        return is_leading_peer


    async def pairing_handler(self,broadcast:aBroadcast,ns_number):
        # handler both host and client sides
        temp_peer = None
        connection,con_timer = 15
        recast,recast_timer = 45
        freq = 0.5
        while True:
            messages = await broadcast.get_messages()
            for message in messages:
                msg:dict = await broadcast.convert_message(message)
                # check for requests
                if msg.get('request') == 'pair':
                    print("[response_handler] found pair request")
                    peer_ip = msg["source_ip"]
                    peer_port = msg["source_port"]
                    peer_node_id = msg["source_node_id"]

                    if not self.get_paired() and not self.is_leading_peer(broadcast.node.node.long_id,peer_node_id,ns_number):
                        temp_peer = (peer_ip,peer_port,peer_node_id)
                        # accept
                        self.send_pair_response(broadcast,temp_peer)
                        # lock in for that response
                        self.set_paired(True)
                        broadcast.set_status(True)
                        # delete from broadcast list
                        await broadcast.delete_and_ignore_message(message)
                # check for response from own request
                elif msg.get('response') == 'pair':
                    print("[response_handler] found pair response")
                    peer_ip = msg["source_ip"]
                    peer_port = msg["source_port"]
                    peer_node_id = msg["source_node_id"]

                    # if not paired and is smaller distance from ns_number
                    if (not self.get_paired() or (temp_peer and temp_peer[2] == peer_node_id)) and self.is_leading_peer(broadcast.node.node.long_id,peer_node_id,ns_number):
                        temp_peer = (peer_ip,peer_port,peer_node_id)
                        # accept pairing
                        # send acceptance back
                        self.accept_pair_response(broadcast,temp_peer)

                        await broadcast.delete_and_ignore_message(message)

                        self.set_paired(True)
                        broadcast.set_status(True)

                        self.set_peer(temp_peer)
                        return self.get_peer()
                    
                    elif self.get_paired():
                        self.deny_pair_response(broadcast,(peer_ip,peer_port,peer_node_id))
                        await broadcast.delete_and_ignore_message(message)

                # check response from peer response
                elif msg.get('response') == 'accept_pair':
                    await broadcast.delete_and_ignore_message(message)
                    self.set_paired(True)
                    broadcast.set_status(True)
                    self.set_peer(temp_peer)
                    return self.get_peer()
                elif msg.get('response') == 'deny_pair':
                    self.set_paired(False)
                    broadcast.set_status(False)
                    temp_peer = None
                    con_timer = connection
                    await broadcast.delete_and_ignore_message(message)
            await asyncio.sleep(freq)
            # timeout function
            recast_timer = recast_timer - freq
            if recast_timer < 0:
                # resends if it has been a while
                self.send_pair_request(broadcast)
                recast_timer = recast
            if temp_peer is not None:
                con_timer = con_timer - freq
                if con_timer < 0:
                    # depair
                    self.set_paired(False)
                    broadcast.set_status(False)
                    temp_peer = None
                    con_timer = connection

    async def pairing(self,broadcast:aBroadcast,ns_number):

        loop = asyncio.get_event_loop()

        peer_node = None
        # send request to all nodes (advertise)
        await self.send_pair_request(broadcast)
        
        # get peer tuple
        peer_node = await self.pairing_handler(broadcast,ns_number)

        return peer_node

    async def deny_pairing_requests(self,broadcast:aBroadcast):
        # after obtaining pairing we deny as much as possible
        try: 
            while True:
                messages = await broadcast.get_messages()
                for message in messages:
                    msg:dict = await broadcast.convert_message(message)
                    if msg.get("response") == "pair":
                        ip = msg.get("source_ip")
                        port = msg.get("source_port")
                        node_id = msg.get("source_node_id")
                        self.deny_pair_response(broadcast,(ip,port,node_id))
                        print(f"[deny_pairing_requests] {ip}:{port} ({node_id}) was denied")
        except:
            print("[deny_pairing_requests] stopped")

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
    
    async def check_peer(self):
        pass

    async def backup_peer(self):
        while True:
            response = await self.check_peer()
            if response == None:
                # bad news as peer has disconnected
                # so we join back in to aggregation
                pass

    async def check_if_last(self,broadcast:aBroadcast):
        nodes = []
        # does a neighbour check if there are any then it 
        nodes = broadcast.node.protocol.router.find_neighbors(broadcast.node.node)
        if len(nodes) == 0:
            return True
        else:
            return False

    async def core_aggregation(self,broadcast:aBroadcast,model,ns_number):
        loop = asyncio.get_event_loop()
        dropped = False
        # starts off at one adds more with each aggregation.
        weighting = 1
        while not dropped:
            #stopping pair denial as we are looking for pairs
            self.pair_denial.cancel()
            try: await self.pair_denial
            except asyncio.CancelledError: pass
            #pairing procedure
            await self.pairing(broadcast,ns_number)
            # restarting pair_denial
            self.pair_denial = loop.create_task(self.deny_pairing_requests(broadcast))

            peer_ip,peer_port,peer_node_id = self.get_peer()

            #then send/recieve model.
            if self.is_leading_peer(broadcast.node.node.long_id,peer_node_id,ns_number):
                # closer so we send to then get back the model
                await self.send_model()
                peer_model= await self.recieve_model()
            elif not self.is_leading_peer(broadcast.node.node.long_id,peer_node_id,ns_number):
                #if reciever then aggregate
                peer_model = await self.recieve_model()

                await self.send_model()
                dropped = True
                return False,model
            if await self.check_if_last():
                # if last then send model out
                return True,model


    async def aggregation(self,broadcast:aBroadcast,model,ns_number):
        timeout = 5
        while True:
            if self.state == self.states[0]:
                is_last,model = await self.core_aggregation(broadcast,model,ns_number)
                #aggregate
                if is_last:
                    #returns global model to share
                    return model
            elif self.state == self.states[1]:
                #backup
                response = await self.check_peer()
                if response == None:
                    # if no response join back and do aggregation instead of peer
                    self.state = self.states[0]
                elif response["response"] == self.state[0]:
                    # if they are currently aggregating then keep this state
                    pass
                elif response["response"] == self.state[1]:
                    # if that node is backing up another node become done
                    self.state = self.state[2]
            elif self.state == self.state[2]:
                # done quits the aggregation cycles
                # return none so it can wait for global model on otherside
                return None
            asyncio.sleep(timeout)
        