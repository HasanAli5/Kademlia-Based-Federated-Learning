import asyncio

from numpy import double
from network import *
from aggregation_network import Network as AggNetwork
import json
import copy

class Aggregate():

    def __init__(self,network:Network,aggregation_network_port:int,aggregation_kademlia_port,model_port:int):
        # we use this to relay back to a peer if finished or dropped from aggregration
        # along with keeping track with the aggregation loop
        self.paired = False
        #self.states = ("aggregating","backup","exited")
        #self.state = self.states[0]
        # we can keep track of last peer to check health of (if they have disconnected or aggregated)
        self.peer = None
        # we set denial task later
        self.pair_denial = None
        # locks
        self.paired_lock = asyncio.Lock()
        self.peer_lock = asyncio.Lock()

        self.network = network
        self.a_network = None

        self.a_relay_port = aggregation_network_port
        self.a_kademlia_port = aggregation_kademlia_port
        self.model_port = model_port

        self.aggregate_ksize = 5

        self.stage_data = {
            'stage':'aggregate'
        }
    
    def set_denial_task(self,cooldown:int):
        self.pair_denial = asyncio.create_task(self.deny_pairing_requests(cooldown))

    async def stop_denial_task(self):
        if self.pair_denial:
            self.pair_denial.cancel()
            try: await self.pair_denial
            except asyncio.CancelledError: print("[stop_denial_task] pair_denial stopped")

    async def set_paired(self,value:bool):
        async with self.paired_lock:
            self.paired = value

    async def get_paired(self):
        async with self.paired_lock:
            return self.paired
        
    async def set_peer(self,value:tuple|None):
        async with self.peer_lock:
            self.peer = value

    async def get_peer(self):
        async with self.peer_lock:
            return self.peer
        
    ## sync phase

    async def send_leader_request(self):
        data = {
            'request':'leader'
        }
        data.update(self.stage_data)
        data = await self.network.make_message(relay=True,extra_data=data)
        try:
            message = json.dumps(data)
            await self.network.relay(message)
            print("[send_leader_request] leader request set")
            return data.get("timestamp")
        except Exception as e:
            print(f"[send_leader_request] Exception : {e}")

    async def send_join_request(self,relay_port:int,kademlia_port:int,model_port:int):
        #require port parameter
        data = {
            'request':'join_aggregate',
            'relay_port':relay_port,
            'kademlia_port':kademlia_port,
            'model_port':model_port
        }
        data.update(self.stage_data)
        data = await self.network.make_message(relay=True,extra_data=data)
        try:
            await self.network.relay(json.dumps(data))
            print(f"[send_join_request] join request relayed")
            return data.get("timestamp")
        except Exception as e:
            print(f"[send_join_request] Exception : {e}")

    # task responder

    async def send_leader_response(self,cooldown:int):

        pending_response = set()

        deny_data = {
            'response':'deny_leader',
            }
        deny_data.update(self.stage_data)
        accept_data = {
            'response':'accept_leader',
            }
        accept_data.update(self.stage_data)
        try:
            while True:

                leader_query_list = await self.network.find_messages({"request":"leader"})

                for message,msg in leader_query_list:
                    ip = msg.get('source_ip')
                    port = msg.get('source_port')
                    timestamp = msg.get('timestamp')
                    if ip and port and timestamp:
                        # if this node has better node id (closer to ns_number)
                        print(f"[deny_leader_request] {self.network.node.node.long_id} vs {msg.get('source_node_id')}")
                        if await self.network.is_leading_peer(self.network.node.node.long_id,int(msg.get('source_node_id'))):
                            data = await self.network.make_message(relay=False,extra_data=deny_data)
                            data = self.network.attach_request_info(data,timestamp)
                            task = asyncio.shield(self.network.send(ip,port,json.dumps(data)))
                            pending_response.add(task)
                            task.add_done_callback(pending_response.discard)
                            print(f"[deny_leader_request] leader request from {ip}:{port} is denied")
                        else:
                            data = await self.network.make_message(relay=False,extra_data=accept_data)
                            data = self.network.attach_request_info(data,timestamp)
                            task = asyncio.shield(self.network.send(ip,port,json.dumps(data)))
                            pending_response.add(task)
                            task.add_done_callback(pending_response.discard)
                            print(f"[deny_leader_request] leader request from {ip}:{port} is accepted")

                await self.network.clean_up_from_list(leader_query_list)

                await asyncio.sleep(cooldown)

        except Exception as e:
            print(f"[deny_leader_request] Exception : {e}")
        finally:
            if pending_response:
                await asyncio.gather(*pending_response)


    async def await_leader_response(self,response_wait:int,exit_wait:int,cooldown:int):
        try:
            
            DENIED_BY_TRAIN = 1
            DENIED_BY_LEADER = 2
            ACCEPTED_AS_LEADER = 3
            NO_RESPONSE_YET = 4

            double_check = False

            # this send a leader request
            timestamp = await self.send_leader_request()
            dc_timestamp = None
            state = NO_RESPONSE_YET

            await asyncio.sleep(cooldown)

            recast_deadline = time.time() + response_wait
            exit_deadline = None

            while True:
                accept_leader_list = await self.network.find_messages({"response":"accept_leader"})
                deny_leader_list = await self.network.find_messages({"response":"deny_leader"})
                deny_aggregate_list = await self.network.find_messages({"response":"deny_aggregate"})
                join_request_list = await self.network.find_messages({"request":"join_aggregate"})

                if len(join_request_list)>0:
                    print("[await_leader_response] found go request get out of here!")
                    await self.network.clean_up_from_list(accept_leader_list)
                    await self.network.clean_up_from_list(deny_leader_list)
                    await self.network.clean_up_from_list(deny_aggregate_list)
                    await self.network.clean_up_from_list(join_request_list)
                    _,msg = join_request_list[0]
                    return msg

                elif len(deny_aggregate_list)>0:
                    for _,msg in  deny_aggregate_list:
                        ts = msg.get("request_timestamp")
                        if ts == timestamp or ts == dc_timestamp:
                            # there are people still training just wait
                            print("[await_leader_response] found deny aggregate")
                            
                            # stop the exit
                            exit_deadline = None
                            double_check = False
                            recast_deadline = time.time() + response_wait
                            state = DENIED_BY_TRAIN
                            break
                    await self.network.clean_up_from_list(accept_leader_list)
                    await self.network.clean_up_from_list(deny_leader_list)
                    await self.network.clean_up_from_list(deny_aggregate_list)

                elif len(deny_leader_list)>0 and state > DENIED_BY_TRAIN:
                    for _,msg in deny_leader_list:
                        ts = msg.get("request_timestamp")
                        if ts  == timestamp or ts == dc_timestamp:
                            print("[await_leader_response] found deny leader")
                            exit_deadline = None
                            double_check = False
                            recast_deadline = time.time() + response_wait
                            state = DENIED_BY_LEADER
                            break
                    await self.network.clean_up_from_list(accept_leader_list)
                    await self.network.clean_up_from_list(deny_leader_list)
                
                elif len(accept_leader_list)>0 and state > DENIED_BY_LEADER:
                    for _,msg in accept_leader_list:
                        ts = msg.get("request_timestamp")
                        if ts  == timestamp or ts == dc_timestamp:
                            print("[await_leader_response] found accept leader")
                            exit_deadline = time.time() + exit_wait
                            recast_deadline = None
                            state = ACCEPTED_AS_LEADER
                            break
                    await self.network.clean_up_from_list(accept_leader_list)

                if recast_deadline and time.time() > recast_deadline:
                    timestamp = await self.send_leader_request()
                    recast_deadline = time.time() + response_wait
                    state = NO_RESPONSE_YET

                if exit_deadline and time.time() > exit_deadline:
                    if not double_check:
                        double_check = True
                        dc_timestamp = await self.send_leader_request()
                        exit_deadline = time.time() + exit_wait
                        state = NO_RESPONSE_YET
                    else:
                        return True
                
                await asyncio.sleep(cooldown)
        except Exception as e:
            print(f"[await_leader_response] Exception : {e}")

    async def sync_and_join_aggregation(self):

        # start deny if this node is closer to ns_number
        responder = asyncio.create_task(self.send_leader_response(cooldown=2))

         # we then await for other people who want to be leader and decide who shall
        response = await self.await_leader_response(response_wait=15,exit_wait=30,cooldown=2)

        try:
            if response is True:
                if type(self.a_relay_port) is int and type(self.model_port) is int:
                    self.aggregation_node = Server(ksize=self.aggregate_ksize)
                    await self.network.create(node=self.aggregation_node,node_port=self.a_kademlia_port)
                    self.a_network = AggNetwork(node=self.aggregation_node,port=self.a_relay_port,buffer_length=100,messages_length=100,ignores_length=100,model_transfer_port=self.model_port)
                    ns,ts = await self.network.get_ns_number()
                    data = {
                        "ns_number":ns,
                        "ns_number_ts":ts
                    }
                    await self.a_network.set_ns_number(ns,ts)
                    await self.reset_peer_state()
                    await self.send_join_request(relay_port=self.a_relay_port,kademlia_port=self.a_kademlia_port,model_port=self.model_port)
                    print(f"\n[sync_and_join_aggregation] Started Aggregation Network\n")
                else: raise TypeError

            elif type(response) is dict:
                peer_ip = response.get("source_ip")
                peer_port = response.get("kademlia_port")
                self.a_relay_port = response.get("relay_port")
                self.model_port = response.get("model_port")
                if type(self.a_relay_port) is int and type(self.model_port) is int:
                    self.aggregation_node = Server(ksize=self.aggregate_ksize)
                    await self.network.connect(node=self.aggregation_node,node_port=self.a_kademlia_port,peer_ip=peer_ip,peer_port=peer_port)
                    self.a_network = AggNetwork(node=self.aggregation_node,port=self.a_relay_port,buffer_length=100,messages_length=100,ignores_length=100,model_transfer_port=self.model_port)
                    await self.reset_peer_state()
                    print(f"\n[sync_and_join_aggregation] Connecting to Aggregation Network at {peer_ip}:{peer_port}\n")
                else: raise TypeError
        except Exception as e:
            raise e

        responder.cancel()
        try: await responder
        except asyncio.CancelledError: print("[is_leader] responder stopped")
        return

    ## main aggregation steps
    # uses aggregation network (aBroadcast)

    async def send_pair_request(self):
        if self.a_network is None: raise TypeError
        # broadcasts pair request to all nodes
        data = {
            'syn':'pair'
        }
        data.update(self.stage_data)
        data = await self.a_network.make_message(relay=True,extra_data=data)
        try:
            message = json.dumps(data)
            await self.a_network.relay(message)
            print(f"[send_pair_request] relayed")
            return data.get("timestamp")
        except Exception as e:
            print(f"[send_pair_request] Exception : {e}")

    async def send_direct_pair_request(self,temp_peer:tuple):
        if self.a_network is None: raise TypeError
        # send a pair request to specific node
        # incase the kademlia system is uneven and only one side
        # is able to receive the request
        ip,port,node_id = temp_peer
        data = {
            'syn':'pair'
        }
        data.update(self.stage_data)
        data = await self.a_network.make_message(relay=True,extra_data=data)
        try:
            message = json.dumps(data)
            await self.a_network.send(ip,port,message)
            print(f"[send_pair_request] relayed")
            return data.get("timestamp")
        except Exception as e:
            print(f"[send_pair_request] Exception : {e}")

    async def send_pair_response(self,temp_peer:tuple,request_timestamp):
        if self.a_network is None: raise TypeError
        # only response if interested
        ip,port,node_id = temp_peer
        data = {
            'syn-ack':'pair'
        }
        data.update(self.stage_data)
        data = await self.a_network.make_message(relay=False,extra_data=data)
        self.a_network.attach_request_info(data,request_timestamp)
        try:
            message = json.dumps(data)
            await self.a_network.send(ip,port,message)
            print(f"[send_pair_response] sent to {ip}:{port}")
            return data.get("timestamp")
        except Exception as e:
            print(f"[send_pair_response] Exception : {e}")

    async def accept_pair_response(self,temp_peer:tuple,request_timestamp):
        if self.a_network is None: raise TypeError
        # informs the pair that the pairing has been accepted by the peer
        # both node are now paired once sent for aggregation
        ip,port,node_id = temp_peer
        data = {
            'ack':'accept_pair'
        }
        data.update(self.stage_data)
        data = await self.a_network.make_message(relay=False,extra_data=data)
        self.a_network.attach_request_info(data,request_timestamp)
        try:
            message = json.dumps(data)
            await self.a_network.send(ip,port,message)
            print(f"[accept_pair_response] sent to {ip}:{port}")
            return data.get("timestamp")
        except Exception as e:
            print(f"[accept_pair_response] Exception : {e}")

    async def deny_pair_response(self,temp_peer:tuple,request_timestamp):
        if self.a_network is None: raise TypeError
        # this denies pair response so they can move unlock from peer
        ip,port,node_id = temp_peer
        data = {
            'ack':'deny_pair'
        }
        data.update(self.stage_data)
        data = await self.a_network.make_message(relay=False,extra_data=data)
        self.a_network.attach_request_info(data,request_timestamp)
        try:
            message = json.dumps(data)
            await self.a_network.send(ip,port,message)
            print(f"[deny_pair_response] sent to {ip}:{port}")
            return data.get("timestamp")
        except Exception as e:
            print(f"[deny_pair_response] Exception : {e}")

    async def reset_peer_state(self):
        if self.a_network is None: raise TypeError
        # cleanup before repairing
        await self.set_paired(False)
        await self.a_network.set_status(False)
        await self.set_peer(None)

    async def set_peer_state(self,peer:tuple,paired_bool:bool,network_paired_bool:bool):
        if self.a_network is None: raise TypeError
        # cleanup before repairing
        await self.set_paired(paired_bool)
        await self.a_network.set_status(network_paired_bool)
        await self.set_peer(peer)

    async def pairing(self,recast_wait:int,connect_wait:int,check_last_wait:int,cooldown:int):
        if self.a_network is None: raise TypeError
        # handler both host and client sides
        deny_pairings_list = await self.a_network.find_messages({"ack":"deny_pair"})
        accept_pairings_list = await self.a_network.find_messages({"ack":"accept_pair"})
        pair_syn_ack_list = await self.a_network.find_messages({"syn-ack":"pair"})
        pair_syn_list = await self.a_network.find_messages({"syn":"pair"})
        await self.a_network.clean_up_from_list(accept_pairings_list)
        await self.a_network.clean_up_from_list(deny_pairings_list)
        await self.a_network.clean_up_from_list(pair_syn_ack_list)
        await self.a_network.clean_up_from_list(pair_syn_list)
        try:
            print("[pairing] started")
            # set all peer states for pairing
            await self.reset_peer_state()
            await self.send_pair_request()
            recast_deadline = time.time() + recast_wait
            check_last_deadline = time.time() + check_last_wait
            connection_deadline = None

            while True:
                print("[pairing] check started")

                deny_pairings_list = await self.a_network.find_messages({"ack":"deny_pair"})
                accept_pairings_list = await self.a_network.find_messages({"ack":"accept_pair"})
                pair_syn_ack_list = await self.a_network.find_messages({"syn-ack":"pair"})
                pair_syn_list = await self.a_network.find_messages({"syn":"pair"})


                # go through accepts
                if len(accept_pairings_list)>0:
                    print("[response_handler] found pair accept")
                    _,msg = accept_pairings_list[0]
                    peer = (msg.get("source_ip"),msg.get("source_port"),int(msg.get("source_node_id")))
                    await self.set_peer_state(peer,paired_bool=True,network_paired_bool=True)
                    print("[pairing_handler] pair request accepted")
                    await self.a_network.clean_up_from_list(accept_pairings_list)
                    return True

                # go through denies
                if len(deny_pairings_list)>0:
                    print("[response_handler] found pair deny")
                    await self.reset_peer_state()
                    connection_deadline = None
                    print("[pairing_handler] pair request denied")
                    await self.send_pair_request()
                    recast_deadline = time.time() + recast_wait
                    check_last_deadline = time.time() + check_last_wait
                    print("[pairing_handler] resent pair request after deny")

                # go through pair sync-ack list
                if len(pair_syn_ack_list)>0:
                    for _,msg in pair_syn_ack_list:
                        print("[response_handler] found pair response")
                        peer = (msg.get("source_ip"),msg.get("source_port"),int(msg.get("source_node_id")))
                        ts = msg.get("timestamp")
                        is_leader_peer = await self.a_network.is_leading_peer(self.a_network.node.node.long_id,peer[2])

                        # if not paired and is smaller distance from ns_number
                        if not await self.get_paired() and is_leader_peer:
                            print("[pairing_handler] attempting pairing")
                            await self.accept_pair_response(peer,ts)
                            await self.set_peer_state(peer,paired_bool=True,network_paired_bool=True)
                            await self.a_network.clean_up_from_list(pair_syn_ack_list)
                            return True
                        
                        else:
                            print("[pairing_handler] already paired or not leader")
                            await self.deny_pair_response(peer,ts)
                
                # go through sync requests
                if len(pair_syn_list)>0 and not await self.get_paired():
                    for _,msg in pair_syn_list:
                        print("[response_handler] found pair request")
                        peer = (msg.get("source_ip"),msg.get("source_port"),int(msg.get("source_node_id")))
                        ts = msg.get("timestamp")
                        is_leader_peer = await self.a_network.is_leading_peer(self.a_network.node.node.long_id,peer[2])

                        if not is_leader_peer:
                            print("[pairing_handler] sending response to request")
                            await self.send_pair_response(peer,ts)
                            await self.set_peer_state(peer,paired_bool=True,network_paired_bool=False)
                            recast_deadline = None
                            check_last_deadline = None
                            connection_deadline = time.time() + connect_wait
                            break
                        else:
                            print(f"[pairing_handler] found a pairing request this node is the leader so send own request to it.")
                            await self.send_direct_pair_request(peer)
                    
                await self.a_network.clean_up_from_list(accept_pairings_list)
                await self.a_network.clean_up_from_list(deny_pairings_list)
                await self.a_network.clean_up_from_list(pair_syn_ack_list)
                await self.a_network.clean_up_from_list(pair_syn_list)

                # check last
                if check_last_deadline and time.time() > check_last_deadline:
                    is_last = await self.check_if_last(wait_time=30,cooldown=2)
                    if is_last:
                        return False
                    else:
                        check_last_deadline = time.time() + check_last_wait
                        await self.reset_peer_state()
                        connection_deadline = None
                        await self.send_pair_request()
                        recast_deadline = time.time() + recast_wait
                        await asyncio.sleep(cooldown)
                        continue

                # disconnect
                if connection_deadline and time.time() > connection_deadline:
                    check_last_deadline = time.time() + check_last_wait
                    await self.reset_peer_state()
                    connection_deadline = None
                    await self.send_pair_request()
                    recast_deadline = time.time() + recast_wait
                    await asyncio.sleep(cooldown)
                    print("[pairing_handler] disconnected from ghosted peer")
                    continue
                
                # recast
                if recast_deadline and time.time() > recast_deadline:
                    await self.send_pair_request()
                    recast_deadline = time.time() + recast_wait
                    await asyncio.sleep(cooldown)
                    print("[pairing_handler] resent pair request")
                    continue

                # timeout function
                await asyncio.sleep(cooldown)
                
        except Exception as e:
            print(f"[pairing_handler] Exception : {e}")
        finally:
            deny_pairings_list = await self.a_network.find_messages({"ack":"deny_pair"})
            accept_pairings_list = await self.a_network.find_messages({"ack":"accept_pair"})
            pair_syn_ack_list = await self.a_network.find_messages({"syn-ack":"pair"})
            pair_syn_list = await self.a_network.find_messages({"syn":"pair"})
            await self.a_network.clean_up_from_list(accept_pairings_list)
            await self.a_network.clean_up_from_list(deny_pairings_list)
            await self.a_network.clean_up_from_list(pair_syn_ack_list)
            await self.a_network.clean_up_from_list(pair_syn_list)


    async def shield_wrap(self,coro):
        return await asyncio.shield(coro)

    async def deny_pairing_requests(self,cooldown:int):
        if self.a_network is None: raise TypeError
        pending_response = set()
        # after obtaining pairing we deny as much as possible
        try: 
            while True:
                syn_ack_list = await self.a_network.find_messages({"syn-ack":"pair"})
                for _,msg in syn_ack_list:
                    peer = (msg.get("source_ip"),msg.get("source_port"),int(msg.get("source_node_id")))
                    ts = msg.get("timestamp")
                    task = asyncio.create_task(self.shield_wrap(self.deny_pair_response(peer,ts)))
                    pending_response.add(task)
                    task.add_done_callback(pending_response.discard)
                    print(f"[deny_pairing_requests] {peer[0]}:{peer[1]} ({[peer[2]]}) was denied")
                await self.a_network.clean_up_from_list(syn_ack_list)
                await asyncio.sleep(cooldown)
        except Exception as e:
            print(f"[pairing_handler] Exception : {e}")
        finally:
            if pending_response:
                await asyncio.gather(*pending_response,return_exceptions=True)


    async def FedAvg(self,model1,model1weighting,model2,model2weighting):
        dict1 = model1.state_dict()
        dict2 = model2.state_dict()

        aggregated_dict = copy.deepcopy(dict1)
        aggregated_weights = model1weighting + model2weighting

        for key in aggregated_dict.keys():
            aggregated_dict[key] = (dict1[key] * model1weighting + dict2[key] * model2weighting)/aggregated_weights
        
        aggregated_model = copy.deepcopy(model1)
        aggregated_model.load_state_dict(aggregated_dict)
        return aggregated_model
    
    async def response_not_last(self,cooldown:int):
        if self.a_network is None: raise TypeError
        pending_response = set()
        print("[response_not_last] task started")
        not_last_data = {
            'response':'not_last'
        }
        not_last_data.update(self.stage_data)
        try:
            while True:
                is_last_list = await self.a_network.find_messages({"request":"is_last"})
                for _,msg in is_last_list:
                    print("[response_not_last] found is_last request")
                    peer = (msg.get("source_ip"),msg.get("source_port"),int(msg.get("source_node_id")))
                    ts = msg.get("timestamp")
                    if peer[0] and peer[1] and ts:
                        data = await self.a_network.make_message(relay=False,extra_data=not_last_data)
                        data = self.a_network.attach_request_info(data,ts)
                        task = asyncio.create_task(self.shield_wrap(self.a_network.send(peer[0],peer[1],json.dumps(data))))
                        pending_response.add(task)
                        task.add_done_callback(pending_response.discard)
                await self.a_network.clean_up_from_list(is_last_list)
                await asyncio.sleep(cooldown)
        finally:
            if pending_response:
                await asyncio.gather(*pending_response,return_exceptions=True)

    async def response_not_in(self,cooldown:int):
        if self.a_network is None: raise TypeError
        pending_response = set()
        not_in_data = {
            'response':'not_in'
        }
        not_in_data.update(self.stage_data)
        try:
            while True:
                is_last_list = await self.a_network.find_messages({"request":"is_last"})
                for _,msg in is_last_list:
                    print("[response_not_last] found is_last request")
                    peer = (msg.get("source_ip"),msg.get("source_port"),int(msg.get("source_node_id")))
                    ts = msg.get("timestamp")
                    if peer[0] and peer[1] and ts:
                        data = await self.a_network.make_message(relay=False,extra_data=not_in_data)
                        data = self.a_network.attach_request_info(data,ts)
                        task = asyncio.create_task(self.shield_wrap(self.a_network.send(peer[0],peer[1],json.dumps(data))))
                        pending_response.add(task)
                        task.add_done_callback(pending_response.discard)
                await self.a_network.clean_up_from_list(is_last_list)
                await asyncio.sleep(cooldown)
        finally:
            if pending_response:
                await asyncio.gather(*pending_response,return_exceptions=True)

    async def request_is_last(self):
        if self.a_network is None:raise TypeError
        is_last_data = {
            'request':'is_last'
        }
        is_last_data.update(self.stage_data)
        data = await self.a_network.make_message(relay=True,extra_data=is_last_data)
        await self.a_network.relay(json.dumps(data))
        return data.get("timestamp")

    
    async def check_if_last(self,wait_time:int,cooldown:int):
        if self.a_network is None: raise TypeError

        timestamp = await self.request_is_last()
        dc_timestamp = None
        deadline = None
        double_check = False
        await asyncio.sleep(cooldown)
        while True:

            not_last_list = await self.a_network.find_messages({"response":"not_last"})
            for _,msg in not_last_list:
                if msg.get("request_timestamp") == timestamp or msg.get("request_timestamp") == dc_timestamp:
                    await self.a_network.clean_up_from_list(not_last_list)
                    print(f"[check_if_last] found not_last response")
                    return False
                
            is_last_list = await self.a_network.find_messages({"request":"is_last"})
            for _,msg in is_last_list:
                ts = msg.get("timestamp")
                if ts and ts > timestamp:
                    #if others are asking the same question after this one
                    # then return false
                    print(f"[check_if_last] found others response")
                    return False
                
            in_agg_list = await self.a_network.find_messages({"syn":"pair"})
            for _,msg in in_agg_list:
                ts = msg.get("timestamp")
                if ts and ts > timestamp:
                    #if new request to pair is present
                    # then return false
                    print(f"[check_if_last] found pairing response")
                    return False

            not_in_list = await self.a_network.find_messages({"response":"not_in"})
            for _,msg in not_in_list:
                if msg.get("request_timestamp") == timestamp or msg.get("request_timestamp") == dc_timestamp:
                    print(f"[check_if_last] found not_in response")
                    #refresh/start deadline when found
                    deadline = time.time() + wait_time
                    break
            await self.a_network.clean_up_from_list(not_in_list)
        
            if deadline and time.time() > deadline:
                if double_check:
                    print(f"\n[check_if_last] is the last node aggregating\n")
                    return True
                else:
                    dc_timestamp = await self.request_is_last()
                    double_check = True
                    deadline = time.time() + wait_time
            
            await asyncio.sleep(cooldown)

    async def core_aggregation(self,model):
        if self.a_network is None: raise TypeError
        # starts off at one adds more with each aggregation.
        weighting = int(1)
        try:
            while True:
                print("[core_aggregation] started")
        
                #stopping pair denial as we are looking for pairs
                await self.stop_denial_task()
                await asyncio.sleep(1)

                # pairing procedure
                found_pair = await self.pairing(recast_wait=25,connect_wait=15,check_last_wait=40,cooldown=2)

                if not found_pair:
                    # if last then send model out
                    return True,model

                await asyncio.sleep(1)

                # restarting pair_denial
                self.set_denial_task(cooldown=2)

                # get peer
                peer = await self.get_peer()
                if peer is None:
                    await asyncio.sleep(1)
                    continue

                print(f"[core_aggregation] peer : node {peer[2]}")

                #then send/recieve model.
                if await self.a_network.is_leading_peer(self.a_network.node.node.long_id,peer[2]):
                    # closer so we send model
                    print(f"[core_aggregation] sending to {peer[0]}")
                    success = await self.a_network.send_model(peer[0],self.model_port,model,weighting,wait_time=60)
                    if not success:
                        await asyncio.sleep(1)
                        continue

                    # immediately open reciever
                    print(f"[core_aggregation] now recieving from {peer[0]}")
                    aggregated_model_dict,aggregated_weighting = await self.a_network.recieve_model(self.model_port,connect_timeout=60,model_timeout=360)
                    if aggregated_model_dict is None or aggregated_weighting is None:
                        await asyncio.sleep(1)
                        continue

                    # set aggregate model after successful send and recieve
                    model.load_state_dict(aggregated_model_dict)
                    weighting = aggregated_weighting

                elif not await self.a_network.is_leading_peer(self.a_network.node.node.long_id,peer[2]):
                    #if reciever then aggregate
                    print(f"[core_aggregation] recieving to {peer[0]}")
                    peer_model_dict,peer_weighting = await self.a_network.recieve_model(self.model_port,connect_timeout=60,model_timeout=360)
                    if peer_model_dict is None or peer_weighting is None:
                        # next loop if bad model
                        await asyncio.sleep(1)
                        continue
                    peer_model = copy.deepcopy(model)
                    peer_model.load_state_dict(peer_model_dict)
                    # do aggregating
                    aggregated_model = await self.FedAvg(model,weighting,peer_model,peer_weighting)
                    aggregated_weighting = weighting + peer_weighting
                    print(f"[core_aggregation] Models have been aggregated")

                    # sending to peer now
                    print(f"[core_aggregation] now sending to {peer[0]}")
                    success = await self.a_network.send_model(peer[0],self.model_port,aggregated_model,aggregated_weighting,wait_time=60)
                    if not success:
                        await asyncio.sleep(1)
                        continue

                    # set aggregate model after successful send and recieve
                    model = aggregated_model
                    weighting = aggregated_weighting

                    return False,model
        except Exception as e:
            print(f"[core_aggregation] Exception : {e}")
            return None,None

    async def aggregation(self,model):
        in_aggregation_task = asyncio.create_task(self.response_not_last(cooldown=2))
        try:
            while True:
                print("[aggregation] started")
                is_last,model = await self.core_aggregation(model)
                if model is None:
                    await asyncio.sleep(1)
                    continue
                print(f"[core_aggregation] finished aggregating")
                #aggregate
                if is_last:
                    #returns global model to share
                    return model
                else:
                    return None
        finally:
            in_aggregation_task.cancel()
            try: await in_aggregation_task
            except asyncio.CancelledError:pass
            