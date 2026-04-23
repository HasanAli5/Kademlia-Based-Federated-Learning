import asyncio
from broadcast import *
from broadcast_alt import Broadcast as aBroadcast
from network import get_host
import model
import torch
import json
import copy

MODEL_TRANSFER_PORT = 449

class Aggregate_Stage():

    def __init__(self):
        # we use this to relay back to a peer if finished or dropped from aggregration
        # along with keeping track with the aggregation loop
        self.states = ["aggregate","backup","done"]
        self.paired = False
        self.state = self.states[0]
        # we can keep track of last peer to check health of (if they have disconnected or aggregated)
        self.peer = None
        # we set denial task later
        self.pair_denial = None
        self.paired_lock = asyncio.Lock()
        self.peer_lock = asyncio.Lock()
        self.stage_data = {
            'stage':'aggregate'
        }
    
    def set_denial_task(self,a_broadcast:aBroadcast,cooldown:int):
        self.pair_denial = asyncio.create_task(self.deny_pairing_requests(a_broadcast,cooldown))

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
    
    ## pre-aggregation steps

    # first pass (long-term)

    async def aggregate_request(self,broadcast:Broadcast):
        data = {
            'request':'aggregate'
        }
        data.update(self.stage_data)
        data = broadcast.make_message(relay=True,extra_data=data)
        try:
            message = json.dumps(data)
            await broadcast.relay(message)
            print("[aggregate_request] aggregate requests sent")
        except Exception as e:
            print(f"[aggregate_request] Exception : {e}")

    async def await_aggregate_response(self,broadcast:Broadcast,,wait_time:int,cooldown:int):

        await self.aggregate_request(broadcast)
        
        async def checker(cooldown):
            try:
                while True:
                    aggregate_denials = await broadcast.find_messages("response","deny_aggregate")
                    if len(aggregate_denials) > 0:
                        await broadcast.clean_up_from_list(aggregate_denials)
                        if not denied_future.done():
                            denied_future.set_result(True)
                            return
                    await broadcast.clean_up_from_list(aggregate_denials)
                    await asyncio.sleep(cooldown)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"[await_aggregate_response] Checker Exception : {e}")

        
        checker_task = asyncio.create_task(checker(cooldown))

        try:    
            await asyncio.wait_for(denied_future,wait_time)
            print("[await_aggregate_response] recieved denial")
            return True
        except asyncio.TimeoutError:
            print("[await_aggregate_response] did not receive denial")
            return False
        finally:
            checker_task.cancel()
            try: await checker_task
            except asyncio.CancelledError: pass

    # second pass (short-term)

    async def send_leader_request(self,broadcast:Broadcast):
        data = {
            'request':'leader_query'
        }
        data.update(self.stage_data)
        data = broadcast.make_message(relay=True,extra_data=data)
        try:
            message = json.dumps(data)
            await broadcast.relay(message)
            print("[send_leader_request] leader request sent")
        except Exception as e:
            print(f"[send_leader_request] Exception : {e}")

    async def deny_leader_request(self,broadcast:Broadcast,ns_number,cooldown:int):
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

                leader_query_list = await broadcast.find_messages("request","leader_query")

                for message,msg in leader_query_list:
                    ip = msg.get('source_ip')
                    port = msg.get('source_port')
                    if ip and port:
                        # if this node has better node id (closer to ns_number)
                        print(f"[deny_leader_request] {broadcast.node.node.long_id} vs {msg.get('source_node_id')}")
                        if self.is_leading_peer(broadcast.node.node.long_id,msg.get('source_node_id'),ns_number):
                            data = broadcast.make_message(relay=False,extra_data=deny_data)
                            await broadcast.send(ip,port,json.dumps(data))
                            print(f"[deny_leader_request] leader request from {ip}:{port} is denied")
                        else:
                            data = broadcast.make_message(relay=False,extra_data=accept_data)
                            await broadcast.send(ip,port,json.dumps(data))
                            print(f"[deny_leader_request] leader request from {ip}:{port} is accepted")

                await broadcast.clean_up_from_list(leader_query_list)

                await asyncio.sleep(cooldown)

        except Exception as e:
            print(f"[deny_leader_request] Exception : {e}")

    async def await_leader_response(self,broadcast:Broadcast,wait_time:int,cooldown:int):
        leading_future = asyncio.Future()

        async def checker(wait_time:int,cooldown:int):
            try:

                deadline = None

                while True:
                    accept_leader_list = await broadcast.find_messages("response","accept_leader")
                    deny_leader_list = await broadcast.find_messages("response","deny_leader")

                    if len(deny_leader_list)>0:
                        print("[await_leader_response] found deny leader")
                        await broadcast.clean_up_from_list(accept_leader_list)
                        await broadcast.clean_up_from_list(deny_leader_list)
                        if not leading_future.done():
                            leading_future.set_result(False)
                            return
                    
                    if len(accept_leader_list)>0:
                        print("[await_leader_response] found accept leader")
                        await broadcast.clean_up_from_list(accept_leader_list)
                        deadline = time.time() + wait_time

                    if deadline and time.time() > deadline:
                        if not leading_future.done():
                            leading_future.set_result(True)
                            return
                    
                    await asyncio.sleep(cooldown)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"[await_leader_response] Checker Exception : {e}")

        checker_task = asyncio.create_task(checker(wait_time,cooldown))

        try:
            return await leading_future
        finally:
            checker_task.cancel()
            try: await checker_task
            except asyncio.CancelledError: pass 

            

    async def is_leader(self,broadcast:Broadcast,ns_number):
        
        # this send a leader request
        await self.send_leader_request(broadcast)

        # start deny if this node is closer to ns_number
        deny_task = asyncio.create_task(self.deny_leader_request(broadcast,ns_number,2))

         # we then await for other people who want to be leader and decide who shall
        is_leader = await self.await_leader_response(broadcast,30,2)

        deny_task.cancel()
        try: await deny_task
        except asyncio.CancelledError: print("[is_leader] deny_task stopped")
        return is_leader

    ## starting aggregation network
    
    async def send_join_request(self,broadcast:Broadcast,aggregation_port):
        #require port parameter
        data = {
            'request':'join_aggregation',
            'aggregation_port':f'{aggregation_port}'
        }
        data.update(self.stage_data)
        data = broadcast.make_message(relay=True,extra_data=data)
        try:
            await broadcast.relay(json.dumps(data))
            print(f"[send_join_request] join request relayed")
        except Exception as e:
            print(f"[send_join_request] Exception : {e}")
        
    async def wait_for_leader(self,broadcast:Broadcast,cooldown:int):
        # this will read the broadcast buffer and check if there is a connect request and exit
        join_request_list = []
        while True:
            join_requests = await broadcast.find_messages("request","join_aggregation")

            leader_ip = None
            leader_port = None

            for request,rqst in join_requests:
                leader_ip = rqst.get('source_ip')
                leader_port = rqst.get('aggregation_port')
                print(f"[wait_for_leader] found leader at {leader_ip}:{leader_port}")

            if leader_ip and leader_port:
                await broadcast.clean_up_from_list(join_requests)
                return leader_ip,leader_port

            await asyncio.sleep(cooldown)

    ## main aggregation steps
    # uses aggregation network (aBroadcast)

    async def send_pair_request(self,broadcast:aBroadcast):
        # broadcasts pair request to all nodes
        data = {
            'syn':'pair'
        }
        data.update(self.stage_data)
        data = broadcast.make_message(relay=True,extra_data=data)
        try:
            message = json.dumps(data)
            await broadcast.relay(message)
            print(f"[send_pair_request] relayed")
        except Exception as e:
            print(f"[send_pair_request] Exception : {e}")

    async def send_pair_response(self,broadcast:aBroadcast,temp_peer:tuple):
        # only response if interested
        ip,port,node_id = temp_peer
        data = {
            'syn-ack':'pair'
        }
        data.update(self.stage_data)
        data = broadcast.make_message(relay=False,extra_data=data)
        try:
            message = json.dumps(data)
            await broadcast.send(ip,port,message)
            print(f"[send_pair_response] sent to {ip}:{port}")
        except Exception as e:
            print(f"[send_pair_response] Exception : {e}")

    async def accept_pair_response(self,broadcast:aBroadcast,temp_peer:tuple):
        # informs the pair that the pairing has been accepted by the peer
        # both node are now paired once sent for aggregation
        ip,port,node_id = temp_peer
        data = {
            'ack':'accept_pair'
        }
        data.update(self.stage_data)
        data = broadcast.make_message(relay=False,extra_data=data)
        try:
            message = json.dumps(data)
            await broadcast.send(ip,port,message)
            print(f"[accept_pair_response] sent to {ip}:{port}")
        except Exception as e:
            print(f"[accept_pair_response] Exception : {e}")

    async def deny_pair_response(self,broadcast:aBroadcast,temp_peer:tuple):
        # this denies pair response so they can move unlock from peer
        ip,port,node_id = temp_peer
        data = {
            'ack':'deny_pair'
        }
        data.update(self.stage_data)
        data = broadcast.make_message(relay=False,extra_data=data)
        try:
            message = json.dumps(data)
            await broadcast.send(ip,port,message)
            print(f"[deny_pair_response] sent to {ip}:{port}")
        except Exception as e:
            print(f"[deny_pair_response] Exception : {e}")

    def is_leading_peer(self,node_id,peer_node_id,ns_number):
        # leading peer is closest to ns_number
        node_long = node_id
        peer_long = peer_node_id
        distance = abs(ns_number-node_long)
        peer_distance = abs(ns_number-peer_long)
        is_leading_peer = None
        # tie breaker term
        if distance == peer_distance:
            # default to greater number wins leader
            is_leading_peer = node_long > peer_long
        else:
            # leader if closer to ns_number
            is_leading_peer = peer_distance > distance
        return is_leading_peer
    
    async def reset_peer_state(self,broadcast:aBroadcast):
        # cleanup before repairing
        await self.set_paired(False)
        await broadcast.set_status(False)
        await self.set_peer(None)

    async def set_peer_state(self,broadcast:aBroadcast,peer:tuple,paired_bool:bool,relay_bool:bool):
        # cleanup before repairing
        await self.set_paired(paired_bool)
        await broadcast.set_status(relay_bool)
        await self.set_peer(peer)

    async def pairing(self,broadcast:aBroadcast,ns_number:int,recast_wait:int,connect_wait:int,cooldown:int):
        # handler both host and client sides

        # set all peer states for pairing
        await self.reset_peer_state(broadcast)

        try:
            await self.send_pair_request(broadcast)
            recast_deadline = time.time() + recast_wait
            connection_deadline = None

            while True:

                # go through denies

                deny_pairings_list = await broadcast.find_messages("ack","deny_pair")

                if len(deny_pairings_list)>0:
                    await self.reset_peer_state(broadcast)
                    connection_deadline = None
                    print("[pairing_handler] pair request denied")
                    await broadcast.clean_up_from_list(deny_pairings_list)
                    await self.send_pair_request(broadcast)
                    recast_deadline = time.time() + recast_wait
                    print("[pairing_handler] resent pair request after deny")

                # go through accepts
                    
                accept_pairings_list = await broadcast.find_messages("ack","accept_pair")

                if len(accept_pairings_list)>0:
                    _,msg = accept_pairings_list[0]
                    peer = (msg.get("source_ip"),msg.get("source_port"),msg.get("source_node_id"))
                    await self.set_peer_state(broadcast,peer,paired_bool=True,relay_bool=True)
                    print("[pairing_handler] pair request accepted")
                    await broadcast.clean_up_from_list(accept_pairings_list)
                    return
                
                # go through pair sync-ack list
                pair_syn_ack_list = await broadcast.find_messages("syn-ack","pair")

                for message,msg in pair_syn_ack_list:
                    print("[response_handler] found pair response")
                    peer = (msg.get("source_ip"),msg.get("source_port"),msg.get("source_node_id"))
                    is_leader_peer = self.is_leading_peer(broadcast.node.node.long_id,peer[2],ns_number)

                    # if not paired and is smaller distance from ns_number
                    if not await self.get_paired() and is_leader_peer:
                        print("[pairing_handler] attempting pairing")
                        await self.accept_pair_response(broadcast,peer)
                        await self.set_peer_state(broadcast,peer,paired_bool=True,relay_bool=True)
                        return
                    
                    elif await self.get_paired() or not is_leader_peer:
                        print("[pairing_handler] already paired or not leader")
                        await self.deny_pair_response(broadcast,peer)
                
                await broadcast.clean_up_from_list(pair_syn_ack_list)

                # go through sync requests

                pair_syn_list = await broadcast.find_messages("syn","pair")

                for message,msg in pair_syn_list:
                    peer = (msg.get("source_ip"),msg.get("source_port"),msg.get("source_node_id"))
                    is_leader_peer = self.is_leading_peer(broadcast.node.node.long_id,peer[2],ns_number)

                    if not await self.get_paired() and not is_leader_peer:
                        print("[pairing_handler] sending response to request")
                        await self.send_pair_response(broadcast,peer)
                        await self.set_peer_state(broadcast,peer,paired_bool=True,relay_bool=False)
                        recast_deadline = None
                        connection_deadline = time.time() + connect_wait

                await broadcast.clean_up_from_list(pair_syn_list)

                # timeout function
                await asyncio.sleep(cooldown)
                
                # recast
                if recast_deadline and time.time() > recast_deadline:
                    await self.send_pair_request(broadcast)
                    recast_deadline = time.time() + recast_wait
                    print("[pairing_handler] resent pair request")

                # disconnect
                if connection_deadline and time.time() > connection_deadline:
                    await self.reset_peer_state(broadcast)
                    connection_deadline = None
                    print("[pairing_handler] disconnected from ghosted peer")

        except Exception as e:
            print(f"[pairing_handler] Exception : {e}")

    async def deny_pairing_requests(self,broadcast:aBroadcast,cooldown:int):
        # after obtaining pairing we deny as much as possible
        try: 
            while True:

                syn_ack_list = await broadcast.find_messages("syn-ack","pair")
                ack_list = await broadcast.find_messages("ack","pair")

                for message,msg in syn_ack_list:
                    peer = (msg.get("source_ip"),msg.get("source_port"),msg.get("source_node_id"))
                    await self.deny_pair_response(broadcast,peer)
                    print(f"[deny_pairing_requests] {peer[0]}:{peer[1]} ({[peer[2]]}) was denied")
                await broadcast.clean_up_from_list(syn_ack_list)
                await broadcast.clean_up_from_list(ack_list)
                await asyncio.sleep(cooldown)
        except Exception as e:
            print(f"[pairing_handler] Exception : {e}")

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
    
    async def response_not_last(self,broadcast:aBroadcast,cooldown:int):
        print("[response_not_last] task started")
        not_last_data = {
            'response':'not_last'
        }
        not_last_data.update(self.stage_data)
        while True:
            is_last_list = await broadcast.find_messages("request","is_last")
            for message,msg in is_last_list:
                print("[response_not_last] found is_last request")
                peer = (msg.get("source_ip"),msg.get("source_port"),msg.get("source_node_id"))
                data = broadcast.make_message(relay=False,extra_data=not_last_data)
                await broadcast.send(peer[0],peer[1],json.dumps(data))
            await broadcast.clean_up_from_list(is_last_list)
            await asyncio.sleep(cooldown)

    async def response_not_in(self,broadcast:aBroadcast,cooldown:int):
        not_in_data = {
            'response':'not_in'
        }
        while True:
            is_last_list = await broadcast.find_messages("request","is_last")
            for message,msg in is_last_list:
                print("[response_not_last] found is_last request")
                peer = (msg.get("source_ip"),msg.get("source_port"),msg.get("source_node_id"))
                data = broadcast.make_message(relay=False,extra_data=not_in_data)
                await broadcast.send(peer[0],peer[1],json.dumps(data))
            await broadcast.clean_up_from_list(is_last_list)
            await asyncio.sleep(cooldown)

    
    async def check_if_last(self,broadcast:aBroadcast,wait_time:int,max_tries:int,cooldown:int):
        # cleanup from last check
        not_last_list = await broadcast.find_messages("response","not_last")
        await broadcast.clean_up_from_list(not_last_list)
        is_last_data = {
            'request':'is_last'
        }
        is_last_data.update(self.stage_data)
        data = broadcast.make_message(relay=True,extra_data=is_last_data)
        await broadcast.relay(json.dumps(data))
        deadline = time.time() + wait_time
        tries = 0
        await asyncio.sleep(cooldown)
        while tries < max_tries:

            not_last_list = await broadcast.find_messages("response","not_last")
            if len(not_last_list) > 0:
                await broadcast.clean_up_from_list(not_last_list)
                print(f"[check_if_last] found not_last response")
                return False

            not_in_list = await broadcast.find_messages("response","not_in")
            if len(not_last_list) > 0:
                await broadcast.clean_up_from_list(not_in_list)
                print(f"[check_if_last] found not_in response")
                #refresh when found
                deadline = time.time() + wait_time

            is_last_list = await broadcast.find_messages("request","is_last")
            if len(is_last_list) > 0:
                #if others are asking the same question 
                # then return false
                print(f"[check_if_last] found others response")
                return False
            
            if time.time() > deadline:
                tries += 1
                deadline = time.time() + wait_time
            
            await asyncio.sleep(cooldown)

        print(f"[check_if_last] IS THE LAST NODE!!!!")
        return True

    async def core_aggregation(self,broadcast:aBroadcast,model,ns_number):
        dropped = False
        # starts off at one adds more with each aggregation.
        weighting = int(1)
        try:
            while not dropped:

                # is last
                is_last = await self.check_if_last(broadcast,wait_time=30,max_tries=3,cooldown=2)
                if is_last:
                    # if last then send model out
                    return True,model
                
                #stopping pair denial as we are looking for pairs
                await self.stop_denial_task()

                # pairing procedure
                await self.pairing(broadcast,ns_number,recast_wait=30,connect_wait=30,cooldown=2)

                # restarting pair_denial
                self.set_denial_task(broadcast,cooldown=2)

                # get peer
                peer = await self.get_peer()
                if peer is None:
                    await asyncio.sleep(1)
                    continue

                print(f"[core_aggregation] peer : node {peer[2]}")

                #then send/recieve model.
                if self.is_leading_peer(broadcast.node.node.long_id,peer[2],ns_number):
                    # closer so we send model
                    print(f"[core_aggregation] sending to {peer[0]}")
                    success = await network.send_model(peer[0],MODEL_TRANSFER_PORT,model,weighting,60)
                    if not success:
                        await asyncio.sleep(1)
                        continue

                    # immediately open reciever
                    print(f"[core_aggregation] now recieving from {peer[0]}")
                    aggregated_model,aggregated_weighting = await network.recieve_model(MODEL_TRANSFER_PORT,60,9999)
                    if aggregated_model is None or aggregated_weighting is None:
                        await asyncio.sleep(1)
                        continue

                    # set aggregate model after successful send and recieve
                    model = aggregated_model
                    weighting = aggregated_weighting

                elif not self.is_leading_peer(broadcast.node.node.long_id,peer[2],ns_number):
                    #if reciever then aggregate
                    print(f"[core_aggregation] recieving to {peer[0]}")
                    peer_model,peer_weighting = await network.recieve_model(MODEL_TRANSFER_PORT,60,9999)
                    if peer_model is None or peer_weighting is None:
                        # next loop if bad model
                        await asyncio.sleep(1)
                        continue

                    # do aggregating
                    aggregated_model = await self.FedAvg(model,weighting,peer_model,peer_weighting)
                    aggregated_weighting = weighting + peer_weighting
                    print(f"[core_aggregation] Models have been aggregated")

                    # sending to peer now
                    print(f"[core_aggregation] now sending to {peer[0]}")
                    success = await network.send_model(peer[0],MODEL_TRANSFER_PORT,aggregated_model,aggregated_weighting,60)
                    if not success:
                        await asyncio.sleep(1)
                        continue

                    # set aggregate model after successful send and recieve
                    model = aggregated_model
                    weighting = aggregated_weighting

                    dropped = True
                    return False,model
            return None,None
        except Exception as e:
            print(f"[core_aggregation] Exception : {e}")
            return None,None
        

    async def check_peer(self):
        pass

    async def aggregation(self,broadcast:aBroadcast,model,ns_number):
        timeout = 5
        while True:
            if self.state == self.states[0]:
                in_aggregation_task = asyncio.create_task(self.response_not_last(broadcast,2))
                try:
                    is_last,model = await self.core_aggregation(broadcast,model,ns_number)
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
            """elif self.state == self.states[1]:
                #backup
                response = await self.check_peer()
                if response == None:
                    # if no response join back and do aggregation instead of peer
                    self.state = self.states[0]
                elif response["response"] == self.states[0]:
                    # if they are currently aggregating then keep this state
                    pass
                elif response["response"] == self.states[1]:
                    # if that node is backing up another node become done
                    self.state = self.states[2]
            elif self.state == self.states[2]:
                # done quits the aggregation cycles
                # return none so it can wait for global model on otherside
                return None"""
            await asyncio.sleep(timeout)