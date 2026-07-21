import time
from kademlia.network import Server
from network import Network
import asyncio
import json


class Share():

    def __init__(self,network:Network,model_share_port:int):
        self.model_status = False
        self.model_lock = asyncio.Lock()
        self.stage_data = {
            'stage':'share'
        }
        self.network = network
        self.msp = model_share_port

    async def set_status(self,value):
        async with self.model_lock:
            self.model_status = value

    async def get_status(self):
        async with self.model_lock:
            return self.model_status

    async def send_share_model_request(self):
        print("[send_share_model_request] started")
        ns_number,ns_number_ts = await self.network.get_ns_number()
        share_data = {
            'request':'share_model',
            'ns_number':ns_number,
            'ns_number_ts':ns_number_ts,
        }
        share_data.update(self.stage_data)
        # send model share to all neighbours
        try:
            data = await self.network.make_message(relay=True,extra_data=share_data)
            await self.network.relay(json.dumps(data))
            return data.get("timestamp")
        except Exception as e:
            print(f"[send_share_model_request] Exception : {e}")

    async def shield_wrap(self,coro):
        return await asyncio.shield(coro)

    async def share_model_reponse(self):
        pending_response = set()
        print("[share_model_reponse] started")
        try:
            while True:
                share_model_requests = await self.network.find_messages({"request":"share_model"})
                for message,msg in share_model_requests:
                    print("[share_model_reponse] found a share_model request")
                    ip = msg.get("source_ip")
                    port = msg.get("source_port")
                    ts = msg.get("timestamp")
                    if ip and port and ts:
                        data = {
                            'response':'model_check',
                            'status':await self.get_status()
                        }
                        data.update(self.stage_data)
                        data = await self.network.make_message(relay=False,extra_data=data)
                        data = self.network.attach_request_info(data,ts)
                        task = asyncio.create_task(self.shield_wrap(self.network.send(ip,port,json.dumps(data))))
                        pending_response.add(task)
                        task.add_done_callback(pending_response.discard)
                        if not await self.get_status():
                            # only start extension by getting send
                            model_dict = None
                            try:
                                model_dict = await self.get_global_model()
                            except Exception as ex:
                                print(ex)
                            if model_dict:
                                await self.set_status(True)
                                print("[share_model_reponse] recieved Global Model")
                                await self.network.clean_up_from_list(share_model_requests)
                                return model_dict
                            
                await self.network.clean_up_from_list(share_model_requests)
                await asyncio.sleep(3)
        except:
            if pending_response:
                await asyncio.gather(*pending_response,return_exceptions=True)


    async def check_accept_models(self,model,wait_time,ready_wait_time,cooldown):
        model_check_list = await self.network.find_messages({"response":"model_check"})
        await self.network.clean_up_from_list(model_check_list)
        print("[check_accept_models] started")
        timestamp = await self.send_share_model_request()
        resend_deadline = time.time() + wait_time
        await asyncio.sleep(cooldown)
        while True:
            if time.time() > resend_deadline:
                await self.send_ready_request()
                await asyncio.sleep(cooldown)
                done = await self.get_ready_response(cooldown,wait_time)
                if done:
                    # if there is no not_ready response stay
                    return
                else:
                    # if not then leave
                    timestamp = await self.send_share_model_request()
                    resend_deadline = time.time() + wait_time

            model_check_list = await self.network.find_messages({"response":"model_check"})

            for _,msg in model_check_list:
                peer = (msg.get("source_ip"),msg.get("source_port"),int(msg.get("source_node_id")))
                ts = msg.get("request_timestamp")
                if peer[0] and peer[1] and ts == timestamp:
                    status = msg.get("status")
                    if status == False:
                        success = await self.send_global_model(peer[0],model)
                        if not success:
                            # resend request
                            print("[share_model_reponse] failed to send Global Model")
                            timestamp = await self.send_share_model_request()
                            resend_deadline = time.time() + wait_time
                        else:
                            print("[share_model_reponse] sent Global Model")
                    else:
                        print("[share_model_reponse] did not send Global Model")
            await self.network.clean_up_from_list(model_check_list)
            
            await asyncio.sleep(cooldown)

    async def get_global_model(self):
        model_dict,_ = await self.network.recieve_model(self.msp,connect_timeout=60,model_timeout=360)
        return model_dict
    
    async def send_global_model(self,ip,model):
        return await self.network.send_model(ip,self.msp,model,weighting=0,wait_time=60)
    
    async def send_ready_request(self):
        print("[send_ready_request] started")
        ready_data = {
            'request':'ready',
        }
        ready_data.update(self.stage_data)
        # send model share to all neighbours
        try:
            data = await self.network.make_message(relay=True,extra_data=ready_data)
            await self.network.relay(json.dumps(data))
            return data.get("timestamp")
        except Exception as e:
            print(f"[send_share_model_request] Exception : {e}")

    async def send_ready_response(self,cooldown:int):
        print("[send_not_ready_response] started")
        not_ready_data = {
            'response':'not_ready',
        }
        not_ready_data.update(self.stage_data)
        is_ready_data = {
            'response':'is_ready',
        }
        is_ready_data.update(self.stage_data)
        # send model share to all neighbours
        try:
            while True:
                status = await self.get_status()
                ready_requests = await self.network.find_messages({"request":"ready"})
                for _,msg in ready_requests:
                    peer = (msg.get("source_ip"),msg.get("source_port"))
                    ts = msg.get("timestamp")
                    if peer[0] and peer[1] and ts:
                        if status:
                            data = await self.network.make_message(relay=False,extra_data=is_ready_data)
                        else:
                            data = await self.network.make_message(relay=False,extra_data=not_ready_data)
                        data = self.network.attach_request_info(data,ts)
                        await self.network.send(peer[0],peer[1],json.dumps(data))
                await self.network.clean_up_from_list(ready_requests)
                await asyncio.sleep(cooldown)
        except Exception as e:
            print(f"[send_share_model_request] Exception : {e}")


    async def get_ready_response(self,cooldown:int,wait_time:int):
        print("[get_ready_response] started")
        # clean before scanning new
        prev_not_ready_list = await self.network.find_messages({"response":"not_ready"})
        await self.network.clean_up_from_list(prev_not_ready_list)
        prev_is_ready_list = await self.network.find_messages({"response":"is_ready"})
        await self.network.clean_up_from_list(prev_is_ready_list)

        timestamp = await self.send_ready_request()
        await asyncio.sleep(cooldown)

        deadline = None
        while deadline == None or time.time() < deadline :
            not_ready_list = await self.network.find_messages({"response":"not_ready"})
            for _,msg in not_ready_list:
                ts = msg.get("request_timestamp")
                if timestamp == ts:
                    await self.network.clean_up_from_list(not_ready_list)
                    return False
            await self.network.clean_up_from_list(not_ready_list)

            ready_list = await self.network.find_messages({"response":"is_ready"})
            for _,msg in ready_list:
                ts = msg.get("request_timestamp")
                if timestamp == ts:
                    #sets or resets the deadline
                    deadline = time.time() + wait_time
                    break
            await self.network.clean_up_from_list(ready_list)

            await asyncio.sleep(cooldown)
        return True

    async def send_leader_request(self):
        ns_number,ns_number_ts = await self.network.get_ns_number()
        data = {
            'request':'go_leader',
            'ns_number':ns_number,
            'ns_number_ts':ns_number_ts
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

    async def send_leader_response(self,cooldown:int):
        pending_response = set()
        deny_data = {
            'response':'deny_go_leader',
            }
        deny_data.update(self.stage_data)
        accept_data = {
            'response':'accept_go_leader',
            }
        accept_data.update(self.stage_data)
        try:
            while True:
                leader_query_list = await self.network.find_messages({"request":"go_leader"})
                ns_number,_ = await self.network.get_ns_number()

                for message,msg in leader_query_list:
                    ip = msg.get('source_ip')
                    port = msg.get('source_port')
                    if ip and port:
                        # if this node has better node id (closer to ns_number)
                        print(f"[deny_leader_request] {self.network.node.node.long_id} vs {msg.get('source_node_id')}\nns_number : {ns_number}:")
                        if await self.network.is_leading_peer(self.network.node.node.long_id,int(msg.get('source_node_id'))):
                            data = await self.network.make_message(relay=False,extra_data=deny_data)
                            task = asyncio.create_task(self.network.send(ip,port,json.dumps(data)))
                            pending_response.add(task)
                            task.add_done_callback(pending_response.discard)
                            print(f"[deny_leader_request] leader request from {ip}:{port} is denied")
                        else:
                            data = await self.network.make_message(relay=False,extra_data=accept_data)
                            task = asyncio.create_task(self.network.send(ip,port,json.dumps(data)))
                            pending_response.add(task)
                            task.add_done_callback(pending_response.discard)
                            print(f"[deny_leader_request] leader request from {ip}:{port} is accepted")

                await self.network.clean_up_from_list(leader_query_list)

                await asyncio.sleep(cooldown)

        except Exception as e:
            print(f"[deny_leader_request] Exception : {e}")
        finally:
            await asyncio.gather(*pending_response,return_exceptions=True)


    async def await_leader_response(self,response_wait:int,exit_wait:int,cooldown:int):
        try:

            DENIED_BY_SHARE = 1
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
                accept_leader_list = await self.network.find_messages({"response":"accept_go_leader"})
                deny_leader_list = await self.network.find_messages({"response":"deny_go_leader"})
                not_ready_list = await self.network.find_messages({"response":"not_ready"})
                go_request_list = await self.network.find_messages({"request":"go"})

                if len(go_request_list)>0:
                    print("[await_leader_response] found go request get out of here!")
                    await self.network.clean_up_from_list(accept_leader_list)
                    await self.network.clean_up_from_list(deny_leader_list)
                    await self.network.clean_up_from_list(not_ready_list)
                    await self.network.clean_up_from_list(go_request_list)
                    message,msg = go_request_list[0]
                    return msg
                
                elif len(not_ready_list)>0:
                    for _,msg in not_ready_list:
                        ts = msg.get("request_timestamp")
                        if timestamp == ts or dc_timestamp == ts:
                        # there are people still training just wait
                            print("[await_leader_response] found deny go ahead")
                            # stop the exit
                            exit_deadline = None
                            double_check = False
                            recast_deadline = time.time() + response_wait
                            state = DENIED_BY_SHARE
                            break
                    await self.network.clean_up_from_list(accept_leader_list)
                    await self.network.clean_up_from_list(deny_leader_list)
                    await self.network.clean_up_from_list(not_ready_list)

                elif len(deny_leader_list)>0 and state > DENIED_BY_SHARE:
                    for _,msg in deny_leader_list :
                        ts = msg.get("request_timestamp")
                        if timestamp == ts or dc_timestamp == ts:
                            print("[await_leader_response] found deny go leader")
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
                        if timestamp == ts or dc_timestamp == ts:
                            print("[await_leader_response] found accept go leader")
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
    
    async def send_go_request(self):
        print("[send_go_request] started")
        ready_data = {
            'request':'go',
        }
        ready_data.update(self.stage_data)
        # send model share to all neighbours
        try:
            data = await self.network.make_message(relay=True,extra_data=ready_data)
            await self.network.relay(json.dumps(data))
            return data.get("timestamp")
        except Exception as e:
            print(f"[send_share_model_request] Exception : {e}")


    async def sync(self):

        # start deny if this node is closer to ns_number
        responder = asyncio.create_task(self.send_leader_response(cooldown=2))

         # we then await for other people who want to be leader and decide who shall
        response = await self.await_leader_response(response_wait=15,exit_wait=30,cooldown=2)

        try:
            if response is True:
                # is the leader send request and go
                await self.send_go_request()
                print(f"\n[sync] Started Next Cycle\n")
                return

            elif type(response) is dict:
                # found go request so we can go
                return
        except Exception as e:
            print(f"[sync] Exception {e}")
        finally:
            responder.cancel()
            try:await responder
            except asyncio.CancelledError:pass

        responder.cancel()
        try: await responder
        except asyncio.CancelledError: print("[responder] responder stopped")
        return