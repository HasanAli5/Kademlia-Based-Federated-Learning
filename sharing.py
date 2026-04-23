
from pickle import NONE
import time

from sympy import false

import network
import broadcast
import asyncio
import json

MODEL_SHARE_PORT = 449

class Sharing_Stage():

    def __init__(self):
        self.model_status = False
        self.model_lock = asyncio.Lock()
        self.stage_data = {
            'stage':'share'
        }
        pass

    async def set_status(self,value):
        async with self.model_lock:
            self.model_status = value

    async def get_status(self):
        async with self.model_lock:
            return self.model_status

    async def send_share_model_request(self,broadcast:broadcast.Broadcast,ns_number:int):
        print("[send_share_model_request] started")
        share_data = {
            'request':'share_model',
            'ns_number':ns_number
        }
        share_data.update(self.stage_data)
        # send model share to all neighbours
        try:
            data = broadcast.make_message(relay=False,extra_data=share_data)
            await broadcast.relay(json.dumps(data))
        except Exception as e:
            print(f"[send_share_model_request] Exception : {e}")

    async def share_model_reponse(self,broadcast:broadcast.Broadcast):
        print("[share_model_reponse] started")
        while True:
            share_model_requests = await broadcast.find_messages("request","share_model")
            for message,msg in share_model_requests:
                print("[share_model_reponse] found a share_model request")
                ip = msg.get("source_ip")
                port = msg.get("source_port")
                if ip and port:
                    data = {
                        'response':'model_check',
                        'status':await self.get_status()
                    }
                    data.update(self.stage_data)
                    data = broadcast.make_message(relay=False,extra_data=data)
                    await broadcast.send(ip,port,json.dumps(data))
                    if not await self.get_status():
                        # only start extension by getting send
                        model = await self.get_global_model()
                        if model:
                            await self.set_status(True)
                            print("[share_model_reponse] recieved Global Model")
                            await broadcast.clean_up_from_list(share_model_requests)
                            return model,msg.get("ns_number")
            await broadcast.clean_up_from_list(share_model_requests)
            await asyncio.sleep(3)

    async def check_accept_models(self,broadcast:broadcast.Broadcast,model,ns_number,k):
        print("[check_accept_models] started")
        cycles = 0
        done_peers_set = set()
        # expect k amount of responses
        while len(done_peers_set)<k:
            model_check_list = await broadcast.find_messages("response","model_check")
            for message,msg in model_check_list:
                peer = (msg.get("source_ip"),msg.get("source_port"),msg.get("source_node_id"))
                if peer[2] in done_peers_set:
                    continue
                if peer[0] and peer[1]:
                    status = msg.get("status")
                    if status == False:
                        success = await self.send_global_model(peer[0],449,model)
                        if not success:
                            # resend request
                            await self.send_share_model_request(broadcast,ns_number)
                        print("[share_model_reponse] sent Global Model")
                    else:
                        print("[share_model_reponse] did not send Global Model")
                        done_peers_set.add(peer[2])
            await broadcast.clean_up_from_list(model_check_list)
            cycles = cycles + 1
            await asyncio.sleep(3)

    async def get_global_model(self):
        model,_ = await network.recieve_model(MODEL_SHARE_PORT,60,360)
        return model
    
    async def send_global_model(self,ip,port,model):
        return await network.send_model(ip,port,model,0,MODEL_SHARE_PORT)
    
    async def send_ready_request(self,broadcast:broadcast.Broadcast):
        print("[send_share_model_request] started")
        ready_data = {
            'request':'ready',
        }
        ready_data.update(self.stage_data)
        # send model share to all neighbours
        try:
            data = broadcast.make_message(relay=True,extra_data=ready_data)
            await broadcast.relay(json.dumps(data))
        except Exception as e:
            print(f"[send_share_model_request] Exception : {e}")

    async def send_not_ready_response(self,broadcast:broadcast.Broadcast,cooldown:int):
        print("[send_share_model_request] started")
        ready_data = {
            'response':'not_ready',
        }
        ready_data.update(self.stage_data)
        # send model share to all neighbours
        try:
            while True:
                ready_requests = await broadcast.find_messages("request","ready")
                for messasge,msg in ready_requests:
                    peer = (msg.get("source_ip"),msg.get("source_port"))
                    data = broadcast.make_message(relay=False,extra_data=ready_data)
                    await broadcast.send(peer[0],peer[1],json.dumps(data))
                await broadcast.clean_up_from_list(ready_requests)
                await asyncio.sleep(cooldown)
        except Exception as e:
            print(f"[send_share_model_request] Exception : {e}")


    async def get_ready_response(self,broadcast:broadcast.Broadcast,cooldown:int,wait_time:int):
        print("[share_model_reponse] started")
        # clean before scanning new
        not_ready_list = await broadcast.find_messages("response","not_ready")
        await broadcast.clean_up_from_list(not_ready_list)

        await self.send_ready_request(broadcast)

        deadline = time.time() + wait_time
        while time.time() < deadline:
            not_ready_list = await broadcast.find_messages("response","not_ready")
            if len(not_ready_list)>0:
                await broadcast.clean_up_from_list(not_ready_list)
                return False
            await broadcast.clean_up_from_list(not_ready_list)

            ready_list = await broadcast.find_messages("request","ready")
            if len(ready_list)>0:
                deadline = time.time() + wait_time
            await broadcast.clean_up_from_list(ready_list)

            await asyncio.sleep(cooldown)
        return True
    
    async def send_go_request(self,broadcast:broadcast.Broadcast,cooldown:int):
        print("[send_share_model_request] started")
        ready_data = {
            'request':'go',
        }
        ready_data.update(self.stage_data)
        # send model share to all neighbours
        try:
            data = broadcast.make_message(relay=True,extra_data=ready_data)
            await broadcast.relay(json.dumps(data))
        except Exception as e:
            print(f"[send_share_model_request] Exception : {e}")


    async def wait_for_last(self,broadcast:broadcast.Broadcast,cooldown:int):
        print("[share_model_reponse] started")
        while True:
            share_model_requests = await broadcast.find_messages("request","go")
            if len(share_model_requests)>0:
                await broadcast.clean_up_from_list(share_model_requests)
                return
            await broadcast.clean_up_from_list(share_model_requests)
            await asyncio.sleep(cooldown)
