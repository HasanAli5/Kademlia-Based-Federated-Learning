import asyncio
import json
import time

from torch import nn
from typing import Any

from network import Network
from data_manage import Data_Manager
from model_manage import Model_Manager
from model_functions import train_val_loop


class Train():

    def __init__(self,network:Network):

        # this is the main network class not the aggregation one
        self.network = network

        self.STATE_INACTIVE = 0
        self.STATE_PENDING = 1
        self.STATE_ACTIVE = 2
        self.STATE_WAITING = 3

        # state parameter
        self.state = self.STATE_INACTIVE
        self.state_lock = asyncio.Lock()

        # holds the active responder task that will be active throughout to answer training requests
        self.train_responder_task = None

        # static request/response data
        self.stage_data = {
            'stage':'train'
        }
        
        self.start_train_request = {
            'request':'start'
        }
        self.start_train_request.update(self.stage_data)

        self.start_train_response = {
            'response':'start'
        }
        self.start_train_response.update(self.stage_data)

        self.end_train_request = {
            'request':'end'
        }
        self.end_train_request.update(self.stage_data)

        self.end_train_response = {
            'response':'end'
        }
        self.end_train_response.update(self.stage_data)

        self.next_stage_request = {
            'request':'next_stage'
        }
        self.next_stage_request.update(self.stage_data)

    # setters/getters

    async def get_state(self):
        async with self.state_lock:
            return self.state
    
    async def set_state(self,value:int):
        async with self.state_lock:
            self.state = value

    async def start_responder_task(self):
        pass

    async def end_responder_task(self):
        pass

    # send functions

    async def send_train_request(self):
        data = await self.network.make_message(relay=True,extra_data=self.start_train_request)
        await self.network.relay(json.dumps(data))
        return data.get("timestamp")

    async def send_train_response(self,ip:str,port:int,request_timestamp:float):
        data:dict[str,Any] = {
            'status': await self.get_state()
        }
        data = self.network.attach_request_info(data,request_timestamp)
        data.update(self.start_train_response)
        full_msg = await self.network.make_message(relay=False,extra_data=data)
        await self.network.send(ip,port,json.dumps(full_msg))
        return full_msg.get("timestamp")

    # main responder function

    async def shield_wrap(self,coro):
        return await asyncio.shield(coro)
    
    async def responder(self,cooldown:int):
        pending_responses = set()
        try:
            while True:
                training_requests = await self.network.find_messages(self.start_train_request)
                for message,msg in training_requests:
                    peer = (msg.get("source_ip"),msg.get("source_port"),msg.get("timestamp"))
                    if peer[0] and peer[1] and peer[2]:
                        pending_task = asyncio.create_task(self.shield_wrap(self.send_train_response(peer[0],peer[1],peer[2])))
                        pending_responses.add(pending_task)
                        pending_task.add_done_callback(pending_responses.discard)
                await self.network.clean_up_from_list(training_requests)
                await asyncio.sleep(cooldown)
        finally:
            if pending_responses:
                await asyncio.gather(*pending_responses,return_exceptions=True)

    # main sync function

    async def request_to_train(self,response_wait:int,exit_wait:int,cooldown:int):
        timestamp = await self.send_train_request()
        await asyncio.sleep(cooldown)
        recast_deadline = time.time() + response_wait
        exit_deadline = None
        while not exit_deadline or time.time() < exit_deadline:
            # get all train responses
            training_responses = await self.network.find_messages(self.start_train_response)
            # iterated through
            for _,msg in training_responses:
                # for this request
                if timestamp == msg.get("request_timestamp"):
                    status = msg.get("status")
                    # if the 
                    if status == self.STATE_INACTIVE or status == self.STATE_WAITING:
                        # if the nodes are either not in or leaving the stage go to share
                        await self.network.clean_up_from_list(training_responses)
                        return False
                    else:
                        exit_deadline = time.time() + exit_wait
                        recast_deadline = None
            await self.network.clean_up_from_list(training_responses)
            
            # resend if there is no response in a timeframe
            if recast_deadline and time.time() > recast_deadline:
                await self.send_train_request()
                recast_deadline = time.time() + response_wait

            await asyncio.sleep(cooldown)
        return True

    # main action function

    async def training(self,model:nn.Module,data_toolbox:Data_Manager,model_toolbox:Model_Manager,epochs:int):

        model.to(model_toolbox.device)

        train_dl,val_dl = data_toolbox.get_dataloaders()

        config = model_toolbox.get_config()
        
        training_task = asyncio.to_thread(train_val_loop,model,config,(train_dl,val_dl),epochs,model_toolbox.logs,model_toolbox)

        try:
            model_toolbox.logs = await training_task
            model.to('cpu')
            return
        except Exception as e:
            print(f"[Training] Exception : {e}")
        

    async def deny_aggregate_request(self,cooldown:int):
        response_data = {
            'response':'deny_aggregate',
            }
        response_data.update(self.stage_data)
        try:
            while True:
                deny_list = await self.network.find_messages({"request":"leader"})
                for message,msg in deny_list:
                    print("[deny_aggregate_request] found leader request")
                    ip = msg.get("source_ip")
                    port = msg.get("source_port")
                    ts = msg.get("timestamp")
                    if ip and port and ts:
                        data = await self.network.make_message(relay=False,extra_data=response_data)
                        data = self.network.attach_request_info(data,ts)
                        await self.network.send(ip,port,json.dumps(data))
                        print(f"\n [deny_aggregate_request] denied aggregate request : {ip}\n")
                await self.network.clean_up_from_list(deny_list)
                await asyncio.sleep(cooldown)
        except Exception as e:
            print(f"[deny_aggregate_request] Exception : {e}")

    