# we will split the client data up by 10 segments and let the individual train on selected amount of data.

import broadcast
from data_manage import Data_Manager
from model_functions import train_val_loop
import torch
import asyncio
from torch import nn
from broadcast import *
import json

from model_manage import Model_Manager

class Training_Stage():

    def __init__(self):
        self.trained = False
        self.trained_lock = asyncio.Lock()
        self.stage_data = {
            'stage':'train'
        }

    async def start_training_request(self,broadcast:Broadcast):
        request_data = {
            'request':'start_training'
        }
        request_data.update(self.stage_data)
        data = broadcast.make_message(relay=True,extra_data=request_data)
        await broadcast.relay(json.dumps(data))

    async def training_responding(self,broadcast:Broadcast,cooldown:int):
        response_data = {
            'response':'start_training'
        }
        response_data.update(self.stage_data)
        while True:
            training_requests = await broadcast.find_messages("request","start_training")
            for message,msg in training_requests:
                peer = (msg.get("source_ip"),msg.get("source_port"))
                if peer[0] and peer[1]:
                    data = broadcast.make_message(relay=True,extra_data=response_data)
                    await broadcast.send(peer[0],peer[1],json.dumps(data),relay=False)
            await broadcast.clean_up_from_list(training_requests)
            await asyncio.sleep(cooldown)

    async def training_requesting(self,broadcast:Broadcast,resend_wait:int,cooldown:int):
        deadline = time.time() + resend_wait
        await self.start_training_request(broadcast)
        await asyncio.sleep(cooldown)
        while True:
            training_requests = await broadcast.find_messages("response","start_training")
            if len(training_requests)>0:
                await broadcast.clean_up_from_list(training_requests)
                return
            await broadcast.clean_up_from_list(training_requests)
            await asyncio.sleep(cooldown)
            if time.time() > deadline:
                await self.start_training_request(broadcast)
                deadline = time.time() + resend_wait


    async def training(self,model:nn.Module,data_toolbox:Data_Manager,model_toolbox:Model_Manager,epochs:int):

        train_dl,val_dl = data_toolbox.get_dataloaders(32)

        config = model_toolbox.get_config()
        
        training_task = asyncio.to_thread(train_val_loop,model,config,(train_dl,val_dl),epochs,model_toolbox.logs)

        try:
            await training_task
            await self.set_trained(True)
        except Exception as e:
            print(f"[Training] Exception : {e}")

    async def get_trained(self):
        async with self.trained_lock:
            return self.trained
    
    async def set_trained(self,value:bool):
        async with self.trained_lock:
            self.trained = value

    async def deny_aggregate_request(self,broadcast:Broadcast,cooldown:int):
        response_data = {
            'response':'deny_aggregate',
            }
        response_data.update(self.stage_data)
        try:
            while True:
                deny_list = await broadcast.find_messages("request","aggregate")
                for message,msg in deny_list:
                    print("[deny_aggregate_request] found aggregate request")
                    ip = msg.get("source_ip")
                    port = msg.get("source_port")
                    if ip and port:
                        data = broadcast.make_message(relay=False,extra_data=response_data)
                        await broadcast.send(ip,port,json.dumps(data))
                        print(f"[deny_aggregate_request] denied aggregate request : {ip}")
                await broadcast.clean_up_from_list(deny_list)
                await asyncio.sleep(cooldown)
        except Exception as e:
            print(f"[deny_aggregate_request] Exception : {e}")