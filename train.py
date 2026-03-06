# we will split the client data up by 10 segments and let the individual train on selected amount of data.

from functions import train_val_loop
from model import ResNet18
from train_settings import Train_Settings
from torch.utils.data import DataLoader
import torch
import asyncio
from kademlia.network import Server
from broadcast import *
from network import get_host
import json

class Training_Stage():

    def __init__(self):
        self.trained = False
        self.trained_lock = asyncio.Lock()

    async def training(self,model,dataloaders):

        loop = asyncio.get_event_loop()

        train_dl,val_dl = dataloaders

        device = torch.accelerator.current_accelerator().type if torch.accelerator.is_available() else "cpu"

        #config = Train_Settings(model=model,learning_rate=0.1,decay_rate=0.0)

        # model is not used for training just parameters
        loss = torch.nn.BCEWithLogitsLoss()
        learning_rate = 1e-5
        optimiser = torch.optim.Adam(params=model.parameters(),lr=learning_rate)

        config = (loss,device,optimiser),(loss,device)
        logs = [[[],[]],[[],[]]]

        epoch = 1

        training_task = loop.run_in_executor(None,train_val_loop,model,config,(train_dl,val_dl),epoch,logs)

        await training_task
        
        await self.set_trained(True)

    async def get_trained(self):
        async with self.trained_lock:
            return self.trained
    
    async def set_trained(self,value:bool):
        async with self.trained_lock:
            self.trained = value

    async def deny_aggregate_request(self,broadcast:Broadcast):
        try:
            while True:
                print("[deny_aggregaterequest] checking for aggregate requests")
                messages = await broadcast.get_messages()
                # handle all share requests
                for message in messages:
                    msg = json.loads(message)
                    if 'request' in msg.keys() and msg['request'] == 'aggregate':
                        print("[deny_aggregate_request] found aggregate request")
                        ip = msg["source_ip"]
                        data = {
                            'source_ip':f'{get_host()}',
                            'destination_ip':f"{ip}",
                            'response':'deny_aggregate',
                            'relay':False
                        }
                        await broadcast.send(ip,json.dumps(data))
                        await broadcast.ignore_message(message)
                        await broadcast.delete_message(message)
                        print(f"[deny_aggregate_request] denied aggregate request : {ip}")
                await asyncio.sleep(2)
        except:
            print("[deny_aggregate_request] stopped")