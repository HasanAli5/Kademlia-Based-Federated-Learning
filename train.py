# we will split the client data up by 10 segments and let the individual train on selected amount of data.

from functions import train_val_loop
from model import ResNet18
from train_settings import Train_Settings
from torch.utils.data import DataLoader
import torch
import asyncio
import threading
from kademlia.network import Server
from kademlia.routing import RoutingTable
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

    async def deny_share_request(self,broadcast:Broadcast):
        try:
            while True:
                print("[deny_share_request] checking for share requests")
                messages = await broadcast.get_messages()
                # handle all share requests
                for message in messages:
                    msg = json.loads(message)
                    if 'request' in msg.keys() and msg['request'] == 'share':
                        print("[deny_share_request] found share request")
                        ip = msg["source_ip"]
                        data = {
                            'source_ip':f'{get_host()}',
                            'destination_ip':f"{ip}",
                            'response':'deny',
                            'relay':False
                        }
                        await broadcast.send(ip,json.dumps(data))
                        await broadcast.ignore_message(message)
                        await broadcast.delete_message(message)
                        print(f"[deny_share_request] denied share request : {ip}")
                await asyncio.sleep(2)
        except:
            print("[deny_share_request] stopped")

    async def share_request(self,node:Server,broadcast:Broadcast):
        nodes = node.protocol.router.find_neighbors(node.node)
        for n in nodes:
            data = {
                'source_ip':f'{get_host()}',
                'request':'share',
                'relay':True
            }
            try:
                await broadcast.send(n.ip,json.dumps(data))
            except:
                print(f"[share_request] share requests failed to send to {n.ip}")
        print("[share_request] share requests sent")

    async def await_share_response(self,broadcast:Broadcast):
        ticker = 30
        while ticker > 0:
            print("[await_share_response] waiting for share response")
            messages = await broadcast.get_messages()
            for message in messages:
                msg = json.loads(message)
                if 'response' in msg.keys() and msg["response"] == "deny":
                    print("[await_share_response] deny was sent")
                    return True
            await asyncio.sleep(5)
            ticker = ticker - 5
            print(f"[await_share_response] time left {ticker} seconds")
        return False