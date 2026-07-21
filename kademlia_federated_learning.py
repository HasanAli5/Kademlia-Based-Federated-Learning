import asyncio
from copy import deepcopy
import random
from typing import Type
from kademlia.network import Server
import torch

# custom
from model import ResNet18,CNNBasic
from network import Network
from train import *
from aggregate import *
from share import * 

class Kademlia_Federated_Learning():

    def __init__(self,args,
                 relay_port:int,kademlia_port:int,
                 a_relay_port:int,a_kademlia_port:int,
                 model_transfer_port:int):
        
        #connetors
        self.nodeport = args.nodeport
        self.ip = args.ip
        self.port = args.port

        # ports
        self.rp = relay_port
        self.kp = kademlia_port
        self.arp = a_relay_port
        self.akp = a_kademlia_port
        self.mtp = model_transfer_port

        self.data_toolbox = Data_Manager()

        classes = len(self.data_toolbox.train_data.info["label"])
        channels = self.data_toolbox.train_data.info["n_channels"]

        model = self.make_model(ResNet18,classes,channels)
        self.model_toolbox = Model_Manager(model)
        
        # start server
        self.node = Server()

        # start relay system coroutine
        self.network = Network(self.node,port=self.rp,buffer_length=100,messages_length=100,ignores_length=100,model_transfer_port=model_transfer_port)
        self.relay_task = asyncio.create_task(self.network.start())

        self.train = Train(self.network)
        self.aggregate = Aggregate(self.network,a_relay_port,a_kademlia_port,model_transfer_port)
        self.share = Share(self.network,model_transfer_port)

        self.epochs = 5
        self.minimum_train_wait = 60

        self.model_task = None

        self.save_global_model_per_round = True

    def make_model(self,model_class:Type[nn.Module],classes:int,channels:int):
        model = model_class(channels=channels,classes=classes)
        return model

    async def stop_task(self,task:asyncio.Task,name:str):
        task.cancel()
        try: await task
        except asyncio.CancelledError: print(f"[CYCLE] Stopped waiting for {name} to cancel")


    async def cycle(self):
        # [TRAINING STAGE]
        print("\n[CYCLE] [TRAINING STAGE]\n")

        await asyncio.sleep(10)

        await self.train.set_state(self.train.STATE_PENDING)
       
        start_training_response_task = asyncio.create_task(self.train.responder(cooldown=2))

        allowed_to_train = await self.train.request_to_train(response_wait=30,exit_wait=30,cooldown=4)

        await self.network.clear_message_by_stage("share")

        not_in_task = None

        if allowed_to_train:

            await self.train.set_state(self.train.STATE_ACTIVE)

            # start deny routine
            deny_task = asyncio.create_task(self.train.deny_aggregate_request(cooldown=2))

            start_of_train = time.time()

            # start the train corutine
            await self.train.training(self.model_toolbox.model,self.data_toolbox,self.model_toolbox,epochs=self.epochs)

            end_of_train = time.time()

            elapsed_time = end_of_train - start_of_train

            #stop nodes with quick training
            await asyncio.sleep(max(0.1,self.minimum_train_wait-elapsed_time))

            # cancel deny after training complete
            await self.stop_task(deny_task,"deny_task")

            await self.train.set_state(self.train.STATE_WAITING)

            # [AGGREGATION STAGE]
            print("\n[CYCLE] [AGGREGATION STAGE]\n")

            # get all denied request with a certain timeframe
            await self.aggregate.sync_and_join_aggregation()

            # fully exit from training stage so set train state to inactive
            await self.train.set_state(self.train.STATE_INACTIVE)

            # initialise sharing node

            if self.aggregate.a_network:
            
                print("[CYCLE] made aggregation node")

                aggregation_relay_task = asyncio.create_task(self.aggregate.a_network.start())

                self.aggregate.set_denial_task(cooldown=2)

                # do whole aggregation process.
                model = await self.aggregate.aggregation(self.model_toolbox.model)

                not_in_task = asyncio.create_task(self.aggregate.response_not_in(cooldown=2))

                # get rid message that may be around after aggregation
                await self.network.clear_message_by_stage("train")
            
            else:
                model = None
                print("\n[CYCLE] a_network was not set\nSkipping to Share Stage\n")
                await self.network.clear_message_by_stage("train")
        else:
            model = None
            print("\n[CYCLE] nodes already finished training\nSkipping to Share Stage\n")
            await self.network.clear_message_by_stage("train")
        
        # [SHARING STAGE]
        print("\n[CYCLE] [SHARING STAGE]\n")

        await self.share.set_status(False)
        ready_responder = asyncio.create_task(self.share.send_ready_response(cooldown=2))

        if model is not None:
            if self.save_global_model_per_round:
                self.model_toolbox.save_global_model(model)
            ns_number = random.randint(0,2**160)
            ns_number_ts = time.time()
            data = {
                "ns_number":ns_number,
                "ns_number_ts":ns_number_ts
            }
            await self.network.set_ns_number(ns_number,ns_number_ts)
            await self.network.node.set("ns_number",json.dumps(data))

        while model == None:
            print("\n[CYCLE] waiting for a model\n")
            model_dict = await self.share.share_model_reponse()
            if not model_dict is None:
                print("[CYCLE] got a model")
                self.model_toolbox.model.load_state_dict(model_dict)
                model = deepcopy(self.model_toolbox.model)
                await asyncio.sleep(10)

        print("\n[CYCLE] sending global model to neighbours\n")
        
        await self.aggregate.stop_denial_task()
        if not_in_task:
            await self.stop_task(not_in_task,"not_in_task")

        share_responder_task = asyncio.create_task(self.share.share_model_reponse())

        await self.share.check_accept_models(model,wait_time=35,ready_wait_time=30,cooldown=2)

        # stop response task before syncing
        await self.stop_task(ready_responder,"not_ready")

        await self.stop_task(start_training_response_task,"start_training_response_task")

        await self.share.sync()
        
        await self.stop_task(share_responder_task,"\nshare_responder_task\n")

        if allowed_to_train and self.aggregate.a_network:
            # end the aggregation server when finished and stop the task entirely
            await self.stop_task(aggregation_relay_task,"aggregation_relay_task")
            await self.aggregate.a_network.end()
            self.aggregate.aggregation_node.stop()

        await self.network.clear_message_by_stage("aggregate")
        
        print(f"\n[CYCLE] Cycle Ended\n")

        if not allowed_to_train:
            #pads for results
            for epoch in range(self.epochs):
                logs = self.model_toolbox.logs
                logs[0][0].append(logs[0][0][-1])
                logs[0][1].append(logs[0][1][-1])
                logs[1][0].append(logs[1][0][-1])
                logs[1][1].append(logs[1][1][-1])

        self.model_toolbox.save_logs()
    
        await self.stop_task(self.model_task,"model_global_sender_task")
        # remake the model global task with the new global model
        self.model_task = asyncio.create_task(self.network.model_sender(self.model_toolbox.model))


    async def connect_to_kademlia(self,node:Server,nodeport:int,ip:str|None=None,port:int|None=None):
        # north star number
        ns_number = -1
        # start either connect / create network
        if ip is None or port is None:
            # create
            ns_number = random.randint(0,2**160)
            ns_number_ts = time.time()
            data = {
                "ns_number":ns_number,
                "ns_number_ts":ns_number_ts
            }
            await self.network.set_ns_number(ns_number,ns_number_ts)
            await self.network.create(node,nodeport)
            ns_number_set = False
            while not ns_number_set:
                ns_number_set = await node.set("ns_number",json.dumps(data))
                if not ns_number_set:
                    print("[connect_to_kademlia] not found so on 5 second timeout")
                    await asyncio.sleep(5)
                else:
                    print(f"[connect_to_kademlia] ns_number set : {ns_number}")
                    self.model_task = asyncio.create_task(self.network.model_sender(self.model_toolbox.model))
                    
        else:
            # connect
            await self.network.connect(node,nodeport,ip,port)

            while ns_number<0:
                res = await node.get("ns_number")
                if res is not None:
                    message = json.loads(res)
                    ns_number = message.get("ns_number")
                    ns_number_ts = message.get("ns_number_ts")
                    await self.network.set_ns_number(ns_number,ns_number_ts)

                if ns_number<0:
                    print("[connect_to_kademlia] not found so on 5 second timeout")
                    await asyncio.sleep(5)
                else:
                    self.model_toolbox.model.load_state_dict(await self.network.model_get())
                    self.model_task = asyncio.create_task(self.network.model_sender(self.model_toolbox.model))
                    print("model loaded")

            print(f"[connect_to_kademlia] ns_number found : {ns_number}")
        node.refresh_table(60)
        return


    async def run(self):
        await self.connect_to_kademlia(self.node,self.nodeport,self.ip,self.port)
        # Run Cycles
        try:
            while True:
                await self.cycle()
        except KeyboardInterrupt:
            print("[MAIN] Interrupted")
        #except Exception as e:
        #    print(f"[run] Exception : {e}")
        finally:
            await self.stop()

    async def stop(self):
        await self.network.end()
        self.node.stop()
        self.relay_task.cancel()
        try: await self.relay_task
        except asyncio.CancelledError: print("[MAIN] Stopped Relay")
