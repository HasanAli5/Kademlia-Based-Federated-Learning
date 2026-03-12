import asyncio
import threading
from argparse import ArgumentParser
from kademlia.network import Server
from medmnist import ChestMNIST
from torch.utils.data.dataset import Subset

from model import ResNet18
from network import *
from train import *
from aggregate import * 
from broadcast import *

from datetime import datetime

from torchvision.transforms import v2

import random


# this will have the main loop that will be carried out

async def cycle(model,dataloaders,node:Server,broadcast:Broadcast,ns_number):

    loop = asyncio.get_event_loop()
    # import model from file?

    # [TRAINING STAGE]
    print("[TRAINING STAGE]")

    train = Training_Stage()

    # start deny routine
    deny_task = loop.create_task(train.deny_aggregate_request(broadcast))

    # start the train corutine
    await train.training(model,dataloaders)

    # cancel deny after training complete
    deny_task.cancel()
    try: await deny_task
    except asyncio.CancelledError: print("Stopped waiting for deny_task to cancel")

    # [AGGREGATION STAGE]
    print("[AGGREGATION STAGE]")

    aggregate = Aggregate_Stage()

    # send aggregation request if possible to start network
    await aggregate.aggregate_request(broadcast)
    # get all denied request with a certain timeframe
    denied = await aggregate.await_aggregate_response(broadcast)

    # solve potential tied last using ns_number
    leading = False
    if not denied:
        leading = await aggregate.is_leader(node,broadcast,ns_number)

    print(f"Denied : {denied}\n"+
          f"Leading : {leading}")
    
    # initialise sharing node
    aggregation_node = Server()
    aggregation_broadcast = Broadcast(aggregation_node,8889,20)

    aggregation_relay_task = loop.create_task(aggregation_broadcast.start())

    # if denied then wait for the signal to connect instead
    if denied or not leading:
        leader_ip,leader_port = await aggregate.wait_for_leader(broadcast)
        await connect(aggregation_node,8561,leader_ip,leader_port)
    # else create network
    elif not denied and leading:
        await create(aggregation_node,8561)
        #send the join advert
        await aggregate.send_join_request(broadcast,8561)

    # do whole aggregation process.
    await aggregate.aggregation()
        
    # one node drops out of network while other stays to aggregate further

    aggregation_relay_task.cancel()
    try: await aggregation_relay_task
    except asyncio.CancelledError: print("Stopped Aggregation Relay")

    # [SHARING STAGE]
    
    # leaves one that will start to distibute the agregated result

    print(f"Cycle Ended")

if __name__ == "__main__":

    # Arguement Parser
    parser = ArgumentParser()
    parser.add_argument("-np","--nodeport",default = 8560)
    parser.add_argument("-i", "--ip", default=get_host())
    parser.add_argument("-p", "--port", default=None)
    args = parser.parse_args()

    # Event Loop
    loop = asyncio.new_event_loop()
    loop.set_debug(True)
    asyncio.set_event_loop(loop)

    # start model stuff

    # get device info
    device = torch.accelerator.current_accelerator().type if torch.accelerator.is_available() else "cpu"
    print(f"[{datetime.now().isoformat(" ")}] device : {device}")

    # image transform
    tf = v2.Compose([
        v2.ToImage(),
        v2.ToDtype(torch.float32,scale=True)
    ])

    # data
    train_data,val_data = ChestMNIST("train",transform=tf,download=True),ChestMNIST("val",transform=tf,download=True)

    # get the required parameters for model
    classes = len(train_data.info["label"])
    channels = val_data.info["n_channels"]

    # data loaders
    train_dl = DataLoader(Subset(train_data,random.sample(range(len(train_data)), 4096)),batch_size=1024,shuffle=True)
    val_dl = DataLoader(Subset(val_data,random.sample(range(len(val_data)), 4096)),batch_size=1024,shuffle=True)
    
    # initialise a model
    model = ResNet18(channels=channels,classes=classes).to(device)

    # start server
    node = Server()

    # north star number
    ns_number = -1

    # start either connect / create network
    if args.ip is None or args.port is None:
        # create
        ns_number = random.randint(0,2**160)
        loop.run_until_complete(create(node,args.nodeport))
        ns_number_set = False
        while not ns_number_set:
            ns_number_set = loop.run_until_complete(node.set("ns_number",f"{ns_number}"))
            if not ns_number_set:
                print("not found so on 5 second timeout")
                loop.run_until_complete(asyncio.sleep(5))
            else:
                print(f"ns_number set : {ns_number}")
    else:
        # connect
        loop.run_until_complete(connect(node,args.nodeport,args.ip,args.port))
        while ns_number<0:
            res = loop.run_until_complete(node.get("ns_number"))
            if res is not None:
                ns_number = int(res)
            if ns_number<0:
                print("not found so on 5 second timeout")
                loop.run_until_complete(asyncio.sleep(5))
        print(f"ns_number found : {ns_number}")
    
    # start relay system coroutine
    relay = Broadcast(node,8888,20)
    relay_task = loop.create_task(relay.start())

    # Run Cycles
    try:
        loop.run_until_complete(cycle(model,(train_dl,val_dl),node,relay,ns_number))
    except KeyboardInterrupt:
        pass
    finally:
        relay_task.cancel()
        try: loop.run_until_complete(relay_task)
        except asyncio.CancelledError: print("Stopped Relay")
        relay.end()
        node.stop()
        loop.close()
