import asyncio
import threading
from argparse import ArgumentParser
from kademlia.network import Server
from medmnist import ChestMNIST
from torch.utils.data.dataset import Subset

from model import ResNet18
from network import *
from train import *
from broadcast import *

from datetime import datetime

from torchvision.transforms import v2

import random


# this will have the main loop that will be carried out

async def cycle(node:Server,broadcast:Broadcast,ns_number):

    loop = asyncio.get_event_loop()

    device = torch.accelerator.current_accelerator().type if torch.accelerator.is_available() else "cpu"
    print(f"[{datetime.now().isoformat(" ")}] device : {device}")

    tf = v2.Compose([
        v2.ToImage(),
        v2.ToDtype(torch.float32,scale=True)
    ])

    train_data,val_data = ChestMNIST("train",transform=tf,download=True),ChestMNIST("val",transform=tf,download=True)

    classes = len(train_data.info["label"])
    channels = val_data.info["n_channels"]

    # initialise a model
    model = ResNet18(channels=channels,classes=classes).to(device)

    # import model from file?

    # start the train corutine
    # start deny routine

    train_dl = DataLoader(Subset(train_data,random.sample(range(len(train_data)), 4096)),batch_size=1024,shuffle=True)
    val_dl = DataLoader(Subset(val_data,random.sample(range(len(val_data)), 4096)),batch_size=1024,shuffle=True)

    train = Training_Stage()

    deny_task = loop.create_task(train.deny_share_request(broadcast))

    await train.training(model,(train_dl,val_dl))

    deny_task.cancel()
    try: await deny_task
    except asyncio.CancelledError: print("Stop waiting for deny_task to cancel")

    await train.share_request(node,broadcast)
    # get all denied request with a certain timeframe
    denied = await train.await_share_response(broadcast)

    # solve tie break using ns_number
    if denied != False:
        #await is_leader()
        pass

    print(f"Denied : {denied}")

    # if denied then wait for the signal to connect instead
    # else create network

    # pair up the nodes and start sharing process
    # one node drops out of network while other stays

    # leaves one that will start to distibute the agregated result
    # repeat
    pass

if __name__ == "__main__":

    # Arguement Parser
    parser = ArgumentParser()
    parser.add_argument("-np","--nodeport",default = 8560)
    parser.add_argument("-i", "--ip", default=get_host())
    parser.add_argument("-p", "--port", default=None)
    args = parser.parse_args()

    # node_port
    node_port = args.nodeport

    # Event Loop
    loop = asyncio.new_event_loop()
    loop.set_debug(True)
    asyncio.set_event_loop(loop)

    # start server
    node = Server()

    # north star number
    ns_number = -1

    # start either connect / create network
    if args.ip is None or args.port is None:
        # create
        ns_number = random.randint(0,2**160)
        loop.run_until_complete(create(node,node_port))
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
        loop.run_until_complete(connect(node,node_port,args.ip,args.port))
        while ns_number<0:
            res = loop.run_until_complete(node.get("ns_number"))
            if res is not None:
                ns_number = int(res)
            if ns_number<0:
                print("not found so on 5 second timeout")
                loop.run_until_complete(asyncio.sleep(5))
        print(f"ns_number found : {ns_number}")

    processes = set()

    # Run Cycles
    relay = Broadcast(node,20)
    relay_task = loop.create_task(relay.start())
    try:
        loop.run_until_complete(cycle(node,relay,ns_number))
    except KeyboardInterrupt:
        pass
    finally:
        relay_task.cancel()
        relay.end()
        node.stop()
        loop.close()
