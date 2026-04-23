from ast import mod
import asyncio
import random
from tracemalloc import stop
from typing import Type
from kademlia.network import Server
from argparse import ArgumentParser

# custom
from model import ResNet18,CNNBasic
import network
from broadcast import *
from broadcast_alt import Broadcast as aBroadcast
from train import *
from aggregate import *
from sharing import * 

# ports
BROADCAST_PORT = 8888
KADEMLIA_PORT = 8560
A_BROADCAST_PORT = 8889
A_KADEMLIA_PORT = 8561

async def stop_task(task:asyncio.Task,name:str):
    task.cancel()
    try: await task
    except asyncio.CancelledError: print(f"[CYCLE] Stopped waiting for {name} to cancel")


async def cycle(model_toolbox:Model_Manager,data_toolbox:Data_Manager,broadcast:Broadcast,ns_number:int):
    # [TRAINING STAGE]
    print("[CYCLE] [TRAINING STAGE]")

    train = Training_Stage()

    # start deny routine
    deny_task = asyncio.create_task(train.deny_aggregate_request(broadcast,cooldown=2))
    start_training_response_task = asyncio.create_task(train.training_responding(broadcast,cooldown=2))

    await train.training_requesting(broadcast,resend_wait=30,cooldown=2)

    # start the train corutine
    await train.training(model_toolbox.model,data_toolbox,model_toolbox,epochs=1)

    # cancel deny after training complete
    await stop_task(deny_task,"deny_task")

    # cleanup from last cycle
    await broadcast.clear_message_by_stage("share")
    # [AGGREGATION STAGE]
    print("[CYCLE] [AGGREGATION STAGE]")

    aggregate = Aggregate_Stage()

    # send aggregation request if possible to start network
    
    # get all denied request with a certain timeframe
    denied = await aggregate.await_aggregate_response(broadcast,wait_time=30,cooldown=2)

    # solve potential tied last using ns_number
    
    if not denied:
        leading = await aggregate.is_leader(broadcast,ns_number)
    else:
        leading = False

    start_training_response_task.cancel()
    try: await start_training_response_task
    except asyncio.CancelledError: print("[CYCLE] Stopped waiting for start_training_response_task to cancel")
    await broadcast.clear_message_by_stage("train")

    print(f"[CYCLE] Denied : {denied}\n"+
          f"[CYCLE] Leading : {leading}")
    
    # initialise sharing node
    aggregation_node = Server(ksize=4)
    aggregation_broadcast = aBroadcast(aggregation_node,port=A_BROADCAST_PORT,max_length=50)

    print("[CYCLE] made aggregation node")

    aggregation_relay_task = asyncio.create_task(aggregation_broadcast.start())

    # if denied then wait for the signal to connect instead
    if denied or not leading:
        leader_ip,leader_port = await aggregate.wait_for_leader(broadcast,cooldown=2)
        await network.connect(aggregation_node,node_port=A_KADEMLIA_PORT,peer_ip=leader_ip,peer_port=leader_port)
    # else create network
    elif not denied and leading:
        await network.create(aggregation_node,A_KADEMLIA_PORT)
        #send the join advert with the new port
        await aggregate.send_join_request(broadcast,A_KADEMLIA_PORT)

    aggregate.set_denial_task(aggregation_broadcast,cooldown=2)

    # do whole aggregation process.
    model = await aggregate.aggregation(aggregation_broadcast,model_toolbox.model,ns_number)

    not_in_task = asyncio.create_task(aggregate.response_not_in(aggregation_broadcast,cooldown=2))

    # get rid message that may be around after aggregation
    await broadcast.clear_message_by_stage("train")
    
    # [SHARING STAGE]

    sharing = Sharing_Stage()

    not_ready_task = asyncio.create_task(sharing.send_not_ready_response(broadcast,cooldown=2))

    if model is not None:
        new_ns_number = random.randint(0,2**160)
        await broadcast.node.set("ns_number",str(ns_number))

    while model == None:
        print("[CYCLE] waiting for a model")
        model,new_ns_number = await sharing.share_model_reponse(broadcast)

    print("[CYCLE] sending global model to neighbours")

    await aggregate.stop_denial_task()
    await stop_task(not_in_task,"not_in_task")

    share_responder_task = asyncio.create_task(sharing.share_model_reponse(broadcast))
    await sharing.send_share_model_request(broadcast,new_ns_number)
    await sharing.check_accept_models(broadcast,model,new_ns_number,k=4)

    await stop_task(not_ready_task,"not_ready")

    is_ready = await sharing.get_ready_response(broadcast,2,60)
    if is_ready:
        # we send the global request to go
        await sharing.send_go_request(broadcast,cooldown=2)
    else:
        await sharing.wait_for_last(broadcast,cooldown=2)

    await stop_task(share_responder_task,"share_responder_task")

    await stop_task(aggregation_relay_task,"aggregation_relay_task")

    aggregation_node.stop()

    await broadcast.clear_message_by_stage("aggregate")
    print(f"[CYCLE] Cycle Ended")
    model_toolbox.save_logs()
    return new_ns_number


def make_model(model_class:Type[nn.Module],classes:int,channels:int):
    # get device info
    device = torch.accelerator.current_accelerator().type if torch.accelerator.is_available() else "cpu" # type: ignore
    # initialise a model
    #model = ResNet18(channels=channels,classes=classes)
    model = model_class(channels=channels,classes=classes)
    model.parameters()
    model.to(device)
    return model


async def connect_to_kademlia(node:Server,nodeport:int,ip:str|None=None,port:int|None=None):
    # north star number
    ns_number = -1
    # start either connect / create network
    if ip is None or port is None:
        # create
        ns_number = random.randint(0,2**160)
        await network.create(node,nodeport)
        ns_number_set = False
        while not ns_number_set:
            ns_number_set = await node.set("ns_number",str(ns_number))
            if not ns_number_set:
                print("[connect_to_kademlia] not found so on 5 second timeout")
                await asyncio.sleep(5)
            else:
                print(f"[connect_to_kademlia] ns_number set : {ns_number}")
    else:
        # connect
        await network.connect(node,nodeport,ip,port)
        while ns_number<0:
            res = await node.get("ns_number")
            if res is not None:
                ns_number = int(res)
            if ns_number<0:
                print("[connect_to_kademlia] not found so on 5 second timeout")
                await asyncio.sleep(5)
        print(f"[connect_to_kademlia] ns_number found : {ns_number}")
    return ns_number

async def main(args):
    # start model stuff

    data_toolbox = Data_Manager()

    classes = len(data_toolbox.train_data.info["label"])
    channels = data_toolbox.train_data.info["n_channels"]

    model = make_model(CNNBasic,classes,channels)

    model_toolbox = Model_Manager(model,learning_rate=1e-5,decay_rate=1e-3)
    
    # start server
    node = Server(ksize=4)

    ns_number = await connect_to_kademlia(node,args.nodeport,args.ip,args.port)

    # start relay system coroutine
    relay = Broadcast(node,port=BROADCAST_PORT,max_length=50)
    relay_task = asyncio.create_task(relay.start())

    # Run Cycles
    try:
        while True:
            ns_number = await cycle(model_toolbox,data_toolbox,relay,ns_number)
    except KeyboardInterrupt:
        print("[MAIN] Interrupted")
    finally:
        await relay.end()
        node.stop()
        relay_task.cancel()
        try: await relay_task
        except asyncio.CancelledError: print("[MAIN] Stopped Relay")
        
if __name__ == "__main__":

    # Arguement Parser
    parser = ArgumentParser()
    parser.add_argument("-np","--nodeport",default = KADEMLIA_PORT)
    parser.add_argument("-i", "--ip", default=get_host())
    parser.add_argument("-p", "--port", default=None)
    args = parser.parse_args()

    asyncio.run(main(args))

    