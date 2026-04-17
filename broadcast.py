
from kademlia.network import Server
from kademlia.node import Node
import asyncio
import json
import network
import time

class Broadcast():

    def __init__(self,node:Server,port:int,max_length:int):

        self.max_length = max_length
        self.ignorelist = []
        self.messages = []
        self.messages_lock = asyncio.Lock()
        self.loop = None
        self.server = None
        self.node = node
        self.port = port

    async def store(self,message:str):
        async with self.messages_lock:
            self.messages.append(message)
            if len(self.messages)>self.max_length:
                print("[broadcast.store] overflow")
                self.messages.pop(0)
    
    async def get_messages(self):
        async with self.messages_lock:
            return self.messages
        
    async def delete_message(self,message):
        async with self.messages_lock:
            self.messages.remove(message)

    async def get_ignores(self):
        async with self.messages_lock:
            return self.ignorelist

    async def ignore_message(self,message):
        async with self.messages_lock:
            self.ignorelist.append(hash(message))
    
    async def filter_messages(self,key,value):
        async with self.messages_lock:
            for msg in self.messages:
                if msg.get(key) == value:
                    self.messages.remove(msg)

    async def ignore_all_messages(self):
        async with self.messages_lock:
            for msg in self.messages:
                self.ignorelist.append(hash(msg))
            self.messages = []

    # extra message methods

    def make_message(self,relay:bool,extra_data:dict):
    # the message always has this data.
        data = {
            'source_ip':f'{network.get_host()}',
            'source_port':f'{self.port}',
            'source_node_id':f'{self.node.node.long_id}',
            'relay':f'{relay}',
            'timestamp':f'{time.time()}'
        }
        data.update(extra_data)
        return data

    async def convert_message(self,message):
        msg = None
        try:
            msg:dict = json.loads(message)
        except:
            # if malformed message then delete
            await self.delete_and_ignore_message(message)
        return msg

    async def delete_and_ignore_message(self,message):
        await self.ignore_message(message)
        await self.delete_message(message)

    # relay funtions

    async def single_relay(self,node:Node,message:str):
        # intial + 3 retries
        for tries in range(4):
                try:
                    print(f"[broadcast.relay] relaying {message} to {node.ip}:{self.port}")
                    _,writer = await asyncio.wait_for(asyncio.open_connection(node.ip,self.port),timeout=5)
                    writer.write(message.encode())
                    await writer.drain()
                    writer.close()
                    await writer.wait_closed()
                    break
                except:
                    print(f"[broadcast.relay] retrying ({tries+1}) {message} to {node.ip}:{self.port}")
                    await asyncio.sleep(1)


    async def relay(self,message:str):
        nodes = self.node.protocol.router.find_neighbors(self.node.node)
        relay_tasks = []
        for node in nodes:
            relay_tasks.append(self.single_relay(node,message))
        await asyncio.gather(*relay_tasks,return_exceptions=True)

    # recieve
    
    async def receive(self,reader:asyncio.StreamReader,writer:asyncio.StreamWriter):
        # recieve code
        data = await reader.read()
        message = data.decode()
        print(f"[broadcast.receive] received {message}")
        
        exists = False

        messages = await self.get_messages()
        for msg in messages:
            if message == msg:
                print("[broadcast.receive] found in messages")
                exists = True
        for ignore in self.ignorelist:
            if ignore == hash(message):
                print("[broadcast.receive] found in ignore list")
                exists = True

        if not exists and not (json.loads(message)['source_ip'] == network.get_host()):
            # stores and relays if not seen
            await self.store(message)
            if json.loads(message)["relay"]:
                await self.relay(message)
        
        writer.close()
        await writer.wait_closed()

    # send

    async def send(self,peer_ip:str,peer_port:int,message:str):
        print(f"[broadcast.send] sending {peer_ip}:{peer_port} : {message}")
        # intial + 3 retries
        for tries in range(4):
            try:
                _,writer = await asyncio.wait_for(asyncio.open_connection(peer_ip,peer_port),timeout=5)
                writer.write(message.encode())
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                return
            except:
                print(f"[broadcast.send] retrying ({tries+1}) {peer_ip}:{peer_port} : {message}")
                await asyncio.sleep(1)

    # server functionality

    async def start(self):
        self.server = await asyncio.start_server(self.receive,"0.0.0.0",self.port)
        await self.server.serve_forever()

    def end(self):
        self.server.close()