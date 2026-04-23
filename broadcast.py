
from kademlia.network import Server
from kademlia.node import Node
from collections import deque
import asyncio
import json

from sympy import false, true
from triton import TritonError

import network
import time
import struct

class Broadcast():

    def __init__(self,node:Server,port:int,max_length:int):

        self.server = None
        self.process_task = None
        self.node = node
        self.port = port

        self.max_length = max_length

        self.buffer = asyncio.Queue(100)

        # normal list
        self.message_list = deque(maxlen=max_length)
        # used for quick looks
        self.message_lookup = set()

        self.ignore_list = set()

        self.messages_lock = asyncio.Lock()
        self.ignores_lock = asyncio.Lock()
        

    def get_message_lock(self):
        return self.messages_lock
    
    def get_ignores_lock(self):
        return self.ignores_lock

    def store_message(self,message:str):
        if message in self.message_lookup:
            return
        
        self.message_lookup.add(message)

        old_message = None

        if len(self.message_list)==self.max_length:
            old_message = self.message_list.popleft()
        
        self.message_list.append(message)

        if old_message:
            self.message_lookup.discard(old_message)

    def get_messages(self):
        # used to iterate through list
        return self.message_list
    
    def in_messages(self,message:str):
        # used for check value in list
        return message in self.message_lookup
        
    def delete_message(self,message:str):
        self.message_list.remove(message)
        self.message_lookup.discard(message)
    
    def in_ignores(self,message:str):
        return hash(message) in self.ignore_list

    def ignore_message(self,message:str):
        self.ignore_list.add(hash(message))
    
    def ignore_all_messages(self):
        messages = self.get_messages()
        for msg in messages:
            self.ignore_message(msg)
            self.delete_message(msg)

    def delete_and_ignore_message(self,message:str):
        self.ignore_message(message)
        self.delete_message(message)

    # extra message methods

    def make_message(self,relay:bool,extra_data:dict):
    # the message always has this data.
        data = {
            'source_ip':f'{network.get_host()}',
            'source_port':self.port,
            'source_node_id':self.node.node.long_id,
            'relay':relay,
            'timestamp':time.time()
        }
        data.update(extra_data)
        return data

    def convert_message(self,message:str):
        try:
            msg:dict[str,str] = json.loads(message)
            return msg
        except:
            # if malformed message then delete
            self.delete_and_ignore_message(message)
            return None
        
    async def find_messages(self,key:str,value:str):
        matches = []
        async with self.get_message_lock():
            messages = self.get_messages()
            for message in messages:
                msg = self.convert_message(message)
                if msg:
                    if msg.get(key) == value:
                        matches.append((message,msg))
        return matches
    
    async def clean_up_from_list(self,messages):
        if len(messages) > 0:
            async with self.get_message_lock():
                async with self.get_ignores_lock():
                    for message,msg in messages:
                        self.delete_and_ignore_message(message)

    async def auto_clear_messages(self,seconds_old):
        async with self.get_ignores_lock():
            expired = []
            for message in self.message_list:
                msg = json.loads(message)
                stamp = msg.get("timestamp")
                if time.time()-stamp > seconds_old:
                    expired.append((message,msg))
        await self.clean_up_from_list(expired)

    async def clear_message_by_stage(self,stage:str):
        async with self.get_ignores_lock():
            expired = []
            for message in self.message_list:
                msg = json.loads(message)
                request_stage = msg.get("stage")
                if request_stage == stage:
                    expired.append((message,msg))
        await self.clean_up_from_list(expired)

    async def clear_ignores(self):
        async with self.get_ignores_lock():
            self.ignore_list.clear()

    async def clear_messages(self):
        async with self.get_message_lock():
            self.message_list.clear()
            self.message_lookup.clear()
       
    # relay funtions

    async def single_relay(self,node:Node,message:str):
        if node.ip:
            await self.send(node.ip,self.port,message,relay=True)

    async def relay(self,message:str):
        if self.node.protocol:
            nodes = self.node.protocol.router.find_neighbors(self.node.node)
            relay_tasks = []
            for node in nodes:
                relay_tasks.append(self.single_relay(node,message))
            await asyncio.gather(*relay_tasks,return_exceptions=True)
        else:
            raise AttributeError()

    # recieve
    
    async def receive(self,reader:asyncio.StreamReader,writer:asyncio.StreamWriter):
        try:
            # recieve code
            length = struct.unpack("!I",await reader.readexactly(4))[0]
            data = await reader.read(length)
            await self.buffer.put(data)
            
        finally:
            writer.close()

    async def process_message(self):
        # this process takes from recieve.
        while True:
            # if in messages array already
            data = await self.buffer.get()

            message:dict = json.loads(data.decode())
            str_message = json.dumps(message)

            exists = False
            source_node = message.get("source_node_id")
            if source_node:
                is_sender = source_node == self.node.node.long_id
            else:
                is_sender = False

            async with self.messages_lock:
                for msg in self.get_messages():
                    if str_message == msg:
                        print("[broadcast.receive] found in messages")
                        exists = True

            async with self.ignores_lock:
                if self.in_ignores(str_message):
                    print("[broadcast.receive] found in ignore list")
                    exists = True

            # if doesnt exist and is not the sender
            if not exists and not is_sender:
                # stores and relays if not seen
                async with self.messages_lock:
                    self.store_message(str_message)
                if message.get("relay") == True:
                    await self.relay(str_message)

    # send

    async def send(self,peer_ip:str,peer_port,message:str,relay=False):
        if relay:
            print(f"[broadcast.send] relaying {peer_ip}:{peer_port} : {message}")
        else:
            print(f"[broadcast.send] sending {peer_ip}:{peer_port} : {message}")

        msg = self.convert_message(message)

        if msg is None:
            return
        
        elif msg.get("source_ip") == peer_ip:
            print("[broadcast.send] the source ip is the peer")
            return

        encoded_message = message.encode()
        length = struct.pack("!I",len(encoded_message))
        writer = None
        # intial + 3 retries
        for tries in range(4):
            try:
                _,writer = await asyncio.wait_for(asyncio.open_connection(peer_ip,peer_port),timeout=5)
                writer.write(length + encoded_message)
                await writer.drain()
                return
            except:
                print(f"[broadcast.send] retrying ({tries+1}) {peer_ip}:{peer_port} : {message}")
                await asyncio.sleep(1)
            finally:
                if writer:
                    writer.close()
                    try: await writer.wait_closed()
                    except: pass

    # server functionality

    async def start(self):
        self.server = await asyncio.start_server(self.receive,"0.0.0.0",self.port)
        self.process_task = asyncio.create_task(self.process_message())
        await self.server.serve_forever()

    async def end(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        if self.process_task:
            self.process_task.cancel()
            try: await self.process_task
            except asyncio.CancelledError: pass