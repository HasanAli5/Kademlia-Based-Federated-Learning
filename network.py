from csv import reader
from typing import Any
from kademlia.network import Server
from kademlia.node import Node
from collections import deque
import io
import asyncio
import json
import socket
import pickle
import time
import struct
import copy

import torch

class Network():

    def __init__(self,node:Server,port:int,
                 buffer_length:int,messages_length:int,ignores_length:int,
                 model_transfer_port):

        self.node = node
        self.port = port
        
        self.server = None
        self.process_task = None

        self.buffer = asyncio.Queue(buffer_length)

        self.message_list = deque(maxlen=messages_length)
        self.message_lookup = set()

        self.ignore_list = deque(maxlen=ignores_length)
        self.ignore_lookup = set()

        self.messages_lock = asyncio.Lock()
        self.ignores_lock = asyncio.Lock()

        self.ns_number = -1
        self.ns_number_ts = -1
        self.ns_lock = asyncio.Lock()
        self.mtp = model_transfer_port

    # get locks

    def get_message_lock(self):
        return self.messages_lock
    
    def get_ignores_lock(self):
        return self.ignores_lock
    
    # message functions

    def get_messages(self):
        # used to iterate through list
        return self.message_list
    
    def in_messages(self,message:str):
        # used for check value in list
        return message in self.message_lookup
    
    def store_message(self,message:str):
        # if already there dont add
        if message in self.message_lookup:
            return
        
        # remove older message if full
        if len(self.message_list)==self.message_list.maxlen:
            old_message = self.message_list.popleft()
            self.message_lookup.discard(old_message)
        
        # add to the mesages
        self.message_lookup.add(message)
        self.message_list.append(message)
        
    def delete_message(self,message:str):
        self.message_list.remove(message)
        self.message_lookup.discard(message)

    # ignore functions

    def ignore_message(self,message:str):
        # if already there dont add
        if hash(message) in self.ignore_lookup:
            return
        
        # remove older message if full
        if len(self.ignore_list)==self.ignore_list.maxlen:
            old_message = self.ignore_list.popleft()
            self.ignore_lookup.discard(old_message)
        
        # add to the mesages
        self.ignore_lookup.add(hash(message))
        self.ignore_list.append(hash(message))

    def in_ignores(self,message:str):
        return hash(message) in self.ignore_lookup
    
    # message => ignore list

    def delete_and_ignore_message(self,message:str):
        self.ignore_message(message)
        self.delete_message(message)

    # flush lists

    async def clear_ignores(self):
        async with self.get_ignores_lock():
            self.ignore_list.clear()
            self.ignore_lookup.clear()

    async def clear_messages(self):
        async with self.get_message_lock():
            self.message_list.clear()
            self.message_lookup.clear()

    # extra message methods

    async def make_message(self,relay:bool,extra_data:dict):
        # the message always has this data.
        ns_number,ns_number_ts = await self.get_ns_number()
        data = {
            'source_ip':f'{self.get_host()}',
            'source_port':self.port,
            'source_node_id':self.node.node.long_id,
            'source_ns_number':ns_number,
            'source_ns_number_ts':ns_number_ts,
            'relay':relay,
            'timestamp':time.time()
        }
        data.update(extra_data)
        return data

    def attach_request_info(self,data:dict,timestamp:float):
    # the message always has this data.
        extra_data = {
            'request_timestamp':timestamp
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
        
    async def find_messages(self,lookup_dict:dict):
        matches:list[tuple[Any,dict]] = []
        async with self.get_message_lock():
            messages = self.get_messages()
            for message in messages:
                msg = self.convert_message(message)
                if msg:
                    contains_lookup = True
                    for key in lookup_dict.keys():
                        if msg.get(key) != lookup_dict.get(key):
                            contains_lookup = False
                            break
                    if contains_lookup:
                        matches.append((message,msg))
        return matches
    
    async def clean_up_from_list(self,messages):
        if len(messages) > 0:
            async with self.get_message_lock():
                async with self.get_ignores_lock():
                    for message,msg in messages:
                        self.delete_and_ignore_message(message)

    async def clear_messages_by_time(self,seconds_old:int):
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
                reader,writer = await asyncio.wait_for(asyncio.open_connection(peer_ip,peer_port),timeout=5)
                writer.write(length + encoded_message)
                await writer.drain()
                await reader.read(1)
                return
            except:
                print(f"[broadcast.send] retrying ({tries+1}) {peer_ip}:{peer_port} : {message}")
                await asyncio.sleep(1)
            finally:
                if writer:
                    writer.close()
                    try: await writer.wait_closed()
                    except: pass

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
            writer.write(b"\x01") 
            await writer.drain()
            await asyncio.sleep(0.1)
        except Exception as e:
            print(f"Exception : {e}")
        finally:
            if writer:
                writer.close()
                try: await writer.wait_closed()
                except:pass

    # processing function (runs in task)

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
                        #print("[broadcast.receive] found in messages")
                        exists = True

            async with self.ignores_lock:
                if self.in_ignores(str_message):
                    #print("[broadcast.receive] found in ignore list")
                    exists = True

            # if doesnt exist and is not the sender
            if not exists and not is_sender:
                source_ns_number = message.get("source_ns_number")
                source_ns_number_ts = message.get("source_ns_number_ts")
                # if the a newer ns number found then replace existing one
                await self.set_ns_number(source_ns_number,source_ns_number_ts)
                # stores and relays if not seen
                async with self.messages_lock:
                    self.store_message(str_message)
                if message.get("relay") == True:
                    await self.relay(str_message)

    # networking functions

    async def is_leading_peer(self,node_id,peer_node_id):
        # leading peer is closest to ns_number
        ns_number,_ = await self.get_ns_number()
        node_long = node_id
        peer_long = peer_node_id
        distance = abs(ns_number-node_long)
        peer_distance = abs(ns_number-peer_long)
        is_leading_peer = None
        # tie breaker term
        if distance == peer_distance:
            # default to greater number wins leader
            is_leading_peer = node_long > peer_long
        else:
            # leader if closer to ns_number
            is_leading_peer = peer_distance > distance
        return is_leading_peer

    def get_host(self):
        s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        s.connect(("8.8.8.8",80))
        host = s.getsockname()[0]
        return host

    async def create(self,node:Server,node_port):

        await node.listen(node_port,interface="0.0.0.0")
        print(f"listening on port {node_port}")
        await asyncio.sleep(1)

    # connect to network

    async def connect(self,node:Server,node_port,peer_ip,peer_port):
        # wait some time for host to start up
        #await asyncio.sleep(5)
        await node.listen(node_port,interface="0.0.0.0")
        print(f"listening on port {node_port}")
        bootstrap_node = (peer_ip, int(peer_port))
        print(f"trying to connect to {peer_ip}:{peer_port}")
        connected = None
        attempts = 1
        # attempt 60 times (for 5 mins)
        while attempts<61:
            connected = await node.bootstrap([bootstrap_node])
            if connected:
                print(f"[connect] Connected after {attempts} attempts")
                break
            else:
                print(f"[connect] Attempt {attempts} failed. retrying in 5 seconds.")
                await asyncio.sleep(5)
                attempts=attempts+1
        await asyncio.sleep(1)
        return connected
    
    # model send/receive

    async def send_model(self,ip,port,model,weighting,wait_time):
        cooldown = 2
        writer = None
        try:
            for i in range(0,wait_time,cooldown):
                try:
                    reader,writer = await asyncio.wait_for(asyncio.open_connection(ip,port),timeout=5)
                    buffer = io.BytesIO()
                    #data = pickle.dumps(model)
                    torch.save(model.state_dict(),buffer)
                    data = buffer.getvalue()
                    print(f"[send_model] Sending Model Data: Length {len(data)} and Weighting {weighting}")
                    length = struct.pack("!I",len(data))
                    enc_weighting = struct.pack("!I",weighting)
                    writer.write(length+enc_weighting+data)
                    await writer.drain()
                    await reader.read(1)
                    return True
                except (ConnectionRefusedError,asyncio.TimeoutError):
                    await asyncio.sleep(cooldown)
                except Exception as e:
                    print(f"[send_model] Exception : {e}")
                finally:
                    if writer:
                        writer.close()
                        try:await writer.wait_closed()
                        except asyncio.CancelledError:pass
            return False
        except Exception as e:
            print(f"[send_model] Exception : {e}")

    async def recieve_model(self,port,connect_timeout,model_timeout):
        connect_future = asyncio.Future()
        model_future = asyncio.Future()

        async def handler(reader:asyncio.StreamReader,writer:asyncio.StreamWriter):
            #get 4 byte int length
            try:
                # started handler function so activate to prevent stopping
                length = struct.unpack("!I",await reader.readexactly(4))[0]
                weighting = struct.unpack("!I",await reader.readexactly(4))[0]
                print(f"[recieve_model] Recieving Model Data: Length {length} and Weighting {weighting}")
                connect_future.set_result(None)
                data = await reader.readexactly(length)
                buffer = io.BytesIO(data)
                #data = pickle.dumps(model)
                model_dict = torch.load(buffer,map_location='cpu')
                #data = buffer.getvalue()
                #model = pickle.loads(data)
                if not model_future.done():
                    model_future.set_result((model_dict,weighting))
                    writer.write(b"\x01") 
                    await writer.drain()
                    await asyncio.sleep(0.1)
            except Exception as e:
                print(f"[recieve_model] handler Exception : {e}")
                if not model_future.done():
                    model_future.set_exception(e)
            finally:
                writer.close()
                try:await writer.wait_closed()
                except:pass

        server = await asyncio.start_server(handler,"0.0.0.0",port)

        try:
            async with server:
                # wait for the trigger in finally block
                await asyncio.wait_for(connect_future,connect_timeout)
                return await asyncio.wait_for(model_future,model_timeout)
        except (asyncio.TimeoutError,Exception):
            #happens when connect doesnt start or issue
            return None,None
        finally:
            server.close()
            await server.wait_closed()

    async def get_ns_number(self):
        async with self.ns_lock:
            return self.ns_number,self.ns_number_ts
        
    async def set_ns_number(self,ns_number,ns_number_ts):
        async with self.ns_lock:
            #only store new values
            if ns_number_ts>self.ns_number_ts:
                self.ns_number = ns_number
                self.ns_number_ts = ns_number_ts

    async def model_sender(self,model):
        #any specific request for the model is sent here
        print("[model_sender] Started")
        globalmodel = copy.deepcopy(model)
        while True:
            try:
                global_model_requests = await self.find_messages({"request":"global_model_request"})
                for _,msg in global_model_requests:
                    peer = (msg.get("source_ip"),msg.get("source_port"))
                    print(f"[model_sender] found global_model_request sending model to {peer[0]}")
                    await self.send_model(peer[0],self.mtp,globalmodel,0,30)
                await self.clean_up_from_list(global_model_requests)
                if len(global_model_requests)==0:
                    # long sleep
                    await asyncio.sleep(10)
            except Exception as e:
                print(f"[model_sender] Exception : {e}")
    
    async def send_model_request(self):
        data = {
            'request':'global_model_request'
        }
        data = await self.make_message(relay=True,extra_data=data)
        try:
            message = json.dumps(data)
            await self.relay(message)
            print("[send_model_request] model request set")
        except Exception as e:
            print(f"[send_model_request] Exception : {e}")

    async def model_get(self):

        await self.send_model_request()
        await asyncio.sleep(3)

        while True:
            try:
                model_dict,_ = await self.recieve_model(self.mtp,30,9999)
                if model_dict:
                    return model_dict
                else:
                    await self.send_model_request()
                    await asyncio.sleep(3)
            except Exception as e:
                print(f"[model_get] Exception : {e}")

    # server functionality

    async def start(self):
        try:
            self.server = await asyncio.start_server(self.receive,"0.0.0.0",self.port)
            self.process_task = asyncio.create_task(self.process_message())
            await self.server.serve_forever()
        except Exception as e:
            print(f"[start] Exception {e}")

    async def end(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        if self.process_task:
            self.process_task.cancel()
            try: await self.process_task
            except asyncio.CancelledError: pass