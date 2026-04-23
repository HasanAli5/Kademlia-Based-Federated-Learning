from os import write

from kademlia.network import Server
import asyncio
import socket
import pickle
import struct

def get_host():
    s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    s.connect(("8.8.8.8",80))
    host = s.getsockname()[0]
    return host

# make general network on port 3xxx

async def create(node:Server,node_port):

    await node.listen(node_port,interface="0.0.0.0")
    print(f"listening on port {node_port}")
    await asyncio.sleep(1)

# connect to network

async def connect(node:Server,node_port,peer_ip,peer_port):
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

async def send_model(ip,port,model,weighting,wait_time):
    cooldown = 2
    writer = None
    for i in range(0,wait_time,cooldown):
        try:
            _,writer = await asyncio.wait_for(asyncio.open_connection(ip,port),timeout=5)
            data = pickle.dumps(model)
            print(f"[send_model] Sending Model Data: Length {len(data)} and Weighting {weighting}")
            length = struct.pack("!I",len(data))
            enc_weighting = struct.pack("!I",weighting)
            writer.write(length+enc_weighting+data)
            await writer.drain()
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

async def recieve_model(port,connect_timeout,model_timeout):
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
            model = pickle.loads(data)
            if not model_future.done():
                model_future.set_result((model,weighting))
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
   
        
