import kademlia
import asyncio
import logging
import grpc
import socket
from kademlia.network import Server

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

# make general merger network

# send file to other pc using grcp

# recieve file

# fedavg