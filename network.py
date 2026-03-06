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
    await asyncio.sleep(5)
    await node.listen(node_port,interface="0.0.0.0")
    print(f"listening on port {node_port}")
    bootstrap_node = (peer_ip, int(peer_port))
    print(f"trying to connect to {peer_ip}:{peer_port}")
    await node.bootstrap([bootstrap_node])
    await asyncio.sleep(1)

# make general merger network

# send file to other pc using grcp

# recieve file

# fedavg