import asyncio
from argparse import ArgumentParser
from network import socket


from kademlia_federated_learning import Kademlia_Federated_Learning

# ports
BROADCAST_PORT = 8888
KADEMLIA_PORT = 8560

A_BROADCAST_PORT = 8889
A_KADEMLIA_PORT = 8561

MODEL_TRANSFER_PORT = 449

async def main(args):
    kfl = Kademlia_Federated_Learning(args,
                                BROADCAST_PORT,KADEMLIA_PORT,
                                A_BROADCAST_PORT,A_KADEMLIA_PORT,
                                MODEL_TRANSFER_PORT)
    
    await kfl.run()

def get_host():
        s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        s.connect(("8.8.8.8",80))
        host = s.getsockname()[0]
        return host
        
if __name__ == "__main__":

    # Arguement Parser
    parser = ArgumentParser()
    parser.add_argument("-np","--nodeport",default = KADEMLIA_PORT)
    parser.add_argument("-i", "--ip", default=get_host())
    parser.add_argument("-p", "--port", default=None)
    args = parser.parse_args()

    asyncio.run(main(args))

    