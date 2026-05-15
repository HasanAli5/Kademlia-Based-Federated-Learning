
from medmnist import ChestMNIST
from torchvision.transforms import v2
import torch
import os
import random
import torch
from torch.utils.data import Dataset,Subset
from torch.utils.data.dataloader import DataLoader

# setting for the model training.
class Data_Manager():

    def __init__(self,test:bool=False,peers=None,peer_id=None,random_seed=None,centralised=False):
        tf = v2.Compose([
            v2.ToImage(),
            v2.ToDtype(torch.float32,scale=True)
        ])
        self.train_data = ChestMNIST(split='train',transform=tf,root="./data")
        self.val_data = ChestMNIST(split='val',transform=tf,root="./data")
        self.train_subset = None
        self.val_subset = None
        if centralised:
            # the batch per epoch should be 10 times to account for 10 peers
            self.batch_size = 320
        else:
            self.batch_size = 32
        self.test = test
        self.centralised = centralised
        if not centralised:
            self.peer_split(peers,peer_id,random_seed)
    
    def set_t_subset(self,indices:list):
        self.train_subset = Subset(self.train_data,indices)

    def set_v_subset(self,indices:list):
        self.val_subset = Subset(self.val_data,indices)

    def data_split(self,indices:list,peers:int,peer_id:int,test:bool=False):
        split_size = len(indices)//peers
        start = peer_id * split_size
        if test:
            # only take like X samples for testing purposes
            end = start + 4096
        else:
            if peers - 1 == peer_id:
                # last
                end = len(indices)
            else:
                end = (peer_id + 1) * split_size
        return indices[start:end]

    def peer_split(self,peers=None,peer_id=None,random_seed=None):
        if peers is None:
            peers = os.getenv("PEERS")
        if peer_id is None:
            peer_id = os.getenv("PEER_ID")
        if random_seed is None:
            random_seed = os.getenv("SEEDED_RANDOM_DATASET_SPLIT")

        if peers and peer_id and random_seed:
            train_len = len(self.train_data)
            val_len = len(self.val_data)
            train_indicies = list(range(train_len))
            val_indicies = list(range(val_len))

            rand = random.Random(random_seed)
            rand.shuffle(train_indicies)
            rand.shuffle(val_indicies)

            self.set_t_subset(self.data_split(train_indicies,int(peers),int(peer_id),self.test))
            self.set_v_subset(self.data_split(val_indicies,int(peers),int(peer_id),self.test))
            
    def get_dataloaders(self):
        tdata = self.train_subset if self.train_subset else self.train_data
        vdata = self.val_subset if self.val_subset else self.val_data
        train_dl = DataLoader(tdata,batch_size=self.batch_size,shuffle=True)
        val_dl = DataLoader(vdata,batch_size=self.batch_size,shuffle=True)
        return train_dl,val_dl


