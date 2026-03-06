import torch.utils.data as pydata
import numpy
from medmnist import ChestMNIST
from torchvision.transforms import v2
import torch

import torch
from torch.utils.data import Dataset

class ClientDataset(Dataset):
    def __init__(self, data_source, transform=None):
        self.data = data_source
        self.transform = transform

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        item = self.data[idx]

        if self.transform:
            item = self.transform(item)

        return item

# this seperates the dataset by the node
def clientsplit():

    tf = v2.Compose([
        v2.ToImage(),
        v2.ToDtype(torch.float32,scale=True)
    ])

    train_data = ChestMNIST(split='train',transform=tf,download=True,root="./data")
    val_data = ChestMNIST(split='val',transform=tf,download=True,root="./data")
    test_data = ChestMNIST(split='test',transform=tf,download=True,root="./data")

    clients = 10
    # repeat for 10 clients (split the dataset into 10 parts)


    for i in range(clients):
        indexes = []
        for j in range(len(train_data)):
            if i+j % clients == 0:
                indexes.append(i+j)
        client_dataset = pydata.Subset(train_data,indices=indexes)