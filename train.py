from model import Model
import torch
from train_settings import Train_Settings

device,model,loss,optimiser = Train_Settings.export()

def train(dataloader):
    