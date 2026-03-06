from torch import nn
import torch

class ResidualBlock(nn.Module):

    def __init__(self,in_channels,out_channels,downsample):
        super().__init__()
        if downsample!=None:
            #stride of two
            self.conv1 = nn.Conv2d(in_channels,out_channels,kernel_size=3,stride=2,padding=1)
        else:
            self.conv1 = nn.Conv2d(in_channels,out_channels,kernel_size=3,stride=1,padding=1)
        self.bn1 = nn.BatchNorm2d(num_features=out_channels)
        self.conv2 = nn.Conv2d(out_channels,out_channels,kernel_size=3,stride=1,padding=1)
        self.bn2 = nn.BatchNorm2d(num_features=out_channels)
        self.relu = nn.ReLU()
        self.downsample = downsample

    def forward(self,x):
        identity = x

        x = self.conv1(x)
        x = self.bn1(x)
        x = self.relu(x)

        x = self.conv2(x)
        x = self.bn2(x)

        if self.downsample is not None:
            x += self.downsample(identity)
        else:
            x += identity
        
        x = self.relu(x)
        return x

class ResNet18(nn.Module):

    def make_layers(self,blocks,in_channels,out_channels):
        downsample = None
        layers = []

        if in_channels!=out_channels:
            # will double features if mismatched
            downsample = nn.Sequential(
                nn.Conv2d(in_channels,out_channels,1,2,0,bias=False),
                nn.BatchNorm2d(out_channels)
            )
        
        for i in range(blocks):
            if i == 0:
                #first block of the blocks
                layers.append(ResidualBlock(in_channels,out_channels,downsample))
            else:
                #later blocks
                layers.append(ResidualBlock(out_channels,out_channels,None))
        return nn.Sequential(*layers)

    def __init__(self,channels,classes):
        super().__init__()
        
        self.conv1 = nn.Conv2d(in_channels=channels,out_channels=64,kernel_size=7,stride=2,padding=3)
        self.bn1 = nn.BatchNorm2d(num_features=64)
        self.relu = nn.ReLU()
        self.maxpool = nn.MaxPool2d(kernel_size=3,stride=2,padding=1)

        self.layers64 = self.make_layers(blocks=2,in_channels=64,out_channels=64)
        self.layers128 = self.make_layers(blocks=2,in_channels=64,out_channels=128)
        self.layers256 = self.make_layers(blocks=2,in_channels=128,out_channels=256)
        self.layers512 = self.make_layers(blocks=2,in_channels=256,out_channels=512)

        #self.avgpool = nn.AvgPool2d(kernel_size=3,stride=1,padding=1)
        self.avgpool = nn.AdaptiveAvgPool2d((1,1))
        self.flatten = nn.Flatten()
        self.fc = nn.Linear(in_features=512,out_features=classes)

    def forward(self,x):
        x = self.conv1(x)
        x = self.bn1(x)
        x = self.relu(x)
        x = self.maxpool(x)
        x = self.layers64(x)
        x = self.layers128(x)
        x = self.layers256(x)
        x = self.layers512(x)
        x = self.avgpool(x)
        ##x = self.flatten(x)
        x = torch.flatten(x, 1)
        x = self.fc(x)
        return x