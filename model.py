import torch

class Model(torch.nn.Module):
    def __init__(self):
        super(Model,self).__init__()

        self.layer_1 = torch.nn.Sequential(
            torch.nn.Conv2d(1,16,5),
            torch.nn.ReLU(),
            torch.nn.BatchNorm2d(16)
        )

        self.layer_2 = torch.nn.Sequential(
            torch.nn.Conv2d(16,32,5),
            torch.nn.MaxPool2d(2,2),
            torch.nn.ReLU()
        )
        
        self.layer_fc = torch.nn.Sequential(
            torch.nn.Flatten(),
            torch.nn.Linear(32*5*5,256),
            torch.nn.Linear(256,64),
            torch.nn.Linear(64,16),
            torch.nn.Softmax()
        )

        

    def forward(self,x):
        x = self.layer_1(x)
        x = self.layer_2(x)
        x = self.layer_fc(x)
        return x