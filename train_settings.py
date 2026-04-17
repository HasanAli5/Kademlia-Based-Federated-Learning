from model import ResNet18
import torch

# setting for the resnet training.
class Train_Settings():

    def __init__(self,model:ResNet18,learning_rate = 0.001, decay_rate = 0):
        self.device = torch.accelerator.current_accelerator().type if torch.accelerator.is_available() else "CPU"
        # model is not used for training just parameters
        self.model = model.to(self.device)
        self.loss = torch.nn.BCEWithLogitsLoss()
        self.learning_rate = learning_rate
        self.decay_rate = decay_rate
        self.optimiser = torch.optim.Adam(params=self.model.parameters(),lr=self.learning_rate,weight_decay=self.decay_rate)

    def __str__(self):
        return (f"Device : {self.device}\n"+
            f"Model : \n{self.model}\n"+
            f"Loss : {self.loss}\n"+
            f"Optimizer : {self.optimiser}\n"+
            f"Learning Rate : {self.learning_rate}\n"+
            f"Decay : {self.decay_rate}\n")
    
    def train_export(self):
        return (
            self.loss,
            self.device,
            self.optimiser
        )
    
    def test_export(self):
        return (
            self.loss,
            self.device
        )