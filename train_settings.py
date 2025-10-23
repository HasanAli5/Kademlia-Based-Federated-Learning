from model import Model
import torch

class Train_Settings():
    device = torch.accelerator.current_accelerator().type if torch.accelerator.is_available() else "CPU"
    model = Model.to(device)

    loss = torch.nn.CrossEntropyLoss()

    learning_rate = 1e-3
    decay_rate = 3e-3
    optimiser = torch.optim.Adam(params=model.parameters,lr=learning_rate,weight_decay=decay_rate)

    def __str__(self):
        print(f"Device : {self.device}\n"+
            f"Model : \n{self.model}\n"+
            f"Loss : {self.loss}\n"+
            f"Optimizer : {self.optimizer}\n"+
            f"  |=> Learning Rate : {self.learning_rate}\n"+
            f"  |=> Decay : {self.decay_rate}\n")
    
    def export(self):
        return (
            self.device,
            self.model,
            self.loss,
            self.optimiser
            )