
from fileinput import filename
import json
from pathlib import Path
import time

import torch
import os

# setting for the model training.
class Model_Manager():

    def __init__(self,model:torch.nn.Module,
                 learning_rate:float = 1e-3,
                 decay_rate:float = 0):
        self.device = torch.accelerator.current_accelerator().type if torch.accelerator.is_available() else "CPU" # type: ignore
        # model is not used for training just parameters
        self.model = model.to(self.device)
        self.loss = torch.nn.BCEWithLogitsLoss()
        self.learning_rate = learning_rate
        self.decay_rate = decay_rate
        self.optimiser = torch.optim.Adam(params=self.model.parameters(),lr=self.learning_rate,weight_decay=self.decay_rate)
        self.logs = [[[],[]],[[],[]]]

    def __str__(self):
        return (f"Device : {self.device}\n"+
            f"Model : \n{self.model}\n"+
            f"Loss : {self.loss}\n"+
            f"Optimizer : {self.optimiser}\n"+
            f"Learning Rate : {self.learning_rate}\n"+
            f"Decay : {self.decay_rate}\n")
    
    def get_config(self):
        train = (self.loss,self.device,self.optimiser)
        val = (self.loss,self.device)
        return train,val
    
    def save_logs(self):
        peer_id = os.getenv("PEER_ID")

        folder_name = f"node_{peer_id}" if peer_id else "node_pile"

        folder_path = Path("./results") / folder_name
        folder_path.mkdir(parents=True,exist_ok=True)

        timestamp = time.time()
        file_name = f"log_{timestamp}.txt"
        filepath = folder_path / file_name
        try:
            f = open(filepath,"w")
            f.write(json.dumps(self.logs,indent=4))
            f.close()
        except Exception as e:
            print(f"[save_logs] Exception : {e}")
