import json
from pathlib import Path
import time
import torch
import torch
import os

# setting for the model training.
class Model_Manager():

    def __init__(self,model:torch.nn.Module,
                 centralised=False):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        # model is not used for training just parameters
        self.model = model
        self.loss = torch.nn.BCEWithLogitsLoss()
        self.learning_rate = 5e-6
        self.best_val_acc = 0.0
        self.optimiser = torch.optim.Adam(params=self.model.parameters(),lr=self.learning_rate)
        self.logs = [[[],[]],[[],[]]]
        self.centralised = centralised

    def __str__(self):
        return (f"Device : {self.device}\n"+
            f"Model : \n{self.model}\n"+
            f"Loss : {self.loss}\n"+
            f"Optimizer : {self.optimiser}\n"+
            f"Learning Rate : {self.learning_rate}\n")
    
    def get_config(self):
        train = (self.loss,self.device,self.optimiser)
        val = (self.loss,self.device)
        return train,val
    
    def save_logs(self):

        if self.centralised:
            folder_name = f"results_centralised"

            folder_path = Path("./") / folder_name

        else:
            peer_id = os.getenv("PEER_ID")
            folder_name = f"node_{peer_id}" if peer_id else "node_pile"

            folder_path = Path("./results") / folder_name

        while True:
            try:
                folder_path.mkdir(parents=True,exist_ok=True)

                timestamp = time.time()
                file_name = f"log_{timestamp}.txt"
                filepath = folder_path / file_name
                try:
                    f = open(filepath,"w")
                    f.write(json.dumps(self.logs,indent=4))
                    f.close()
                    return True
                except Exception as e:
                    print(f"[save_logs] Exception : {e}")
                    return False
            except Exception as e:
                print(f"[save_logs] Folder Lock (Exception : {e})")
                time.sleep(2)

    def save_global_model(self,model:torch.nn.Module):

        folder_name = f"global_model"

        folder_path = Path("./results") / folder_name

        while True:
            try:
                folder_path.mkdir(parents=True,exist_ok=True)

                timestamp = time.time()
                file_name = f"global_model_{timestamp}.pth"
                filepath = folder_path / file_name
                try:
                    torch.save(model.state_dict(),filepath)
                    return True
                except Exception as e:
                    print(f"[save_logs] Exception : {e}")
                    return False
            except Exception as e:
                print(f"[save_logs] Folder Lock (Exception : {e})")
                time.sleep(2)

    def save_best_local_model(self,model:torch.nn.Module,val_acc):
        
        if val_acc <= self.best_val_acc:
            return False
        
        self.best_val_acc = val_acc
        if self.centralised:
            folder_name = f"results_centralised"
            folder_path = Path("./") / folder_name
        else:
            peer_id = os.getenv("PEER_ID")
            folder_name = f"node_{peer_id}" if peer_id else "node_pile"
            folder_path = Path("./results") / folder_name
        
        while True:
            print("[save_best_local_model] saving best local model")
            try:
                folder_path.mkdir(parents=True,exist_ok=True)
                file_name = f"best_model.pth"
                filepath = folder_path / file_name
                try:
                    torch.save(model.state_dict(),filepath)
                    return True
                except Exception as e:
                    print(f"[save_logs] Exception : {e}")
                    return False
            except Exception as e:
                print(f"[save_logs] Folder Lock (Exception : {e})")
                time.sleep(2)
