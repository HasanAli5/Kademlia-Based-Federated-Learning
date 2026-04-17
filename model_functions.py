import torch

# if the training_setting.py classes types are changed then these should be changed as well.
from torch import sigmoid

def correct_batch(pred,labels):
    total = 0.0
    for i in range(len(pred)):

        pred_abs = sigmoid(pred[i])
        for j in range(len(pred_abs)):
            if pred_abs[j]>0.5:
                pred_abs[j]=1
            else:
                pred_abs[j]=0

        correct = 0
        sumation = (pred_abs==labels[i])
        for j in range(len(sumation)):
            matched = sumation[j].item()
            if matched == True:
                correct +=1
        total += correct/len(sumation)
    return total

def train(dataloader, model, loss_fn, device, optimiser):
    size = len(dataloader.dataset)
    num_batches = len(dataloader)
    model.train()
    train_loss,correct = 0, 0
    for batch, (inputs, labels) in enumerate(dataloader):
        inputs, labels = inputs.to(device), labels.to(device)
        # Compute prediction error
        pred = model(inputs)
        loss = loss_fn(pred, labels.float())
        # Backpropagation
        loss.backward()
        optimiser.step()
        optimiser.zero_grad()
        train_loss += loss.item()
        correct += correct_batch(pred,labels.float())
        if batch % 25 == 0:
            loss, current = loss.item(), (batch + 1) * len(inputs)
            print(f"Loss: {loss:>7f}  [{current:>5d}/{size:>5d}]")
    train_loss /= num_batches
    correct /= size
    print(f"Train Error: Accuracy: {(100*correct):>0.1f}%, Average Loss: {train_loss:>8f}")
    return train_loss,correct

def test(dataloader, model, loss_fn, device):
    size = len(dataloader.dataset)
    num_batches = len(dataloader)
    model.eval()
    test_loss, correct = 0, 0
    with torch.no_grad():
        for _,(inputs, labels) in enumerate(dataloader):
            inputs, labels = inputs.to(device), labels.to(device)
            pred = model(inputs)
            test_loss += loss_fn(pred, labels.float()).item()
            correct += correct_batch(pred,labels.float())
    test_loss /= num_batches
    correct /= size
    print(f"Test Error: Accuracy: {(100*correct):>0.1f}%, Average Loss: {test_loss:>8f}")
    return test_loss,correct

def train_val_loop(model,settings,dataloaders,n,logs=[[[],[]],[[],[]]]):
    # split the dataloader tuple
    train_data, val_data = dataloaders
    # get the configs for the training and testing
    train_config,test_config = settings
    for i in range(n):
        print(f"---| Epoch : {i+1} |---")
        train_loss,train_acc = train(train_data, model,*train_config)
        logs[0][0].append(train_loss)
        logs[0][1].append(train_acc)
        val_loss,val_acc = test(val_data, model,*test_config)
        logs[1][0].append(val_loss)
        logs[1][1].append(val_acc)
    return logs