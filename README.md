# Kademlia-Based Federated Learning
A truly decentralised approach to Federated Learning using Kademlia networks (DHT).

## Setup

For any Jupyter Notebook, you must have the following installed in your Python environment:
- `matplotlib`
- `medmnist`
- `kademlia`
- `torch`
- `torchvision`

There is a [requirements.txt](./requirements.txt) file that you can use to install these dependencies for `Linux` machines running `CUDA 13.0` (as of May 2026).
**PyTorch** ( `torch`, `torchvision` ) installation can vary based on hardware and operating system, so please install accordingly from the [PyTorch website](https://pytorch.org/get-started/locally/).

> [!IMPORTANT]
> Please download the ChestMNIST dataset into the [data folder](./data/) in the project directory using:
> ```python
> from medmnist import ChestMNIST
> train_data = ChestMNIST(split='train',root="./data",download=True)
> ```
> **OR**
>
> Run the [download.ipynb](./download.ipynb) Jupyter Notebook

## For Results

### Loss Over Epoch

To see the loss over epochs graph, look for the [mixed_results.ipynb](./mixed_results.ipynb) file, which grabs log files from the results folders and shows them in a Jupyter Notebook.

### Test Results

To see the test results, check the following folders:

| Result                       | File                                                               |
| ---------------------------- | ------------------------------------------------------------------ |
| Centralised Learning         | [results_centralised/test.ipynb](./results_centralised/test.ipynb) |
| Simulated Federated Learning | [results_sim/test.ipynb](./results_sim/test.ipynb)                 |
| Kademlia-Based Learning      | [results/test.ipynb](./results/test.ipynb)                         |

> [!NOTE]
> If you are re-running the Jupyter Notebooks, it reproduces the results on execution, which takes a long time due to calculating validation accuracies and test accuracies to display them.

### Podman Log Results

To see the calculations that proved O(LogN) communication overhead per node for kademlia-based federated learning look for the [podman_results.ipynb](./podman-logs/podman_results.ipynb) file.

## For Running Code

### Kademlia-Based Federated Learning

1. Run the service named `img-builder` from [base-docker-compose.yml](./base-docker-compose.yml) using Podman, which creates the image that all nodes run on. You can build the image with:
```bash
podman compose -f 'base-docker-compose.yml' up -d --build 'img-builder'
```

2. Execute [compose.yml](./compose.yml), which will spin up 10 nodes by using:
```bash
podman compose -f 'compose.yml' up -d --build
```

> [!NOTE]
> - These `*.yml` files may work with Docker Compose, however I have not tested if they do.
> - Ensure that you are able to handle running all of these containers on your system otherwise the program might fail.
> I used a system with the following:
>   - AMD Ryzen 5600x
>   - 8GB of VRAM (NVIDIA RTX 3060TI)
>   - 32GB of RAM


### Simulated Federated Learning

Run [standard_federated_learning_sim.ipynb](./standard_federated_learning_sim.ipynb), which trains 10 simulated nodes that can be compared to the Kademlia-Based Federated Learning results.

> [!IMPORTANT]
> Check Setup section for the Jupyter Notebooks to work

### Centralised Learning Code

Run [centralised_train.ipynb](./centralised_train.ipynb), which will run centralised training using 320 batches instead of 32 to effectively be equivalent to both the Simulated and Kademlia-Based Federated Learning per epoch (where they would use 10 nodes in 32 batches).

> [!IMPORTANT]
> Check Setup section for the Jupyter Notebooks to work