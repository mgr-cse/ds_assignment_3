# ds_assignment_2

Distributed Queue assignment

## Code structure
```bash
./
├─ broker/ # broker module + heartbeats
│  ├─ broker.py # server for broker
├─ broker_manager/ # broker manager module
│  ├─ common/
│  │  ├─ debug.py
│  │  ├─ manager_routes.py # manager endpoints
│  │  ├─ manager_sync.py # heartbeats for heath,...
│  ├─ manager.py # broker manager entrypoint
├─ consumer/
│  ├─ consumer.py # consumer client
├─ producer/
│  ├─ producer.py # producer client
├─ queueSDK/ # sdk for making clients
├─ scripts/ # scripts for docker containers
├─ tests/ # testing scripts
```

## Prerequisites

The following instructions are tested on Ubuntu 22.04

### Install the required system packages: 
```bash
sudo apt install python3-venv python3-pip docker.io
```
### Add yourself to docker group
```bash
sudo usermod
```
### Setting up repository
```bash
git clone https://github.com/mgr-cse/ds_assignment_2
cd ds_assignment_2
python3 -m venv 01-env
source 01-env/bin/activate
pip install -r requirements.txt
```
### Get container image for cluster setup
```bash
./scripts/create_image.sh
```
## Running test

```bash
./tests/test/sh
```
    