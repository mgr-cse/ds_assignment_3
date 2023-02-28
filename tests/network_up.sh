#!/bin/bash

HOST=172.17.0.2
PORT=5000

# launch primary
./scripts/launch_container_presist.sh w1 ./broker_manager/manager.py write

# launch brokers
./scripts/launch_container_presist.sh b1 ./broker/broker.py
./scripts/launch_container_presist.sh b2 ./broker/broker.py
./scripts/launch_container_presist.sh b3 ./broker/broker.py
sleep 2

# launch replicas
./scripts/launch_container_presist.sh r1 ./broker_manager/manager.py read
./scripts/launch_container_presist.sh r2 ./broker_manager/manager.py read

# create topics and partitions
curl -XPOST "http://${HOST}:${PORT}/topics" -d '{"topic_name": "T-1"}' -H "Content-Type: application/json"
curl -XPOST "http://${HOST}:${PORT}/topics" -d '{"topic_name": "T-2"}' -H "Content-Type: application/json"
curl -XPOST "http://${HOST}:${PORT}/topics" -d '{"topic_name": "T-3"}' -H "Content-Type: application/json"

curl -XPOST "http://${HOST}:${PORT}/brokers/create/partition" -d '{"topic": "T-1", "broker_id": 1}' -H "Content-Type: application/json"
sleep 1
curl -XPOST "http://${HOST}:${PORT}/brokers/create/partition" -d '{"topic": "T-2", "broker_id": 2}' -H "Content-Type: application/json"
sleep 1
curl -XPOST "http://${HOST}:${PORT}/brokers/create/partition" -d '{"topic": "T-3", "broker_id": 3}' -H "Content-Type: application/json"

echo "++++ topics, partitions, machines up!, go launch some producers and consumers"