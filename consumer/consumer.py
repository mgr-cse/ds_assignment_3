import sys
import os
sys.path.append(os.getcwd())
import time
from typing import List
import threading

from queueSDK.consumer import Consumer

broker_maganger_ip = '172.17.0.2'
broker_manager_port = 5000
max_messages = 30

name = sys.argv[1].strip()
topics = sys.argv[2].strip().split(',')
partitions = sys.argv[3].strip().split(',')
try:
    partitions = [int(p) for p in partitions]
except:
    print('can not parse partitions')
    exit(-1)

if len(topics) != len(partitions):
    print('partitions and topics not equal')
    exit(-1)

cons = Consumer(broker_maganger_ip, broker_manager_port, name)
for t, p in zip(topics, partitions):
    while cons.register(t, p) == -1: pass


def dequeue_logs(topic: str, partition: int, max_message):
    while True:
        message = cons.dequeue(topic, partition)
        if type(message) == str:
            print(threading.get_native_id(),':', message)
        time.sleep(1)

threads: List[threading.Thread] = []
for t, p in zip(topics, partitions):
    thread = threading.Thread(target=dequeue_logs, args=(t,p,max_messages,))
    threads.append(thread)
    thread.start()

for t in threads:
    t.join()
