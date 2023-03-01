import sys
import os
sys.path.append(os.getcwd())

from queueSDK.producer import Producer
import random
import string
import time
import threading
from typing import List

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

prod = Producer(broker_maganger_ip, broker_manager_port, name)
for t, p in zip(topics, partitions):
    while prod.register(t, p) == -1: pass


def enqueue_logs(topic: str, partition: int, max_message):
    for i in range(max_message):
        random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=256))
        message = f'{name} debug message {i}: topic {topic}, partition {partition}'
        
        while not prod.enqueue(topic, message, time.time(), random_string, partition):
            time.sleep(1)
        print_str = f'message enqueued! {message}\n'
        print(print_str, end='', flush=True)
        time.sleep(1)

threads: List[threading.Thread] = []
for t, p in zip(topics, partitions):
    thread = threading.Thread(target=enqueue_logs, args=(t,p,max_messages,))
    threads.append(thread)
    thread.start()

for t in threads:
    t.join()