import sys
import os
sys.path.append(os.getcwd())
import time
from typing import List
import threading
import signal

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

kill_signal = False

cons = Consumer(broker_maganger_ip, broker_manager_port, name)
for t, p in zip(topics, partitions):
    while cons.register(t, p) == -1: pass


def dequeue_logs(topic: str, partition: int, max_message):
    while True:
        if kill_signal:
            break
        message = cons.dequeue(topic, partition)
        if type(message) == str:
            print_str = f'{name} {threading.get_native_id()}: {message}\n'
            print(print_str, end='',flush=True)
        time.sleep(0.2)

threads: List[threading.Thread] = []
for t, p in zip(topics, partitions):
    thread = threading.Thread(target=dequeue_logs, args=(t,p,max_messages,))
    threads.append(thread)
    thread.start()

def handler(signum, frame):
    print(f'{name} ctrl-C pressed, exiting...')
    global kill_signal
    global threads
    kill_signal = True

    for t in threads:
        t.join()
signal.signal(signal.SIGINT, handler)

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

import requests
import time
import traceback
import socket

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from __main__ import app, db, app_kill_event, sync_address, health_timeout
app: Flask
db: SQLAlchemy
app_kill_event: bool
sync_address: str

from broker_manager.common.db_model import *

# get self ip
def self_ip_address():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return None

# 
def commit_metadata(response):
    try:
        new_topics : Dict = response['topics']
        new_consumers : Dict = response['consumers']
        new_producers : Dict = response['producers']
        new_brokers : Dict = response['brokers']
        new_partitions : Dict = response['partitions']
    except:
        print('commit_database, invalid response received')
        return False

    try:
        Partition.query.delete()
        Broker.query.delete()
        Producer.query.delete()
        Consumer.query.delete()
        Topic.query.delete()

        for t in new_topics:
            topic = Topic()
            topic.from_dict(t)
            db.session.add(topic)
            db.session.flush()
        
        for t in new_producers:
            producer = Producer()
            producer.from_dict(t)
            db.session.add(producer)
            db.session.flush()

        for t in new_consumers:
            consumer = Consumer()
            consumer.from_dict(t)
            db.session.add(consumer)
            db.session.flush()
        
        for t in new_brokers:
            broker = Broker()
            broker.from_dict(t)
            db.session.add(broker)
            db.session.flush()
        
        for t in new_partitions:
            partition = Partition()
            partition.from_dict(t)
            db.session.add(partition)
            db.session.flush()
        
        db.session.commit()
    except:
        traceback.print_exc()
        print('error occured while synching data')
        return False