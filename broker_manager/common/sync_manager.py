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

def sync_metadata():
    try:
        my_ip = self_ip_address()
        content = None
        if my_ip is not None:
            content = {
                "ip": my_ip,
                "port": 5000
            }
        res = requests.get(f'http://{sync_address}/metadata/sync', params=content)
        if res.ok:
            response = res.json()
            if response['status'] == 'success':
                # commit the response to database
                return commit_metadata(response)
            else: print(response)
        else: print('invalid response code received')
    except:
        print('can not make request for syncing metadata')
    return False

def sync_metadata_heartbeat(beat_time):
    while True:
        if app_kill_event:
            break
        with app.app_context():
            sync_metadata()
        time.sleep(beat_time)

def update_health_data(health_timeout: float):
    try:
        # update broker health
        last_point = time.time() - health_timeout
        brokers = Broker.query.filter(Broker.timestamp < last_point, Broker.health==1).all()
        for b in brokers:
            b.health = 0
        db.session.flush()

        # update consumer health
        last_point = time.time() - health_timeout
        consumers = Consumer.query.filter(Consumer.timestamp < last_point, Consumer.health==1).all()
        for c in consumers:
            c.health = 0
        db.session.flush()
        db.session.commit()

        # update producer health
        last_point = time.time() - health_timeout
        producers = Producer.query.filter(Producer.timestamp < last_point, Producer.health==1).all()
        for p in producers:
            p.health = 0
        db.session.flush()
        db.session.commit()

        # update replica health
        # update producer health
        last_point = time.time() - health_timeout
        replicas = Replica.query.filter(Replica.timestamp < last_point, Replica.health==1).all()
        for r in replicas:
            r.health = 0
        db.session.flush()
        db.session.commit()
    except:
        traceback.print_exc()
        print('update health data: database error')

def health_heartbeat(beat_time):
    while True:
        if app_kill_event:
            break
        with app.app_context():
            update_health_data(health_timeout)
        time.sleep(beat_time)

