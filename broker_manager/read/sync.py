import requests
import time
import traceback

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from __main__ import app, db, app_kill_event, sync_address
app: Flask
db: SQLAlchemy
app_kill_event: bool
sync_address: str

from broker_manager.read.db_model import *

# database syncing stuff
def commit_metadata(response):
    try:
        new_brokers = response['brokers']
        new_topics = response['topics']
        new_partitions = response['partitions']
        brokers = Broker.query.order_by(Broker.id).all()
        topics = Topic.query.order_by(Topic.id).all()
        partitions = Partition.query.order_by(Partition.id).all()

        # sync brokers
        index = 0
        for b in brokers:
            if b.id != new_brokers[index]['id']:
                print('broker syncing fucked up')
                return False
            b.health = new_brokers[index]['health']
            db.session.flush()
            index += 1
        
        # add new brokers
        for i in range(index, len(new_brokers)):
            b = Broker(
                id=new_brokers[i]['id'],
                ip=new_brokers[i]['ip'],
                port=new_brokers[i]['port'],
                health=new_brokers[i]['health']
            )
            db.session.add(b)
            db.session.flush()
        
        # sync topics
        index = 0
        for t in topics:
            if t.id != new_topics[index]['id']:
                print('broker syncing fucked up')
                return False
            index += 1
        for i in range(index, len(new_topics)):
            t = Topic(
                id=new_topics[i]['id'],
                name=new_topics[i]['name']
            )
            db.session.add(t)
            db.session.flush()
        
        # sync partitions
        index = 0
        for p in partitions:
            if p.id != new_partitions[index]['id']:
                print('partition synching fucked up')
                return False
            index += 1
        for i in range(index, len(new_partitions)):
            p = Partition(
                id=new_partitions[i]['id'],
                broker_id=new_partitions[i]['broker_id'],
                topic_id=new_partitions[i]['topic_id']
            )
            db.session.add(p)
            db.session.flush()

        db.session.commit()
        return True  
    except:
        traceback.print_exc()
        print('commit_metadata: error while adding metadata to database')
    return False

def sync_metadata():
    try:
        res = requests.get('http://' + sync_address + '/metadata')
        if res.ok:
            response = res.json()
            if response['status'] == 'success':
                # commit the response to database
                return commit_metadata(response)
        else: print('invalid response code received')
    except:
        print('can not make request for syncing metadata')
    return False

def sync_broker_messages(broker: Broker):
    offset = 0
    ip = None
    port = None
    try:
        if len(broker.messages) > 0:
            offset = broker.messages[-1].broker_message_id
        ip = broker.ip
        port = broker.port
    except:
        traceback.print_exc()
        print('error in getting broker message offset/ip/port')
        return False
    
    response = None
    try:
        res = requests.get(f'http://{ip}:{port}/retreive_messages', params={"offset":offset})
        if res.ok:
            response = res.json()
            if response['status'] != 'success':
                print('broker sync: receieved failure')
                return False
    except:
        print('broker sync: can not make a connection')
        return False

    # write messages to database
    try:
        for m in response['messages']:
            message = Message(
                topic_id=m['topic_id'],
                partition_id=m['partition_id'],
                message_content=m['message_content'],
                producer_client=m['producer_client'],
                timestamp=m['timestamp'],
                random_string=m['random_string'],
                broker_id=broker.id,
                broker_message_id=m['id']
            )
            db.session.add(message)
            db.session.flush()
        db.session.commit()
        return True
    except:
        traceback.print_exc()
        print('Broker sync: can not write messages to database')
    return False

def sync_messages_from_broker():
    brokers = Broker.query.filter_by(health=1).all()
    for b in brokers:
        sync_broker_messages(b)
    
# heartbeat function
def heartbeat_sync_broker(beat_time):
    while True:
        if app_kill_event:
            return
        with app.app_context():
            if sync_metadata():
                sync_messages_from_broker()
        time.sleep(beat_time)

# replica specific
def sync_messages():
    offset = 0
    try:
        m = Message.query.order_by(Message.id.desc()).first()
        if m != None:
            offset = m.id
    except:
        print('parsing except')
        traceback.print_exc()
        return False
    
    response = None
    try:
        res = requests.get(f'http://{sync_address}/retreive_messages', params={"offset":offset})
        if res.ok:
            response = res.json()
            if response['status'] == 'failure':
                return False
        else:
            print('invalid response code')
            return False
    except:
        print('can not make a request')
        return False
    
    try:
        messages = response['messages']
        for m in messages:
            message = Message(
                id=m['id'],
                topic_id=m['topic_id'],
                partition_id=m['partition_id'],
                message_content=m['message_content'],

                producer_client=m['producer_client'],
                timestamp=m['timestamp'],
                random_string=m['random_string'],

                broker_id=m['broker_id'],
                broker_message_id=m['broker_message_id']
            )
            
            db.session.add(message)
            db.session.flush()    
        db.session.commit()
        return True
    except:
        print('sync messages: error while commiting to database')
    return False

# heartbeat function
def heartbeat_sync_primary(beat_time):
    while True:
        if app_kill_event:
            return
        with app.app_context():
            if sync_metadata():
                sync_messages()
        time.sleep(beat_time)