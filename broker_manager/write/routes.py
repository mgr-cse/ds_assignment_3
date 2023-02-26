import random
import threading
import time
import requests
import traceback

from flask import Flask, Request
from __main__ import app, max_tries, try_timeout, request
from broker_manager.common.routes import *
from broker_manager.write.db_model import *

app: Flask
request: Request

# functions for handelling each endpoint
# producer specific endpoints
@app.route('/topics', methods=['POST'])
def topic_register_request():
    print_thread_id()
    content_type = request.headers.get('Content-Type')
    if content_type != 'application/json':
        return return_message('failure', 'Content-Type not supported')

    # parse content
    topic_name = None
    try:
        receive = request.json
        topic_name = receive['topic_name']
    except:
        return return_message('failure', 'Error While Parsing json')
    
    # database
    try:
        if Topic.query.filter_by(name=topic_name).first() is not None:
            return return_message('failure', 'Topic already exists')  
        
        topic = Topic(name=topic_name)
        db.session.add(topic)
        db.session.flush()

        # create a partition in each broker
        brokers = Broker.query.filter_by(health=1).all()
        for b in brokers:
            partition = Partition(topic_id=topic.id, broker_id=b.id)
            db.session.add(partition)
        
        # commit transaction
        db.session.commit()
        return return_message('success', 'topic ' + topic.name + ' created sucessfully')
    except:
        return return_message('failure', 'Error while querying/comitting to database')

@app.route('/brokers/heartbeat',methods=['POST'])
def broker_heartbeat():
    print_thread_id()
    content_type = request.headers.get('Content-Type')
    if content_type != 'application/json':
        return return_message('failure', 'Content-Type not supported')
    
    ip = None
    port = None
    try:
        receive = request.json
        ip = receive['ip']
        port = receive['port']
    except:
        return return_message('failure', 'Error While Parsing json')
    
    # find if broker already exists
    try:
        broker = Broker.query.filter_by(ip=ip, port=port).first()
        if broker is not None:
            broker.health = 1
            db.session.flush()
            db.session.commit()
            return return_message('success', 'broker heartbeat received')
    
        broker = Broker(ip=ip, port=port, health=1)
        db.session.add(broker)
        db.session.flush()
        db.session.commit()
        return return_message('success', 'new broker registered')
    except:
        return return_message('failure', 'Error while querying/comitting to database')

@app.route('/producer/register',methods=['POST'])
def producer_register_request():
    print_thread_id()
    content_type = request.headers.get('Content-Type')
    if content_type != 'application/json':
        return return_message('failure', 'Content-Type not supported')
    
    # parsing
    topic_name = None
    partition_id = None
    try:
        receive = request.json
        topic_name = receive['topic']
        if 'partition_id' in receive:
            partition_id = receive['partition_id']
    except:
        return return_message('failure', 'Error while parsing request')
        
    # query
    try:
        topic = Topic.query.filter_by(name=topic_name).first()
        if topic is None:
            return return_message('failure', 'Topic does not exist')

        if  partition_id is None:
            producer = Producer(topic_id=topic.id, partition_id=-1, health=1)
        else:
            # find if partition exists
            partition = Partition.query.filter_by(id=partition_id).first()
            if partition is None:
                return return_message('failure', 'Partition does not exist')
            
            producer = Producer(topic_id=topic.id, partition_id=partition_id, health=1)

        db.session.add(producer)
        db.session.flush()
        db.session.commit()
        return {
            "status": "success",
            "producer_id": producer.id
        }
    except:
        return return_message('failure','Error while querying/commiting database')

@app.route('/producer/produce',methods=['POST'])
def producer_enqueue():
    print_thread_id()
    content_type = request.headers.get('Content-Type')
    if content_type != 'application/json':
        return return_message('failure', 'Content-Type not supported')
        
    topic_name = None
    producer_id = None
    message_content = None

    prod_client = None
    timestamp = None
    random_string = None
    try:
        receive = request.json
        topic_name = receive['topic']
        producer_id = receive['producer_id']
        message_content = receive['message']
        prod_client = receive['prod_client']
        timestamp = receive['timestamp']
        random_string = receive['random_string']
    except:
        return return_message('failure', 'Error while parsing request')
    
    try:
        producer = Producer.query.filter_by(id=producer_id).first()
        if producer is None:
            return return_message('failure', 'producer_id does not exist')
        
        if producer.topic.name != topic_name:
            return return_message('failure', 'producer_id and topic do not match')
        
        if producer.partition_id == -1:
            # produce to any random partition, with good health
            partitions = producer.topic.partitions
            good_parts = []
            for p in partitions:
                if p.broker.health == 1:
                    good_parts.append(p)
            partitions = good_parts
            for _ in range(max_tries):
                if len(partitions) < 1: continue
                random_choice = random.randint(0, len(partitions)-1)
                partition = partitions[random_choice]
                ip = partition.broker.ip
                port = partition.broker.port

                request_content = {
                    "topic_id": producer.topic.id,
                    "partition_id": partition.id,
                    "message_content": message_content,
                    "producer_client": prod_client,
                    "timestamp": timestamp,
                    "random_string": random_string
                }
                try:
                    res = requests.post('http://' + ip + ":" + str(port) + '/store_message', json=request_content)
                    if res.ok:
                        response = res.json()
                        print(response)
                        if response['status'] == 'success':
                            db.session.commit()
                            return return_message('success')
                except:
                    print('exception occured in parsing response/ can not connect')
                partition.broker.health = 0
                # write ahead
                db.session.flush()
                time.sleep(try_timeout)

        else:
            # produce to that specific partition
            partition = Partition.query.filter_by(id=producer.partition_id).first()
            ip = partition.broker.ip
            port = partition.broker.port

            request_content = {
                "topic_id": producer.topic.id,
                "partition_id": partition.id,
                "message_content": message_content,
                "producer_client": prod_client,
                "timestamp": timestamp,
                "random_string": random_string
            }
            try:
                res = requests.post('http://' + ip + ":" + str(port) + '/store_message', json=request_content)
                if res.ok:
                    response = res.json()
                    if response['status'] == 'success':
                        db.session.commit()
                        return return_message('success')
            except:
                print('exception occured in parsing response/ can not connect')
            partition.broker.health = 0
            db.session.flush()
        
        db.session.flush()
        db.session.commit()
        return return_message('failure', 'can not commit to a broker')
            
    except:
        traceback.print_exc()
        return return_message('failure','Error while querying/commiting database')

@app.route('/metadata', methods=['GET'])
def get_metadata():
    print_thread_id()
    try:
        brokers = Broker.query.order_by(Broker.id).all()
        brokers = [ b.as_dict() for b in brokers ]
        topics = Topic.query.order_by(Topic.id).all()
        topics = [ t.as_dict() for t in topics ]
        partitions = Partition.query.order_by(Partition.id).all()
        partitions = [ p.as_dict() for p in partitions ]

        return {
            "status": "success",
            "brokers": brokers,
            "topics": topics,
            "partitions": partitions
        }
    except:
        traceback.print_exc()
        return return_message('failure', 'error in querying database')
