import random
import threading
import time
import requests
import traceback

from flask import Flask, Request
from __main__ import app, request

from broker_manager.common.routes import *
from broker_manager.read.db_model import *

app: Flask
request: Request

@app.route('/consumer/register', methods=['POST'])
def consumer_register_request():
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

        consumer = None
        if  partition_id is None:
            consumer = Consumer(topic_id=topic.id, partition_id=-1, health=1, offset=-1)
        else:
            # find if partition exists
            partition = Partition.query.filter_by(id=partition_id).first()
            if partition is None:
                return return_message('failure', 'Partition does not exist')
            
            consumer = Consumer(topic_id=topic.id, partition_id=partition_id, health=1, offset=-1)

        db.session.add(consumer)
        db.session.flush()
        db.session.commit()
        return {
            "status": "success",
            "producer_id": consumer.id
        }
    except:
        return return_message('failure','Error while querying/commiting database')

@app.route('/consumer/consume',methods=['GET'])
def consumer_dequeue():
    print_thread_id()   
    topic_name = None
    consumer_id = None
    offset = None
    consumer = None
    try:
        topic_name = request.args.get('topic')
        consumer_id = request.args.get('consumer_id')
        consumer_id = int(consumer_id)
        offset = request.args.get('offset', 'not_given')
        
        consumer = Consumer.query.filter_by(id=consumer_id).first()
        if consumer is None:
             return return_message('failure', 'consumer_id does not exist')
        if offset == 'not_given':
            offset = consumer.offset
        else: offset = int(offset)
    except:
        return return_message('failure', 'Error while parsing request/quering offset')
    
    try:
        if consumer.topic.name != topic_name:
            return return_message('failure', 'consumer_id and topic do not match')
        # the tuff query
        message = None
        if consumer.partition_id == -1:
            message = Message.query.filter(Message.id > offset).filter_by(topic_id=consumer.topic.id).order_by(Message.id.asc()).first()
        else:
            message = Message.query.filter(Message.id > offset).filter_by(topic_id=consumer.topic.id, partition_id=consumer.partition.id).order_by(Message.id.asc()).first()
        
        if message is None:
            return return_message('failure', 'no more messages')
        
        consumer.offset = message.id
        db.flush()
        db.session.commit()

        return {
            "status": "success",
            "message": message.message_content,
            "offset": message.id
        }
    except:
        return return_message('failure','Error while querying/commiting database')

# syncing to replicas
@app.route('/retreive_messages', methods=['GET'])
def retreive_messages():
    offset = 0
    try:
        offset = request.args.get('offset')
        offset = int(offset)
    except:
        return return_message('failure', 'can not parse offset')
    
    try:
        messages = Message.query.filter(Message.id > offset).order_by(Message.id).all()
        messages = [ m.as_dict() for m in messages ]
        return {
            "status": "success",
            "messages": messages
        }
    except:
        return return_message('failure', 'error while querying database')

# syncing to replicas
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
    

