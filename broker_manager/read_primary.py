from flask import Flask
from flask import request
import threading
from flask_sqlalchemy import SQLAlchemy

import socket
import requests
import time
import traceback

username = 'mattie'
password = 'password'
database = 'psqlqueue'
db_port = '5432'

app_kill_event = False
heartbeat_time = 5
broker_manager_address = '172.17.0.2:5000'

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = f"postgresql://{username}:{password}@localhost:{db_port}/{database}"

# queue database structures
db = SQLAlchemy(app)
 
class Broker(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    ip = db.Column(db.String(255))
    port = db.Column(db.Integer)
    health = db.Column(db.Integer)

    # list of partitions that broker handles
    partitions = db.relationship('Partition', backref='broker')

    # messages for a given broker in the replica
    messages = db.relationship('Message', backref='broker', order_by='Message.broker_message_id')

    def as_dict(self):
       return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    
class Consumer(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    topic_id = db.Column(db.Integer, db.ForeignKey('topic.id'))
    offset = db.Column(db.Integer, nullable=False)
    health = db.Column(db.Integer)
    # have a partition_id, just for preference
    partition_id = db.Column(db.Integer)

class Partition(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    broker_id = db.Column(db.Integer, db.ForeignKey('broker.id'))
    topic_id = db.Column(db.Integer, db.ForeignKey('topic.id'))

    def as_dict(self):
       return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class Topic(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    name = db.Column(db.String(255), nullable=False, unique=True)

    consumers  = db.relationship('Consumer', backref='topic')
    messages   = db.relationship('Message', backref='topic')
    partitions = db.relationship('Partition', backref='topic')

    def as_dict(self):
       return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class Message(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    topic_id = db.Column(db.Integer, db.ForeignKey('topic.id'))
    partition_id = db.Column(db.Integer, db.ForeignKey('partition.id'))
    message_content = db.Column(db.String(255))
    
    # producer sends some info to uniquely identify the message
    producer_client = db.Column(db.String(255))
    timestamp = db.Column(db.Float)
    random_string = db.Column(db.String(257))

    # attaching broker information
    broker_id = db.Column(db.Integer, db.ForeignKey('broker.id'))
    broker_message_id = db.Column(db.Integer)

    def as_dict(self):
       return {c.name: getattr(self, c.name) for c in self.__table__.columns}

# debugging functions
def print_thread_id():
    print('Request handled by worker thread:', threading.get_native_id())

def return_message(status:str, message=None):
    content = dict()
    content['status'] = status
    if message is not None:
        content['message'] = message
    return content

# functions for handelling each endpoint
@app.route('/topics', methods=['GET'])
def topic_get_request():
    print_thread_id()
    topics_list = []
    try:
        # database
        topics = Topic.query.all()
        for t in topics:
            topics_list.append(t.name)
        return return_message('success', topics_list)
    except: 
        return return_message('failure', 'Error while listing topics')

@app.route('/topics/partitions', methods=['GET'])
def topic_get_partitions():
    print_thread_id()
    topic_name = None
    try:
        topic_name = request.args.get('topic_name')
    except:
        return return_message('failure', 'Error while parsing request')
    
    try:
        topic = Topic.query.filter_by(name=topic_name).first()
        if topic is None:
            return return_message('failure', 'topic does not exist')
        
        partitions = []
        for p in topic.partitions:
            partitions.append(p.id)

        return {
            "status": "success",
            "partition_ids": partitions
        }
    except:
        return return_message('failure','Error while querying/commiting database')


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
    try:
        topic_name = request.args.get('topic')
        consumer_id = request.args.get('consumer_id')
        consumer_id = int(consumer_id)
    except:
        return return_message('failure', 'Error while parsing request')
    
    try:
        consumer = Consumer.query.filter_by(id=consumer_id).first()
        if consumer_id is None:
            return return_message('failure', 'consumer_id does not exist')

        if consumer.topic.name != topic_name:
            return return_message('failure', 'consumer_id and topic do not match')
        # the tuff query
        message = Message.query.filter(Message.id > consumer.offset).filter_by(topic_id=consumer.topic.id).order_by(Message.id.asc()).first()
        if message is None:
            return return_message('failure', 'no more messages')
        
        consumer.offset = message.id
        db_lock = threading.Lock()
        with db_lock:
            db.session.commit()
        
        with db_lock:
            return {
                "status": "success",
                "message": message.message_content,
                "offset": message.id
            }
    except:
        return return_message('failure','Error while querying/commiting database')
    

@app.route('/consumer/set_offset',methods=['POST'])
def consumer_set_offset():
    print_thread_id()
    content_type = request.headers.get('Content-Type')
    if content_type != 'application/json':
        return return_message('failure', 'Content-Type not supported')
        
    consumer_id = None
    offset = None
    try:
        receive = request.json
        consumer_id = receive['consumer_id']
        offset = receive['offset']
    except:
        return return_message('failure', 'Error while parsing request')
    
    try:
        consumer = Consumer.query.filter_by(id=consumer_id).first()
        if consumer_id is None:
            return return_message('failure', 'consumer_id does not exist')
        
        consumer.offset = offset
        db.session.commit()
        return return_message('success')
    except:
        return return_message('failure','Error while querying/commiting database')    

@app.route('/size',methods=['GET'])
def consumer_size():
    print_thread_id()   
    topic_name = None
    consumer_id = None
    try:
        topic_name = request.args.get('topic')
        consumer_id = request.args.get('consumer_id')
        consumer_id = int(consumer_id)
    except:
        return return_message('failure', 'Error while parsing request')
    
    try:
        consumer = Consumer.query.filter_by(id=consumer_id).first()
        if consumer_id is None:
            return return_message('failure', 'consumer_id does not exist')

        if consumer.topic.name != topic_name:
            return return_message('failure', 'consumer_id and topic do not match')
        # the tuff query
        messages = Message.query.filter(Message.id > consumer.offset).filter_by(topic_id=consumer.topic.id).count()
        return {
            "status": "success",
            "size": messages
        }
    except:
        return return_message('failure', 'Error while querying/commiting database')

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
        res = requests.get('http://' + broker_manager_address + '/metadata')
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

def sync_messages():
    brokers = Broker.query.filter_by(health=1).all()
    for b in brokers:
        sync_broker_messages(b)
    
# heartbeat function
def heartbeat(beat_time):
    while True:
        if app_kill_event:
            return
        with app.app_context():
            if sync_metadata():
                sync_messages()
        time.sleep(beat_time)

if __name__ == "__main__": 
    with app.app_context():
        db.create_all()
        # launch heartbeats
        thread = threading.Thread(target=heartbeat, args=(heartbeat_time,))
        thread.start()

    # launch request handler
    app.run(host='0.0.0.0',debug=False, threaded=True, processes=1)
    app_kill_event = True
    thread.join()