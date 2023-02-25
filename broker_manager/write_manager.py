from flask import Flask
from flask import request
from flask_sqlalchemy import SQLAlchemy

import random
import threading
import time
import requests

username = 'mattie'
password = 'password'
database = 'psqlqueue'
db_port = '5432'

max_tries = 3
try_timeout = 2

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = f"postgresql://{username}:{password}@localhost:{db_port}/{database}"

# queue database structures
db = SQLAlchemy(app)
 
class Producer(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    topic_id = db.Column(db.Integer, db.ForeignKey('topic.id'))
    # have a partition_id, just for preference
    partition_id = db.Column(db.Integer)
    health = db.Column(db.Integer)

class Broker(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    ip = db.Column(db.String(255))
    port = db.Column(db.Integer)
    health = db.Column(db.Integer)

    # list of partitions that broker handles
    partitions = db.relationship('Partition', backref='broker')


class Partition(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    broker_id = db.Column(db.Integer, db.ForeignKey('broker.id'))
    topic_id = db.Column(db.Integer, db.ForeignKey('topic.id'))


class Topic(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    name = db.Column(db.String(255), nullable=False, unique=True)
    
    producers  = db.relationship('Producer', backref='topic')
    partitions = db.relationship('Partition', backref='topic')


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
        brokers = Broker.query.filter_by(health=1)
        for b in brokers:
            partition = Partition(topic_id=topic.id, broker_id=b.id)
            db.session.add(partition)
        
        # commit transaction
        db.session.commit()
        return return_message('success', 'topic ' + topic.name + ' created sucessfully')
    except:
        return return_message('failure', 'Error while querying/comitting to database')

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

@app.route('/broker/heartbeat',methods=['POST'])
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
            # produce to any random partition
            partitions = producer.topic.partitions
            for _ in range(max_tries):
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
                res = requests.post('http://' + ip + ":" + str(port) + '/store_message', json=request_content)
                if res.ok:
                    try:
                        response = res.json()
                        print(response)
                        if response['status'] == 'success':
                            db.session.commit()
                            return return_message('success')
                    except:
                        print('exception occured in parsing response')
                time.sleep(try_timeout)
                partition.broker.health = 0
                # write ahead
                db.session.flush()

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
            res = requests.post('http://' + ip + ":" + str(port) + '/store_message', json=request_content)
            if res.ok:
                try:
                    response = res.json()
                    if response['status'] == 'success':
                        db.session.commit()
                        return return_message('success')
                except:
                    pass
            partition.broker.health = 0
        
        db.session.flush()
        db.session.commit()
        return return_message('failure', 'can not commit to a broker')
            
    except:
        return return_message('failure','Error while querying/commiting database')

if __name__ == "__main__": 
    # 
    with app.app_context():
        db.create_all()

    # launch request handler
    app.run(host='0.0.0.0',debug=False, threaded=True, processes=1)