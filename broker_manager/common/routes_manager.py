import time
import requests
import traceback
import random
import threading

from flask import Flask, Request, redirect
from __main__ import app, request, sync_address, primary, max_tries, db_lock
app: Flask
request: Request
sync_address: str
primary: bool
db_lock: threading.Lock

from broker_manager.common.debug import *
from broker_manager.common.db_model import *

# get info routes, common for read/write
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
            part_info = {
                "id": p.id,
                "broker_ip": p.broker.ip,
                "broker_port": p.broker.port,
                "info": "created partition"
            }
            partitions.append(part_info)
        
        brokers = Broker.query.filter_by(health=1).all()
        for b in brokers:
            part_info = {
                "id": -1,
                "broker_ip": b.ip,
                "broker_port": b.port,
                "info": "implicit healthy broker partition",
            }
            partitions.append(part_info)

        return {
            "status": "success",
            "partitions": partitions
        }
    except:
        return return_message('failure','Error while querying/commiting database')

@app.route('/brokers', methods=['GET'])
def broker_get_request():
    print_thread_id()
    broker_list = []
    try:
        # database
        brokers = Broker.query.filter_by(health=1).all()
        for b in brokers:
            broker_list.append(b.to_dict())
        return {
            "status": "success",
            "brokers": broker_list
        }
    except: 
        return return_message('failure', 'Error while listing topics')

# broker cluster management
@app.route('/brokers/remove', methods=['POST'])
def remove_broker():
    print_thread_id()
    # updates metadata, redirect to primary
    if not primary:
        return redirect(f'http://{sync_address}/brokers/remove', code=307)
    
    content_type = request.headers.get('Content-Type')
    if content_type != 'application/json':
        return return_message('failure', 'Content-Type not supported')
    
    broker_id= None
    try:
        receive = request.json
        broker_id = receive['broker_id']
    except:
        return return_message('failure', 'can not parse request')
    
    try:
        broker = Broker.query.filter_by(broker_id).first()
        if broker is None:
            return return_message('failure', 'broker does not exist')
        broker.health = 0
        broker.timestamp = 0.0
        db.session.flush()
        db.session.commit()
        return return_message('success')
    except:
        return return_message('failure', 'error while commiting/queriing to database')
    
@app.route('/brokers/heartbeat',methods=['POST'])
def broker_heartbeat():
    print_thread_id()
    if not primary:
        return return_message('failure', 'not replica not allowed for this endpoint')
    
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
        # auto register broker
        broker = Broker.query.filter_by(ip=ip, port=port).first()
        if broker is not None:
            broker.health = 1
            broker.timestamp = time.time()
            db.session.flush()
            db.session.commit()
            return return_message('success', 'broker heartbeat received')
    
        broker = Broker(ip=ip, port=port, health=1, timestamp=time.time())
        db.session.add(broker)
        db.session.flush()
        db.session.commit()
        return return_message('success', 'new broker registered')
    except:
        return return_message('failure', 'Error while querying/comitting to database')

# producer endpoints (strictly serviced by write)
# topic registration
@app.route('/topics', methods=['POST'])
def topic_register_request():
    print_thread_id()
    if not primary:
        return return_message('failure', 'endpoint not supported')
    
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

        # commit transaction
        db.session.commit()
        return return_message('success', 'topic ' + topic.name + ' created sucessfully')
    except:
        return return_message('failure', 'Error while querying/comitting to database')

# partition registration
@app.route('/partitions', methods=['POST'])
def partition_register_request():
    print_thread_id()
    if not primary:
        return return_message('failure', 'endpoint not supported')
    
    content_type = request.headers.get('Content-Type')
    if content_type != 'application/json':
        return return_message('failure', 'Content-Type not supported')

    # parse content
    topic_name = None
    try:
        receive = request.json
        topic_name = receive['topic_name']
        broker_id = receive['broker_id']
    except:
        return return_message('failure', 'Error While Parsing json')
    
    # database
    try:
        topic = Topic.query.filter_by(name=topic_name).first()
        if topic is None:
            return return_message('failure', 'Topic does not exist')
        
        broker = Broker.query.filter_by(id=broker_id, health=1).first()
        if broker is None:
            return return_message('failure', 'no healthy broker with given id exists')
        
        partition = Partition(topic_id=topic.id, broker_id=broker.id)
        db.session.add(partition)
        db.session.flush()
        
        # commit transaction
        db.session.commit()
        return {
            "status": "success",
            "partition_id": partition.id
        }
    except:
        return return_message('failure', 'Error while querying/comitting to database')

# producer registration
@app.route('/producer/register',methods=['POST'])
def producer_register_request():
    print_thread_id()
    if not primary:
        return return_message('failure', 'endpoint not supported')
    
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
            producer = Producer(topic_id=topic.id, partition_id=-1, health=1, timestamp=time.time())
        else:
            # find if partition exists
            partition = Partition.query.filter_by(id=partition_id).first()
            if partition is None:
                return return_message('failure', 'Partition does not exist')
            
            producer = Producer(topic_id=topic.id, partition_id=partition_id, health=1,timestamp=time.time())

        db.session.add(producer)
        db.session.flush()
        db.session.commit()
        return {
            "status": "success",
            "producer_id": producer.id
        }
    except:
        return return_message('failure','Error while querying/commiting database')

# producer/produce
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
        
        producer.timestamp = time.time()
        db.session.flush()
        db.session.commit()
        
        if producer.topic.name != topic_name:
            return return_message('failure', 'producer_id and topic do not match')
        
        request_content = {
            "topic_id": producer.topic.id,
            "message_content": message_content,
            "producer_client": prod_client,
            "timestamp": timestamp,
            "random_string": random_string
        }
        
        if producer.partition_id == -1:
            # produce to any random implicit partition, with good health
            brokers = Broker.query.filter_by(health=1).all()
            for _ in range(max_tries):
                if len(brokers) < 1: continue
                random_choice = random.randint(0, len(brokers)-1)
                broker = brokers[random_choice]
                ip = broker.ip
                port = broker.port

                request_content['partition_id'] = -1
                try:
                    res = requests.post(f'http://{ip}:{port}/store_message', json=request_content)
                    if res.ok:
                        response = res.json()
                        print(response)
                        if response['status'] == 'success':
                            return return_message('success')
                except:
                    print('exception occured in parsing response/ can not connect')
        else:
            # produce to that specific partition
            partition = Partition.query.filter_by(id=producer.partition_id).first()
            ip = partition.broker.ip
            port = partition.broker.port

            request_content['partition_id'] = partition.id
            try:
                res = requests.post('http://' + ip + ":" + str(port) + '/store_message', json=request_content)
                if res.ok:
                    response = res.json()
                    if response['status'] == 'success':
                        return return_message('success')
            except:
                print('exception occured in parsing response/ can not connect')

        return return_message('failure', 'can not commit to a broker')    
    except:
        traceback.print_exc()
        return return_message('failure','Error while querying/commiting database')

# consumer endpoints (strictly/partially serviced by read)
# consumer registration
# producer registration
@app.route('/consumer/register',methods=['POST'])
def consumer_register_request():
    print_thread_id()
    # updates metadata, redirect to primary
    if not primary:
        return redirect(f'http://{sync_address}/consumer/register', 307)
    
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
            consumer = Consumer(topic_id=topic.id, partition_id=-1, health=1, timestamp=time.time())
        else:
            # find if partition exists
            partition = Partition.query.filter_by(id=partition_id).first()
            if partition is None:
                return return_message('failure', 'Partition does not exist')
            
            consumer = Consumer(topic_id=topic.id, partition_id=partition_id, health=1,timestamp=time.time())

        db.session.add(consumer)
        db.session.flush()
        db.session.commit()
        return {
            "status": "success",
            "consumer_id": consumer.id
        }
    except:
        return return_message('failure','Error while querying/commiting database')

@app.route('/consumer/health_poll', methods=['POST'])
def consumer_health_poll():
    print_thread_id()
    if not primary:
        return redirect(f'http://{sync_address}/consumer/health_poll', code=307)
    
    content_type = request.headers.get('Content-Type')
    if content_type != 'application/json':
        return return_message('failure', 'Content-Type not supported')

    # parsing
    consumer_id = None
    try:
        receive = request.json
        consumer_id = receive['consumer_id']
    except:
        return return_message('failure', 'Error while parsing request')
    
    try:
        consumer = Consumer.query.filter_by(id=consumer_id).first()
        if consumer is None:
            return return_message('failure', 'consumer_does not exist')
        consumer.health = 1
        consumer.timestamp = time.time()
        db.session.flush()
        db.session.commit()
    except:
        traceback.print_exc()
        return_message('failure', 'error while updating health status!')


# consumer/consume
@app.route('/consumer/consume', methods=['GET'])
def consumer_dequeue():
    print_thread_id()
    if primary:
        return return_message('failure', 'primary does not serve consumers')
    consumer_id = None
    try:
        consumer_id = request.args.get('consumer_id')
        consumer_id = int(consumer_id)
    except:
        return return_message('failure', 'Error while parsing request')
    
    # query relevant parameters
    consumer = None
    brokers = None
    topic_id = None
    partition_id = None
    try:
        consumer = Consumer.query.filter_by(id=consumer_id).first()
        if consumer is None:
            return return_message('failure', 'consumer does not exist')
        topic_id = consumer.topic.id
        partition_id = consumer.partition_id

        try:
            res = requests.post(f'http://{sync_address}/consumer/health_poll', json={"id":consumer.id})
            if not res.ok:
                print(f'health status, wrong code: {res.status_code}')
        except:
            traceback.print_exc()

        # find broker information
        if consumer.partition_id == -1:
            brokers = Broker.query.filter_by(health=1).all()
        else:
            partition = Partition.query.filter_by(id=consumer.partition_id).first()
            broker = partition.broker
            if broker.health != 1:
                return return_message('failure', 'broker not healthy for requested partition')
            brokers = [broker]

    except:
        traceback.print_exc()
        return return_message('failure','Error while querying/commiting database')
    
    # request the brokers
    for _ in range(max_tries):
        broker = random.choice(brokers)
        try:
            content = {
                "consumer_id": consumer_id,
                "topic_id": topic_id,
                "partition_id": partition_id
            }
            res = requests.get(f'http://{broker.ip}:{broker.port}/consume', params=content)
            if res.ok:
                result = res.json()
                return result
            else: print(f'invalid response code: {res.status_code}')
        except:
            traceback.print_exc()
    return return_message('failure', 'max tries reached')

# metadata sync
@app.route('/metadata/sync', methods=['GET'])
def get_metadata():
    print_thread_id()
    if not primary:
        return return_message('failure', 'I am not the leader')
    
    #parse parameters
    try:
        ip = request.args.get('ip')
        port = request.args.get('port')
        if ip is None:
            raise Exception("no ip received from replica")
        
        # query database
        replica = Replica.query.filter_by(ip=ip, port=port).first()
        if replica is None:
            replica = Replica(ip=ip, port=port, health=1, timestamp=time.time())
            db.session.add(replica)
        else:
            replica.timestamp = time.time()
            replica.health = 1
        db.session.flush()

        db.session.commit() 
    except:
        traceback.print_exc()
        print('can not update health status for repica')
    
    # sending required stuff
    try:
        with db_lock:
            topics = Topic.query.order_by(Topic.id).all()
            producers = Producer.query.order_by(Producer.id).all()
            consumers = Consumer.query.order_by(Consumer.id).all()
            brokers = Broker.query.order_by(Broker.id).all()
            partitions = Partition.query.order_by(Partition.id).all()
        
        topics     = [ e.as_dict() for e in topics ]
        producers  = [ e.as_dict() for e in producers ]
        consumers  = [ e.as_dict() for e in consumers ]
        brokers    = [ e.as_dict() for e in brokers ]
        partitions = [ e.as_dict() for e in partitions ]

        return {
            "status": "success",
            "topics": topics,
            "producers": producers,
            "consumers": consumers,
            "brokers": brokers,
            "partitions": partitions
        }
    except:
        traceback.print_exc()
        return return_message('failure', 'error while querying database')
    
# discover replicas
@app.route('/replicas', methods=['GET'])
def get_replicas():
    print_thread_id()
    if not primary:
        return return_message('failure', 'I am not the leader')
    
    try:
        replicas = Replica.query.filter_by(health=1).all()
        replicas = [ {"ip": r.ip, "port": r.port} for r in replicas ]
        return {
            "status": "success",
            "replicas": replicas
        }
    except:
        traceback.print_exc()
        return return_message('failure', 'can not query database')