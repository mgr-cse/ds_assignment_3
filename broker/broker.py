from flask import Flask
from flask import request
from flask_sqlalchemy import SQLAlchemy
import traceback

import threading
import time
import socket
import requests

broker_manager_address = '172.17.0.2:5000'
raft_port = '4001'
heartbeat_time = 2
app_kill_event = False

username = 'mattie'
password = 'password'
database = 'psqlqueue'
db_port = '5432'

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = f"postgresql://{username}:{password}@localhost:{db_port}/{database}"

# queue database structures
db = SQLAlchemy(app)

db_lock = threading.Lock()

# raft
from pysyncobj import SyncObj, SyncObjConf
from pysyncobj.batteries import ReplLockManager, ReplList, ReplDict

class Message(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    topic_id = db.Column(db.Integer, nullable=False)
    partition_id = db.Column(db.Integer, nullable=False)
    message_content = db.Column(db.String(255))
    
    # producer sends some info to uniquely identify the message
    producer_client = db.Column(db.String(255), nullable=False)
    timestamp = db.Column(db.Float, nullable=False)
    random_string = db.Column(db.String(257), nullable=False)

    def as_dict(self):
       return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class Offsetscons(db.Model):
    consumer_id = db.Column(db.Integer, primary_key = True)
    offset = db.Column(db.Integer)

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
@app.route('/store_message', methods=['POST'])
def topic_register_request():
    print_thread_id()
    content_type = request.headers.get('Content-Type')
    if content_type != 'application/json':
        return return_message('failure', 'Content-Type not supported')

    # parse content
    topic_id = None
    partition_id = None
    message_content = None
    producer_client = None
    timestamp = None
    random_string = None
    try:
        receive = request.json
        topic_id = receive['topic_id']
        partition_id = receive['partition_id']
        message_content = receive['message_content']
        producer_client = receive['producer_client']
        timestamp = receive['timestamp']
        random_string = receive['random_string']
    except:
        print('parsing error')
        return return_message('failure', 'Error While Parsing json')
    
    # database
    try:
        # check if the message was already written
        message = Message.query.filter_by(producer_client=producer_client, timestamp=timestamp, random_string=random_string).first()
        if message is not None:
            return return_message('success')

        # write the messaage
        message = Message(
                topic_id=topic_id,
                partition_id=partition_id,
                message_content=message_content,
                producer_client=producer_client,
                timestamp=timestamp,
                random_string = random_string
            )
        
        print('reached')
        # lock database because message id must be strictly orderd among requests
        with db_lock:
            db.session.add(message)
            # write ahead
            db.session.flush()
            # commit transaction
            db.session.commit()
        
        message_object.append(receive, sync=True)
        return return_message('success')
    except:
        print('database error')
        traceback.print_exc()
        return return_message('failure', 'Error while querying/comitting to database')

@app.route('/retreive_messages', methods=['GET'])
def topic_get_request():
    print_thread_id()
    offset = None
    try:
        offset = request.args.get('offset')
        offset = int(offset)
    except:
        return return_message('failure', 'Error while parsing request')
    
    try:
        messages = Message.query.filter(Message.id > offset).order_by(Message.id).all()
        message_list = [ m.as_dict() for m in messages ]
        
        return {
            "status": 'success',
            "messages": message_list,
        }
    except:
        return return_message('failure','Error while querying database')
    
@app.route('/consume', methods=['GET'])
def consume():
    consumer_id = None
    topic_id = None
    partition_id = None

    try:
        consumer_id = request.args.get('consumer_id')
        consumer_id = int(consumer_id)
        topic_id = request.args.get('topic_id')
        topic_id = int(topic_id)
        partition_id = request.args.get('partition_id')
        partition_id =int(partition_id)
    except:
        print('can not parse request')
        return return_message('failure', 'error in parsing request parameters')
    
    try:
        offset = Offsetscons.query.filter_by(consumer_id=consumer_id).first()
        if offset is None:
            offset = Offsetscons(consumer_id=consumer_id, offset=0)
            db.session.add(offset)
            db.session.flush()
        
        if partition_id == -1:
            message = Message.query.filter(Message.id>offset.offset, Message.topic_id==topic_id).order_by(Message.id).first()
        else:
            message = Message.query.filter(Message.id>offset.offset, Message.topic_id==topic_id, Message.partition_id==partition_id).order_by(Message.id).first()
        if message is None:
            return return_message('failure', 'no more messages')
        offset.offset = message.id
        
        db.session.flush()
        db.session.commit()
        
        if message is not None:
            return return_message('success', message.message_content)
        
    except:
        traceback.print_exc()
        return return_message('failure', 'can not query or commit to database')

# heartbeat function
def heartbeat(beat_time):
    while True:
        if app_kill_event:
            return
        print('heart beat <3')
        # get self ip
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()

        # send ip to broker_manager
        try:
            res = requests.post('http://' + broker_manager_address + '/brokers/heartbeat', json={"ip": ip, "port": 5000})
        except:
            print('can not make connection')
        time.sleep(beat_time)

        # debug prints
        print(message_object.rawData())

        # add other brokers to cluster automatically
        print('Current connected nodes:', syncObj.otherNodes)
        broker_set = set()
        for b in syncObj.otherNodes:
            broker_set.add(b.host)

        broker_set_new = set()
        try:
            res = requests.get(f'http://{broker_manager_address}/brokers')
            if not res.ok:
                raise Exception(f'response received: {res.status_code}')
            response = res.json()
            if response['status'] != 'success':
                raise Exception(f'status: failure')
            
            for b in response['brokers']:
                broker_set_new.add(b['ip'])
            difference = broker_set_new - broker_set

            for b in difference:
                syncObj.addNodeToCluster(f'{b}:{raft_port}')
        except:
            print('can not find other brokers')



if __name__ == "__main__":
    thread = None
    with app.app_context():
        db.create_all()
        # launch heartbeats
        thread = threading.Thread(target=heartbeat, args=(heartbeat_time,))
        thread.start()

        # set up raft
        message_object = ReplList()
        offset_object = ReplDict()
        obj_lock = ReplLockManager(autoUnlockTime=10)

        # get self ip
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()

        cfg = SyncObjConf(dynamicMembershipChange = True)
        syncObj = SyncObj(f'{ip}:{raft_port}', [], cfg, [message_object, offset_object, obj_lock])

    # launch request handler
    app.run(host='0.0.0.0',debug=False, threaded=True, processes=1)
    app_kill_event = True
    thread.join()