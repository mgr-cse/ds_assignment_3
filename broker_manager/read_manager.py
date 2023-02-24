from flask import Flask
from flask import request
import threading
from flask_sqlalchemy import SQLAlchemy

username = 'mattie'
password = 'password'
database = 'psqlqueue'
db_port = '5432'

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


class Topic(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    name = db.Column(db.String(255), nullable=False, unique=True)
    
    producers  = db.relationship('Producer', backref='topic')
    consumers  = db.relationship('Consumer', backref='topic')
    messages   = db.relationship('Message', backref='topic')
    partitions = db.relationship('Partition', backref='topic')

class Message(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    topic_id = db.Column(db.Integer, db.ForeignKey('topic.id'))
    partition_id = db.Column(db.Integer, db.ForeignKey('partition.id'))
    message_content = db.Column(db.String(255))
    # producer sends some info to uniquely identify the message
    producer_client = db.Column(db.String(255))
    timestamp = db.Column(db.Float)
    random_string = db.Column(db.String(257))

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

if __name__ == "__main__": 
    # 
    with app.app_context():
        db.create_all()

    # launch request handler
    app.run(host='0.0.0.0',debug=False, threaded=True, processes=1)