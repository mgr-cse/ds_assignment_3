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

class Message(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    topic_id = db.Column(db.Integer, nullable=False)
    partition_id = db.Column(db.Integer, nullable=False)
    message_content = db.Column(db.String(255))
    
    # producer sends some info to uniquely identify the message
    producer_client = db.Column(db.String(255), nullable=False)
    timestamp = db.Column(db.Float, nullable=False)
    random_string = db.Column(db.String(257), nullable=False)

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

    except:
        return return_message('failure', 'Error while querying/comitting to database')
    
    return return_message('success', 'topic ' + topic.name + ' created sucessfully')

@app.route('/topics', methods=['GET'])
def topic_get_request():
    print_thread_id()
    topics_list = []
    try:
        # database
        topics = Topic.query.all()
        for t in topics:
            topics_list.append(t.name)
        db.session.commit()
        return return_message('success', topics_list)
    except: 
        return return_message('failure', 'Error while listing topics')

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
            producer = Producer(topic_id=topic.id)
        else:
            # find if partition exists
            partition = Partition.query.filter_by(id=partition_id).first()
            if partition is None:
                return return_message('failure', 'Topic does not exist')
            
            producer = Producer(topic_id=topic.id, partition_id=partition_id)

        db.session.add(producer)
        db.session.commit()
        return {
            "status": "success",
            "producer_id": producer.id
        }
    except:
        return return_message('Failure','Error while querying/commiting database')

@app.route('/consumer/register', methods=['POST'])
def consumer_register_request():
    print_thread_id()
    content_type = request.headers.get('Content-Type')
    if content_type != 'application/json':
        return return_message('failure', 'Content-Type not supported')
    topic_name = None
    try:
        receive = request.json
        topic_name = receive['topic']
    except:
        return return_message('failure', 'Error while parsing request')
        
    # query
    try:
        topic = Topic.query.filter_by(name=topic_name).first()
        if topic is None:
            return return_message('failure', 'Topic does not exist')
        consumer = Consumer(topic_id=topic.id, offset=-1)
        db.session.add(consumer)
        db.session.commit()
        return {
            "status": "success",
            "consumer_id": consumer.id
        }
    except:
        return return_message('Failure','Error while querying/commiting database')

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
    try:
        receive = request.json
        topic_name = receive['topic']
        producer_id = receive['producer_id']
        message_content = receive['message']
        prod_client = receive['prod_client']
    except:
        return return_message('failure', 'Error while parsing request')
    
    try:
        producer = Producer.query.filter_by(id=producer_id).first()
        if producer is None:
            return return_message('failure', 'producer_id does not exist')
        
        if producer.topic.name != topic_name:
            return return_message('failure', 'producer_id and topic do not match')
        
        message = Message(topic_id=producer.topic.id, message_content=message_content, producer_client=prod_client)
        db.session.add(message)
        db.session.commit()
        return return_message('success')
    except:
        return return_message('Failure','Error while querying/commiting database')

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
        return return_message('Failure','Error while querying/commiting database')
    

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
        return return_message('Failure','Error while querying/commiting database')    

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
    
@app.route('/producer/client_size',methods=['GET'])
def prod_client_size():
    print_thread_id()
    prod_client_name = None
    try:
        prod_client_name = request.args.get('prod_client')
    except:
        return return_message('failure', 'Error while parsing request')
    
    try:
        message_count = Message.query.filter_by(producer_client=prod_client_name).count()
        return {
            "status": "success",
            "count": message_count
        }
    except:
        return return_message('Failure','Error while querying/commiting database')

if __name__ == "__main__": 
    # 
    with app.app_context():
        db.create_all()
        # crash recovery
        # heartbeat threads

    # launch request handler
    app.run(host='0.0.0.0',debug=True, threaded=True, processes=1)