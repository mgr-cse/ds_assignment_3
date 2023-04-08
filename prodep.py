@app.route('/brokers', methods=['GET'])
def broker_get_request():
    print_thread_id()
    broker_list = []
    try:
        # database
        brokers = Broker.query.filter_by(health=1).all()
        for b in brokers:
            broker_list.append(b.as_dict())
        return {
            "status": "success",
            "brokers": broker_list
        }
    except:
        traceback.print_exc()
        return return_message('failure', 'Error while listing brokers')

# broker cluster management
@app.route('/brokers/create/partition', methods=['POST'])
def broker_create_part():
    print_thread_id()
    if not primary:
        return return_message('failure', 'not replica not allowed for this endpoint')
    
    content_type = request.headers.get('Content-Type')
    if content_type != 'application/json':
        return return_message('failure', 'Content-Type not supported')
    
    broker_id = None
    topic_name = None
    try:
        receive = request.json
        broker_id = receive['broker_id']
        topic_name = receive['topic']
    except:
        return return_message('failure', 'can not parse request')
    
    try:
        topic = Topic.query.filter_by(name=topic_name).first()
        if topic is None:
            return return_message('failure', 'Topic does not exist')

        broker = Broker.query.filter_by(id=broker_id).first()
        if broker is None:
            return return_message('failure', 'broker does not exist')
        
        partition = Partition(broker_id=broker.id, topic_id=topic.id)
        db.session.add(partition)
        db.session.flush()
        db.session.commit()
        return {
            "status": "success",
            "partition_id": partition.id
        }
    except:
        traceback.print_exc()
        return return_message('failure', 'error while commiting/quering to database')