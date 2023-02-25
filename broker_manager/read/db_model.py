from flask_sqlalchemy import SQLAlchemy

from __main__ import db
db: SQLAlchemy

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
