from flask_sqlalchemy import SQLAlchemy
from typing import Dict

from __main__ import db
db: SQLAlchemy

class Producer(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    topic_id = db.Column(db.Integer, db.ForeignKey('topic.id'))
    partition_id = db.Column(db.Integer)
    
    health = db.Column(db.Integer)
    timestamp = db.Column(db.Float)

    def as_dict(self):
       return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class Consumer(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    topic_id = db.Column(db.Integer, db.ForeignKey('topic.id'))
    partition_id = db.Column(db.Integer)
    
    health = db.Column(db.Integer)
    timestamp = db.Column(db.Float)

class Broker(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    ip = db.Column(db.String(255))
    port = db.Column(db.Integer)
    
    health = db.Column(db.Integer)
    timestamp = db.Column(db.Float)

class Partition(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    broker_id = db.Column(db.Integer, db.ForeignKey('broker.id'))
    topic_id = db.Column(db.Integer, db.ForeignKey('topic.id'))

class Topic(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    name = db.Column(db.String(255), nullable=False, unique=True)
    
    producers  = db.relationship('Producer', backref='topic')
    partitions = db.relationship('Partition', backref='topic')
    consumers = db.relationship('Consumer', backref='topic')
