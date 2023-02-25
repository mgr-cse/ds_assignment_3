from flask_sqlalchemy import SQLAlchemy

from __main__ import db
db: SQLAlchemy

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

    def as_dict(self):
       return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class Partition(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    broker_id = db.Column(db.Integer, db.ForeignKey('broker.id'))
    topic_id = db.Column(db.Integer, db.ForeignKey('topic.id'))

    def as_dict(self):
       return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class Topic(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    name = db.Column(db.String(255), nullable=False, unique=True)
    
    producers  = db.relationship('Producer', backref='topic')
    partitions = db.relationship('Partition', backref='topic')

    def as_dict(self):
       return {c.name: getattr(self, c.name) for c in self.__table__.columns}