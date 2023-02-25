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