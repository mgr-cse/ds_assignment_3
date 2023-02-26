from flask import Flask
from flask import request
import threading
from flask_sqlalchemy import SQLAlchemy
from typing import Dict
import traceback

username = 'mattie'
password = 'password'
database = 'psqlqueue'
db_port = '5432'

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = f"postgresql://{username}:{password}@localhost:{db_port}/{database}"

# # queue database structures
db = SQLAlchemy(app)
 
class Name(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    name = db.Column(db.String(255))

    def as_dict(self):
       return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
    def from_dict(self, obj: Dict):
        for k in obj:
            setattr(self, k, obj[k])


@app.route('/test', methods=['GET'])
def topic_get_request():
    topics_list = []
    try:
        # database
        name = Name()
        name.from_dict({
            "id": 301,
            "name": "foo1"
        })
        db.session.add(name)
        name = Name(name='bar')
        db.session.add(name)
        db.session.commit()
        return {"status": "success"}
    except: 
        traceback.print_exc()
    return {"status": "false"}

if __name__ == "__main__": 
    # create database
    with app.app_context():
        db.create_all()
    app.run(debug=True, threaded=True, processes=1)