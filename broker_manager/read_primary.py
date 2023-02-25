from flask import Flask
from flask import request
import threading
from flask_sqlalchemy import SQLAlchemy

import os
import sys

username = 'mattie'
password = 'password'
database = 'psqlqueue'
db_port = '5432'

app_kill_event = False
heartbeat_time = 5
sync_address = '172.17.0.2:5000'

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = f"postgresql://{username}:{password}@localhost:{db_port}/{database}"

# queue database structures
db = SQLAlchemy(app)

sys.path.append(os.getcwd())
from broker_manager.read.db_model import *
from broker_manager.read.routes import *
from broker_manager.read.sync import *

if __name__ == "__main__": 
    with app.app_context():
        db.create_all()
        # launch heartbeats
        thread = threading.Thread(target=heartbeat_sync_broker, args=(heartbeat_time,))
        thread.start()

    # launch request handler
    app.run(host='0.0.0.0',debug=False, threaded=True, processes=1)
    app_kill_event = True
    thread.join()