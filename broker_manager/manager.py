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
health_timeout = 5

sync_address = '172.17.0.2:5000'
primary = False

if sys.argv[1] == 'write':
    print('+++++ started as primary')
    primary = True

max_tries = 3
try_timeout = 2

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = f"postgresql://{username}:{password}@localhost:{db_port}/{database}"

# queue database structures
db = SQLAlchemy(app)
db_lock = threading.Lock()

sys.path.append(os.getcwd())
from broker_manager.common.db_model import *
from broker_manager.common.routes_manager import *
from broker_manager.common.sync_manager import *

if __name__ == "__main__": 
    with app.app_context():
        db.create_all()
    
    thread = None
    if primary:
        thread = threading.Thread(target=health_heartbeat, args=(heartbeat_time,))
    else:
        thread = threading.Thread(target=sync_metadata_heartbeat, args=(heartbeat_time,))
    thread.start()

    # launch request handler
    app.run(host='0.0.0.0',debug=False, threaded=True, processes=1)
    app_kill_event = True
    thread.join()