from flask import Flask
from flask import request
from flask_sqlalchemy import SQLAlchemy

import sys
import os

username = 'mattie'
password = 'password'
database = 'psqlqueue'
db_port = '5432'

max_tries = 3
try_timeout = 2

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = f"postgresql://{username}:{password}@localhost:{db_port}/{database}"

# queue database structures
db = SQLAlchemy(app)

sys.path.append(os.getcwd())
from broker_manager.write.db_model import *
from broker_manager.write.routes import *


if __name__ == "__main__": 
    # 
    with app.app_context():
        db.create_all()

    # launch request handler
    app.run(host='0.0.0.0',debug=False, threaded=True, processes=1)