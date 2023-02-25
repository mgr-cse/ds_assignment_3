import sys
import os
sys.path.append(os.getcwd())

from queueSDK.producer import Producer
import random
import string
import time
import requests

broker_maganger_ip = '172.17.0.2'
broker_manager_port = 5000

# create topic request
res = requests.post('http://' + broker_maganger_ip + ':' + str(broker_manager_port) + '/topics', json={'topic_name': 'T-1'})


prod = Producer(broker_maganger_ip, broker_manager_port, 'P-1')

prod.register('T-1', 1)

for i in range(30):
    # prepare message
    random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=256))
    timestamp = time.time()

    while not prod.enqueue('T-1', f'debug {i}', timestamp, random_string, 1):
        time.sleep(1)
    print('message enqueued!')
    time.sleep(1)