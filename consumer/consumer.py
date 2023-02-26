import sys
import os
sys.path.append(os.getcwd())
import time

from queueSDK.consumer import Consumer

address = '172.17.0.2'
port = 5000

consumer = Consumer(address, port, 'C-1')
print(consumer.register('T-1'))

while True:
   print(consumer.dequeue('T-1'))
   time.sleep(2)

