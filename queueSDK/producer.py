from typing import Dict
from typing import Tuple
import requests
import json
import traceback
import sys

class Producer:
    def __init__(self, host: str, port: int, name: str = '') -> None:
        self.hostname: str = 'http://' + host + ':' + str(port) + '/'
        self.ids : Dict[Tuple(str, int), int] = {}
        self.name: str = name

    def eprint(self, *args, **kwargs):
        print(self.name, *args, file=sys.stderr, **kwargs)
    
    def register(self, topic: str, partition: int=-1) -> int:
        # self check
        if (topic, partition) in self.ids:
            self.eprint('already registered for the topic', topic)
            return -1
        
        # prepare request
        request_content = {"topic":topic}
        if partition != -1:
            request_content['partition_id'] = partition
        
        # make request
        try:
            res = requests.post(self.hostname + 'producer/register', json=request_content)
        except:
            self.eprint('Error Can not make a post request')
            return -1
        
        # parse request
        try: 
            if not res.ok:
                self.eprint('received unexpected response code', res.status_code)
                return -1
            
            response = res.json()
            if response['status'] == 'success':
                self.ids[(topic, partition)] = response['producer_id']
                return response['producer_id']
            
            self.eprint(response)
        except:
            self.eprint('Invalid Response:', res.text) 
        return -1
            

    def enqueue(self, topic: str, message: str, timestamp: float, random_string: str, partition: int=-1) -> bool:
        # self check
        if (topic, partition) not in self.ids:
            self.eprint('not registered for the topic/partition!')
            return False
        
        # prepare request
        prod_id = self.ids[(topic, partition)]
        request_content = {
            "topic": topic,
            "producer_id": prod_id,
            "message": message,
            "prod_client": self.name,
            "timestamp": timestamp,
            "random_string": random_string
        }
        
        # make request
        try:
            res = requests.post(self.hostname + 'producer/produce', json=request_content)
        except:
            self.eprint('Can not make a post request')
            return False
        
        # parse request
        try:    
            if not res.ok:
                self.eprint('received unexpected response code', res.status_code)
                return False
                    
            response = res.json()
            if response['status'] == 'success':
                return True  
            self.eprint(response)
        except:
            self.eprint('Invalid response:', res.text)
        return False
