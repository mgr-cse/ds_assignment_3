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
        if (topic, partition) in self.ids:
            self.eprint('already registered for the topic', topic)
            return -1
        try:
            request_content = {"topic":topic}
            if partition != -1:
                request_content['partition_id'] = partition
            res = requests.post(self.hostname + 'producer/register', json=request_content)
            if res.ok:
                try:
                    response = res.json()
                    if response['status'] == 'success':
                        self.ids[(topic, partition)] = response['producer_id']
                        return response['producer_id']
                    else: self.eprint(response)
                except :
                    self.eprint('Invalid Response:', res.text)
            else: self.eprint('received unexpected response code', res.status_code)
        except:
            self.eprint('Error Can not make a post request')
        return -1
            

    def enqueue(self, topic: str, message: str, timestamp: float, random_string: str, partition: int=-1) -> bool:
        if (topic, partition) not in self.ids:
            self.eprint('not registered for the topic/partition!')
            return False
        
        prod_id = self.ids[(topic, partition)]
        request_content = {
            "topic": topic,
            "producer_id": prod_id,
            "message": message,
            "prod_client": self.name,
            "timestamp": timestamp,
            "random_string": random_string
        }
        
        try:
            res = requests.post(self.hostname + 'producer/produce', json=request_content)
            if res.ok:
                try:
                    response = res.json()
                    if response['status'] == 'success':
                        return True  
                    self.eprint(response)
                except:
                    self.eprint('Invalid response:', res.text)
            else: self.eprint('received unexpected response code', res.status_code)
        except:
            self.eprint('Can not make a post request')
        return False


    
    def get_count(self) -> int:
        try:
            res = requests.get(self.hostname + 'producer/client_size', params={"prod_client":self.name})
            if res.ok:
                try:
                    response = res.json()
                    if response['status'] == 'success':
                        return response['count']
                except:
                    self.eprint('Invalid response:', res.text)
            else: self.eprint('received unexpected response code', res.status_code)
        except:
            self.eprint('Can not make a post request')
        return -1
            
