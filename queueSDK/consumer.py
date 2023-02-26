from typing import Dict
from typing import Tuple
import requests
import json
import time
import sys
import threading

class Consumer:
    def __init__(self, host: str, port: int, name: str = '') -> None:
        self.hostname: str = 'http://' + host + ':' + str(port) + '/'
        self.ids : Dict[Tuple(str, int), int] = {}
        self.name: str = name
    
    def eprint(self, *args, **kwargs):
            print(self.name, *args, file=sys.stderr, **kwargs)
    
    def register(self, topic: str, partition: int=-1) -> int:
        if (topic, partition) in self.ids:
            self.eprint('already registered to topic/partition', topic)
            return -1
        
        response = None
        try:
            request_content = {"topic":topic}
            if partition != -1:
                request_content['partition_id'] = partition
            res = requests.post(self.hostname + 'consumer/register', json=request_content)
            if res.ok:
                response = res.json()
                if response['status'] == 'failure':
                    self.eprint(response)
                    return -1
        except:
            self.eprint('Can not make a post request/received unparsable response')
            return -1
        try:
           pass
        except:
            self.eprint('error while parsing response')

    def dequeue(self, topic: str) -> bool|str:
        if topic not in self.ids:
            self.eprint('not registered for topic', topic)
            return False
        
        cons_id = self.ids[topic]
        try:
            res = requests.get(self.hostname + 'consumer/consume', params={"consumer_id": cons_id, "topic": topic})
            if res.ok:
                try:
                    response = res.json()
                    if response['status'] == 'success':
                        return response['message'] 
                    else: self.eprint(response['message'])
                except:
                    self.eprint('Invalid response:', res.text)
            else:
                self.eprint('received unexpected response code', res.status_code)
        except:
            self.eprint('Can not make post request')
        return False
    

