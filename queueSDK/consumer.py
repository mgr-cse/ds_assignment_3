from typing import Dict
from typing import Tuple
from typing import Set
import requests
import json
import time
import sys
import threading

class Consumer:
    def __init__(self, host_primary: str, port_primary: int, name: str = '') -> None:
        replica_info:Set[Tuple(str,int)] = set()  
        self.hostname: str = f'http://{host_primary}:{port_primary}/'
        self.current_replica = None

        self.ids : Dict[Tuple(str, int), int] = {}
        self.name: str = name
    
    def eprint(self, *args, **kwargs):
            print(self.name, *args, file=sys.stderr, **kwargs)

    def change_replica(self):
        # update list
        self.eprint('replica change init')
        try:
            res = requests.get(self.hostname + 'replicas')
            if  not res.ok:
                self.eprint(f'invalid response code received: {res.status_code}')
                return
            response = res.json()
            
            if response['status'] != 'success':
                self.eprint(response)
                return
        
            for r in response['replicas']:
                ip = r['ip']
                port = r['port']
                host = f'http://{ip}:{port}/'
                if host != self.current_replica:
                    self.current_replica = host
                    print(host)
                    return

        except:
            self.eprint('can not communicate to primary for replicas')

    def register(self, topic: str, partition: int=-1) -> int:
        if (topic, partition) in self.ids:
            return -1
        
        response = None
        try:
            request_content = {"topic":topic}
            if partition != -1:
                request_content['partition_id'] = partition
            if self.current_replica is None:
                self.eprint('error: replica address not set')
                raise Exception('Change replica exception')
            res = requests.post(self.hostname + 'consumer/register', json=request_content)
            if res.ok:
                response = res.json()
                if response['status'] == 'failure':
                    self.eprint(response)
                    return -1
            else:
                self.eprint(f'invalid response code: {res.status_code}')
        except:
            self.eprint('Can not make a post request/received unparsable response')
            self.change_replica()
            return -1
        try:
           self.ids[(topic, partition)] = response['consumer_id']
           return response['consumer_id']
        except:
            self.eprint('error while parsing response')
            return -1

    def dequeue(self, topic: str, partition: int=-1) -> bool|str:
        if (topic,partition) not in self.ids:
            self.eprint('not registered for topic', topic)
            self.change_replica()
            return False
        
        cons_id = self.ids[(topic,partition)]
        try:
            if self.current_replica is None:
                self.eprint('error: replica address not set')
                raise Exception("Exception: no replica")
            res = requests.get(self.current_replica + 'consumer/consume', params={"consumer_id": cons_id, "topic": topic})
            if res.ok:
                try:
                    response = res.json()
                    if response['status'] == 'success':
                        return response['message'] 
                    else: self.eprint(response['message'])
                except:
                    self.eprint('Invalid response:', res.text)
            else: self.eprint('received unexpected response code', res.status_code)
        except:
            self.eprint('Can not make post request')
            self.change_replica()
        return False
    

