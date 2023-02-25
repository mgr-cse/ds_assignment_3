from typing import Dict
from typing import Tuple
import requests
import sys

class Consumer:
    def __init__(self, host_primary: str, port_primary: int, name: str = '') -> None:
        self.hostname: str = f'http://{host_primary}:{port_primary}/'
        self.current_replica = None

        self.ids : Dict[Tuple(str, int), int] = {}
        self.name: str = name
    
    def eprint(self, *args, **kwargs):
            print(self.name, *args, file=sys.stderr, **kwargs)

    def change_replica(self):
        # update list
        self.eprint('replica change init')
        
        # request
        res = None
        try:
            res = requests.get(self.hostname + 'replicas')
        except:
            self.eprint('can not communicate to primary for replicas')
            return
        
        # parse the request
        try:    
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
                    return
        except:
            self.eprint('change replica: error while parsing response')
        return