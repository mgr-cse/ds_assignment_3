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