from typing import Dict
import requests
import json
import time
import sys
import threading

class Consumer:
    def __init__(self, host: str, port: int, name: str = '') -> None:
        self.hostname: str = 'http://' + host + ':' + str(port) + '/'
        self.ids : Dict[str, int] = {}
        self.name: str = name
        self.last_message_time = time.time()
    
    def eprint(self, *args, **kwargs):
            print(self.name, *args, file=sys.stderr, **kwargs)
    
    def register(self, topic: str) -> int:
        if topic in self.ids:
            self.eprint('already registered to topic', topic)
            return -1
        try:
            res = requests.post(self.hostname + 'consumer/register', json={"topic":topic})
            if res.ok:
                try:
                    response = res.json()
                    if response['status'] == 'success':
                        self.ids[topic] = response['consumer_id']
                    else: self.eprint(response)
                except:
                    self.eprint('Invalid response:', res.text)
            else:
                self.eprint('received unexpected response code', res.status_code)
        except:
            self.eprint('Can not make a post request')
        return -1

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
    
    def dequeue_recoverable(self, topic: str):
        if topic not in self.ids:
            self.eprint('not registered for topic', topic)
            return False, -2
        
        cons_id = self.ids[topic]
        try:
            res = requests.get(self.hostname + 'consumer/consume', params={"consumer_id": cons_id, "topic": topic})
            if res.ok:
                try:
                    response = res.json()
                    if response['status'] == 'success':
                        return response['message'], response['offset'] 
                    else: self.eprint(response['message'])
                except:
                    self.eprint('Invalid response:', res.text)
            else:
                self.eprint('received unexpected response code', res.status_code)
        except:
            self.eprint('Can not make post request')
            return 'crash', -2
        return False, -2
    
    
    def poll(self, topic: str, timeout: float, poll_time:float, poll_after_error:float ,consume_log=sys.stdout):
        while time.time() < self.last_message_time + timeout:
            message = self.dequeue(topic)
            if message == False:
                time.sleep(poll_after_error)
                pass
            else:
                self.last_message_time = time.time()
                consume_log.write(message + '\n')
                time.sleep(poll_time)
    
    def poll_recoverable(self, topic: str, timeout: float, poll_time:float, poll_after_error:float ,consume_log=sys.stdout):
        good_offset = -1
        while time.time() < self.last_message_time + timeout:
            message, offset  = self.dequeue_recoverable(topic)
            if message == False:
                time.sleep(poll_after_error)
            elif message == 'crash':
                # enter recovery mode
                while not self.set_offset(topic, good_offset):
                    time.sleep(poll_after_error)
            else:
                self.last_message_time = time.time()
                if type(message) == type(True):
                    print('fucked up', message)
                else: consume_log.write(message + '\n')
                good_offset = offset
                time.sleep(poll_time)
    
    def set_offset(self, topic: str, offset) -> bool:
        if topic not in self.ids:
            self.eprint('not registered for topic', topic)
            return False
        
        cons_id = self.ids[topic]
        try:
            res = requests.post(self.hostname + 'consumer/set_offset', json={"consumer_id": cons_id, "offset": offset})
            if res.ok:
                try:
                    response = res.json()
                    if response['status'] == 'success':
                        return True
                except:
                    self.eprint('Invalid response:', res.text)
            else:
                self.eprint('received unexpected response code', res.status_code)
        except:
            self.eprint('Can not make post request')
        return False
    
    def run(self, timeout: float, poll_time=0.5, poll_after_error=5, log_folder=None):
        topic_threads = []
        files = []

        self.last_message_time = time.time()
        for t in self.ids:
            file = sys.stdout
            if log_folder is not None:
                file = open(log_folder + self.name + '_' + t + '.txt', 'w')
            thread = threading.Thread(target=self.poll, args=[t, timeout, poll_time, poll_after_error, file])
            topic_threads.append(thread)
            files.append(file)
            thread.start()
        
        for t in topic_threads:
            t.join()
        print(self.name, ': All threads timed out Stopping')
        if log_folder is not None:
            for f in files:
                f.close()
    
    def run_recoverable(self, timeout: float, poll_time=0.5, poll_after_error=5, log_folder=None):
        topic_threads = []
        files = []

        self.last_message_time = time.time()
        for t in self.ids:
            file = sys.stdout
            if log_folder is not None:
                file = open(log_folder + self.name + '_' + t + '.txt', 'w')
            thread = threading.Thread(target=self.poll_recoverable, args=[t, timeout, poll_time, poll_after_error, file])
            topic_threads.append(thread)
            files.append(file)
            thread.start()
        
        for t in topic_threads:
            t.join()
        print(self.name, ': All threads timed out Stopping')
        if log_folder is not None:
            for f in files:
                f.close()
    
    

