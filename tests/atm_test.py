import os
import sys

sys.path.append(os.getcwd())

import subprocess

atm_count = 3
if len(sys.argv) > 1:
    try:
        atm_count = int(sys.argv[1])
    except:
        pass

ip = "0.0.0.0"
base_port = 4001

ports = range(base_port, base_port + atm_count)

for host in ports:
    base_command = ['gnome-terminal', '--', 'bash', '-c']
    command = f'source 01-env/bin/activate; echo atm on {ip}:{host}; python atm/atm.py {ip}:{host}'
    
    for partner in ports:
        if partner == host:
            continue
        command += f' {ip}:{partner}'
    base_command.append(command)
    subprocess.Popen(base_command)

    