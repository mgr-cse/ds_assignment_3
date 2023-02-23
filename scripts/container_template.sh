#!/bin/bash

# boot up container
docker run -itd --name broker_manager --ip 172.17.0.2 --privileged -v $PWD/:$PWD/:rw localhost/ds_queue_host /sbin/init
sleep 10

# run application
docker exec -it broker_manager /bin/bash -c "sudo -iu mattie /bin/bash -c 'cd source/repos/ds_assignment_2; source 01-env/bin/activate; python broker_manager/write_manager.py'"

# clear container on quit
docker stop broker_manager
docker rm broker_manager