#!/bin/bash

NAME=$1
IP=$2
FILE=$3

# boot up container
docker run -itd --name $NAME --ip $IP --privileged -v $PWD/:$PWD/:rw localhost/ds_queue_host /sbin/init
sleep 10

# run application
docker exec -it $NAME /bin/bash -c "sudo -iu mattie /bin/bash -c 'cd source/repos/ds_assignment_2; source 01-env/bin/activate; python $FILE'"

# clear container on quit
docker stop $NAME
docker rm $NAME