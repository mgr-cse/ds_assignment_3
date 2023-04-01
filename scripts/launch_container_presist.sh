#!/bin/bash

NAME=$1
FILE=$2
shift
shift

# boot up container
docker run -itd --name $NAME --privileged -v $PWD/:$PWD/:rw localhost/ds_queue_3 /sbin/init
sleep 5

# run application
docker exec -itd $NAME /bin/bash -c "sudo -iu mattie /bin/bash -c 'cd $PWD; source 01-env/bin/activate; python $FILE $@'"

#echo stopping container
# clear container on quit
#docker stop $NAME
#docker rm $NAME