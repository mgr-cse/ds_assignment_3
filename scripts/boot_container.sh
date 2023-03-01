#!/bin/bash

NAME=$1
FILE=$2
shift
shift

docker start $NAME
sleep 5
docker exec -itd $NAME /bin/bash -c "sudo -iu mattie /bin/bash -c 'cd $PWD; source 01-env/bin/activate; python $FILE $@'"
