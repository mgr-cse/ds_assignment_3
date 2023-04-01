#!/bin/bash

IMAGE_NAME=localhost/ds_queue_3

docker build -t $IMAGE_NAME -f scripts/dockerfile.base ./ --build-arg path=$PWD
docker run -itd --privileged --name building_container $IMAGE_NAME /sbin/init
sleep 10
docker exec -it building_container /bin/bash -c "apt-get install -y postgresql;\
systemctl enable postgresql;\
systemctl start postgresql;\
sudo -iu postgres psql < /create_database.sql"

docker stop building_container
docker commit building_container $IMAGE_NAME
docker rm building_container
mkdir -p 02-image_backups
docker save $IMAGE_NAME -o 02-image_backups/ds_queue_3.tar
