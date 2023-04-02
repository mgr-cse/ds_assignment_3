#!/bin/bash

source 01-env/bin/activate

LOG_DIR=./tests/logs
mkdir -p $LOG_DIR
mkdir -p $LOG_DIR/producer
mkdir -p $LOG_DIR/consumer

./tests/network_up.sh
sleep 6

# start producers
python ./producer/producer.py P-1 T-1,T-2,T-3 -1,2,3 2> $LOG_DIR/producer/P-1.err | tee $LOG_DIR/producer/P-1.log &
python ./producer/producer.py P-2 T-1,T-3 1,3 2> $LOG_DIR/producer/P-2.err | tee $LOG_DIR/producer/P-2.log &
python ./producer/producer.py P-3 T-1 1 2> $LOG_DIR/producer/P-3.err | tee $LOG_DIR/producer/P-3.log &
python ./producer/producer.py P-4 T-2 -1 2> $LOG_DIR/producer/P-4.err | tee $LOG_DIR/producer/P-4.log &
python ./producer/producer.py P-5 T-2 -1 2> $LOG_DIR/producer/P-5.err | tee $LOG_DIR/producer/P-5.log &

# start consumers
pids=""
python ./consumer/consumer.py C-1 T-1,T-2,T-3 -1,-1,-1 2> $LOG_DIR/consumer/C-1.err | tee $LOG_DIR/consumer/C-1.log &
pids="$! $pids"
python ./consumer/consumer.py C-2 T-1,T-3 1,3 2> $LOG_DIR/consumer/C-2.err | tee $LOG_DIR/consumer/C-2.log &
pids="$! $pids"
python ./consumer/consumer.py C-3 T-1,T-3 -1,3 2> $LOG_DIR/consumer/C-3.err | tee $LOG_DIR/consumer/C-3.log &
pids="$! $pids"

kill_consumers () {
    echo 'killing consumer scripts!'
    for p in $pids; do
        echo $p
        kill -INT $p
    done
}

trap 'kill_consumers' 2
echo '++++ press ctrl+C to exit'
wait
