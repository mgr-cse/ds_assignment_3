#!/bin/bash

# creating required topics fot testing
curl -XPOST "http://localhost:5000/topics" -d '{"topic_name": "T-1"}' -H "Content-Type: application/json"
curl -XPOST "http://localhost:5000/topics" -d '{"topic_name": "T-2"}' -H "Content-Type: application/json"
curl -XPOST "http://localhost:5000/topics" -d '{"topic_name": "T-3"}' -H "Content-Type: application/json"

python producer_process.py P-1 T-1,T-2,T-3 test_asgn1/producer_1.txt &
python producer_process.py P-2 T-1,T-3 test_asgn1/producer_2.txt &
python producer_process.py P-3 T-1 test_asgn1/producer_3.txt 2>&1 &
python producer_process.py P-4 T-2 test_asgn1/producer_4.txt 2>&1 &
python producer_process.py P-5 T-2 test_asgn1/producer_5.txt 2>&1 &

python consumer_process.py C-1 T-1,T-2,T-3 &
python consumer_process.py C-2 T-1,T-3 &
python consumer_process.py C-3 T-1,T-3 &

wait

python tests/verify.py