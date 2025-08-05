#!/bin/bash
python3 ./DataCenter/persistence.py > persis.txt 2>&1 &

sleep 1

python3 ./Edge/Push.py > edge_push.txt 2>&1 &

wait