#!/bin/bash

source config.sh

start_server() {
    local i=$1
    local container_name="robot_node_$i"

    docker exec "$container_name" bash -c "source /root/ros_catkin_ws/devel/setup.bash && cd /workspace/ROSfs/Edge && python3 ./server.py > ./${container_name}_serverlog.txt 2>&1" &
}

for ((i = 1; i < cluster_size; i++)); do
    start_server "$i" &
done

wait

echo "Done"