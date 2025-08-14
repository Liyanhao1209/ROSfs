#!/bin/bash

source config.sh

cleanup() {
    echo "Cleaning up processes..."
    for ((i = 1; i < cluster_size; i++)); do
        local container_name="robot_node_$i"
        docker exec "$container_name" bash -c "lsof -i :11311 | awk 'NR>1 {print \$2}' | xargs -r kill -9"
        docker exec "$container_name" bash -c "lsof -i :5555 | awk 'NR>1 {print \$2}' | xargs -r kill -9"
        docker exec "$container_name" bash -c "lsof -i :5556 | awk 'NR>1 {print \$2}' | xargs -r kill -9"
    done

    local server_name="robot_node_$cluster_size"
    docker exec "$server_name" bash -c "ps aux | grep 'python' | awk '{print \$2}' | xargs -r kill -9"
    echo "Cleanup done."
}

cleanup