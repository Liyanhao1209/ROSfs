#!/bin/bash

source config.sh

# 定义一个函数，用于在单个容器中执行 git pull 操作
git_pull_container() {
    local container_name="robot_node_$1"

    if docker inspect "$container_name" &>/dev/null; then
        
        # 执行 rosbag 的 git pull
        docker exec "$container_name" bash -c "cd $rosbag_workdir && git reset --hard && git pull origin $rosbag_branch"
        if [ $? -eq 0 ]; then
            echo "rosbag pull completed successfully in $container_name"
        else
            echo "Failed to execute rosbag git pull in $container_name" >&2
        fi

        # 执行 rosbag_storage 的 git pull
        docker exec "$container_name" bash -c "cd $rosbag_storage_workdir && git reset --hard && git pull origin $rosbag_storage_branch"
        if [ $? -eq 0 ]; then
            echo "rosbag_storage pull completed successfully in $container_name"
        else
            echo "Failed to execute rosbag_storage git pull in $container_name" >&2
        fi
    else
        echo "Container $container_name does not exist. Skipping..." >&2
    fi
}

# 定义一个函数，用于在单个容器中执行 catkin build 操作
catkin_build_container() {
    local container_name="robot_node_$1"

    if docker inspect "$container_name" &>/dev/null; then
        # echo "Executing catkin build in container: $container_name"
        docker exec "$container_name" bash -c "cd $rosbag_workdir && catkin build --this" > /dev/null 2>&1
        if [ $? -eq 0 ]; then
            echo "catkin build completed successfully in $container_name"
        else
            echo "Failed to execute catkin build in $container_name" >&2
        fi
    else
        echo "Container $container_name does not exist. Skipping..." >&2
    fi
}

# 并行处理所有容器的 git pull 操作，每次并发 5 个
for ((i=1; i<=cluster_size; i+=5)); do
    for ((j=i; j<i+5 && j<=cluster_size; j++)); do
        git_pull_container "$j" &
    done
    wait  # 等待当前批次的 5 个容器完成 git pull
done

echo "All git pull operations have been completed."


for ((i=1; i<=cluster_size; i++)); do
    catkin_build_container "$i" &
done

wait

echo "All catkin build operations have been completed."