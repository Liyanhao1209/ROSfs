#!/bin/bash

source config.sh

play_record() {
    local i=$1
    local bag_file=$2
    local target_file=$3
    local container_name="robot_node_$i"

    echo "Executing commands in container: $container_name with bag file: $bag_file and target file: $target_file"

    docker exec "$container_name" bash -c "source /root/ros_catkin_ws/devel/setup.bash && roscore" > /dev/null 2>&1 &
    sleep 2

    docker exec "$container_name" bash -c "source /root/ros_catkin_ws/devel/setup.bash && python3 /workspace/ROSfs/Edge/Push.py > /workspace/ROSfs/Edge/${container_name}_pushlog.txt 2>&1" &
    sleep 2

    docker exec "$container_name" bash -c "source /root/ros_catkin_ws/devel/setup.bash && rosbag record -a --rosfs -O $target_file" > ./log/${container_name}_recordlog.txt 2>&1 &
    sleep 2

    docker exec "$container_name" bash -c "source /root/ros_catkin_ws/devel/setup.bash && rosbag play $bag_file" > ./log/${container_name}_playlog.txt 2>&1 &
}

persis_and_read() {
    local i=$1
    local container_name="robot_node_$i"

    docker exec "$container_name" bash -c "source /root/ros_catkin_ws/devel/setup.bash && roscore" > /dev/null 2>&1 &
    sleep 1
    docker exec "$container_name" bash -c "source /root/ros_catkin_ws/devel/setup.bash && rosbag record -a --rosfs -O /workspace/ROSfs/DataCenter/record.bag > /workspace/ROSfs/DataCenter/${container_name}_recordlog.txt 2>&1" &
    sleep 1
    docker exec "$container_name" bash -c "source /root/ros_catkin_ws/devel/setup.bash && python3 /workspace/ROSfs/DataCenter/persistence.py > /workspace/ROSfs/DataCenter/${container_name}_persislog.txt 2>&1" &
    # docker exec "$container_name" bash -c "source /root/ros_catkin_ws/devel/setup.bash && python3 /workspace/ROSfs/DataCenter/reader.py > /workspace/ROSfs/DataCenter/${container_name}_readlog.txt 2>&1" &
}

bag_files=(
    "/workspace/data/ground.bag"
    "/workspace/data/aerial.bag"
)

target_files=(
    "/workspace/target/ground.bag"
    "/workspace/target/aerial.bag"
)

cleanup() {
    echo "Cleaning up processes..."
    for ((i = 1; i <= cluster_size; i++)); do
        local container_name="robot_node_$i"
        docker exec "$container_name" bash -c "lsof -i :11311 | awk 'NR>1 {print \$2}' | xargs -r kill -9"
        docker exec "$container_name" bash -c "lsof -i :5555 | awk 'NR>1 {print \$2}' | xargs -r kill -9"
        docker exec "$container_name" bash -c "lsof -i :5556 | awk 'NR>1 {print \$2}' | xargs -r kill -9"
    done

    # local server_name="robot_node_$cluster_size"
    # docker exec "server_name" bash -c "ps aux | grep 'python' | awk '{print \$2}' | xargs -r kill -9"
    echo "Cleanup done."
}

cleanup

trap cleanup SIGINT

persis_and_read "$cluster_size" &
sleep 2

for ((i = 1; i < cluster_size; i++)); do
    bag_file=${bag_files[$((i - 1))]}
    target_file=${target_files[$((i - 1))]}
    play_record "$i" "$bag_file" "$target_file" &
done

wait

echo "Done"