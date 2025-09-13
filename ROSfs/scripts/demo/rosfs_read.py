import rosbag

if __name__ == "__main__":
    rosfs = rosbag.Bag("/workspace/target/aerial.bag",'rosfs')
    for topic,raw_msg,ts,conn_header in rosfs.read_messages(
            [tp.topic for tp in rosfs.get_connections() if tp.topic],
            rosfs.get_start_time(),
            rosfs.get_end_time()+1,
            raw = True,
            return_connection_header=True
    ):
        print(topic,ts) 