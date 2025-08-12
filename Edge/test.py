import socket,pickle,time
import zmq
import rosbag,rospy
import os

ip2port = {
    "172.17.0.2":5555
}

ex = ["/rosout","/rosout_agg"]

def get_local_ip():
    try:
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        return ip
    except Exception as e:
        print(f"Error: {e}")
        return None

if __name__ == "__main__":
    local_ip = get_local_ip()
    
    rosfs_backend = f"./{local_ip}.bag"
    
    st = None
    while st is None:
        try:
            rosfs = rosbag.Bag(rosfs_backend,'rosfs')
            st = rosfs.get_start_time()
        except Exception as e:
            time.sleep(1)
            continue
    
    print(f"st:{st}")
    print([tp.topic for tp in rosfs.get_connections() if tp.topic not in ex])
    
    try:
        rosfs = rosbag.Bag(rosfs_backend,'rosfs')
        et = rosfs.get_end_time()
        for topic,raw_msg,ts in rosfs.read_messages(
            [tp.topic for tp in rosfs.get_connections() if tp.topic not in ex],
            st,
            st+14
        ):
            print('reading')
    except Exception as e:
        print(e)