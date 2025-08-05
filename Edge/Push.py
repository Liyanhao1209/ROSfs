import socket,pickle,time
import zmq
import rosbag,rospy

ip2port = {
    "172.17.0.2":5555
}

delta_t,factor = 2,0.5

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
    
    context = zmq.Context()
    s = context.socket(zmq.PUSH)
    s.bind(f"tcp://*:{ip2port[local_ip]}")
    
    st = None
    while st is None:
        try:
            rosfs = rosbag.Bag(rosfs_backend,'rosfs')
            st = rosfs.get_start_time()
        except Exception as e:
            time.sleep(1)
            continue
    
    delta = delta_t*factor
    
    while True:
        time.sleep(delta_t)
        try:
            rosfs = rosbag.Bag(rosfs_backend,'rosfs')
            
            for topic,raw_msg,ts,conn_header in rosfs.read_messages(
                [tp.topic for tp in rosfs.get_connections() if tp.topic not in ex],
                st,
                st+delta,
                raw = True,
                return_connection_header=True
            ):
                s.send_multipart(
                    (
                        topic.encode(),
                        pickle.dumps(raw_msg),
                        pickle.dumps(ts)
                    )
                )
        except Exception as e:
            ...
        finally:
            st  += delta