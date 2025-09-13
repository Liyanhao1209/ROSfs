import socket,pickle,time,os
import zmq
import rosbag

os.environ['PYTHONUNBUFFERED'] = '1'

ip2port = {
    "172.19.0.2": 5555,
    "172.19.0.4": 5556
}

ip2bag = {
    "172.19.0.2":"ground.bag",
    "172.19.0.4":"aerial.bag"
}

delta_t,factor = 2,0.5
max_retry = 10

ex = ["/rosout","/rosout_agg"]

def get_local_ip():
    try:
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        return ip
    except Exception as e:
        print(f"Error: {e}")
        return None
    
def read_and_send(rosfs_handler,st,et):
    try:
        for topic,raw_msg,ts,conn_header in rosfs_handler.read_messages(
            [tp.topic for tp in rosfs.get_connections() if tp.topic not in ex],
            st,
            et,
            raw = True,
            return_connection_header=True
        ):
            if topic in ex:
                continue
            s.send_multipart(
                (
                    topic.encode(),
                    pickle.dumps(raw_msg),
                    pickle.dumps(ts)
                )
            )
            
            print(f"send:{topic},{ts}",flush=True)
    except Exception as e:
        print(e,flush=True)

if __name__ == "__main__":
    local_ip = get_local_ip()
    
    rosfs_backend = f"/workspace/target/{ip2bag[local_ip]}"
    print(rosfs_backend,flush=True)
    
    context = zmq.Context()
    s = context.socket(zmq.PUSH)
    s.bind(f"tcp://*:{ip2port[local_ip]}")
    
    st = None
    while st is None:
        try:
            rosfs = rosbag.Bag(rosfs_backend,'rosfs')
            st = rosfs.get_start_time()
        except Exception as e:
            # print(e,flush=True)
            time.sleep(1)
            continue
    
    delta = delta_t*factor
    retry = 0
    while True:
        try:
            rosfs = rosbag.Bag(rosfs_backend,'rosfs')
            et = rosfs.get_end_time()
            if st>=et:
                time.sleep(delta_t)
                retry += 1
                if retry > max_retry:
                    break
                continue
            else:
                retry = 0
            
            read_and_send(rosfs,st,et)
            
        except Exception as e:
            print(e,flush=True)
        finally:
            st = et
    
    # last read
    print("max retry,last read",flush=True)
    try:
        rosfs = rosbag.Bag(rosfs_backend,'rosfs')
        et = rosfs.get_end_time()
        
        read_and_send(rosfs,st,et+1)
    except Exception as e:
        ...

    # close session
    try:
        s.send_multipart([
            b'kill',b'kill',b'kill'
        ])
        print('kill session',flush=True)
        s.close()
    except Exception as e:
        print(e)