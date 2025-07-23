import rosbag
import zmq,msgpack
import shutil,concurrent.futures,os

edge_devices_ips = [
    "192.168.10.53",
    "192.168.10.54"
    # maybe more
]

# mapping ip to temporary port on linux(i.e. 49152~65535)
# identical
def ip2port(ip:str) -> int:
    a, b, c, d = map(int, ip.split('.'))
    h = (a << 24) | (b << 16) | (c << 8) | d
    return 49152 + (h * 2654435761 & 0xFFFF) % (65535 - 49152 + 1)

# deserialize bytes array(ROS raw msg data) from edge device
def deserialize_message(raw_msg):
    index, serialized_message, md5, pos, msg_class = raw_msg
    ros_msg = msg_class()
    ros_msg.deserialize(serialized_message)
    return ros_msg

def persistence(ip:str,context):
    # push/pull model
    socket = context.socket(zmq.PULL)
    socket.connect(f"tcp://{ip}:{ip2port(ip)}")
    socket.setsockopt(zmq.RCVTIMEO, 1000*60*60)
    
    # create rosfs backend
    rosfs_backend = f"./{ip}.bag"
    try:
        if os.path.exists(rosfs_backend):
            shutil.rmtree(rosfs_backend)
        rosbag.rosfs_timekv.create(rosfs_backend)
        rosfs = rosbag.Bag(rosfs_backend,'rosfs')
    except Exception as e:
        print(e)
        exit(-1)

    while True:
        topic,msg,ts,conn_header = socket.recv_multipart()
        if msg==b'kill':
            socket.close()
            break
        topic,ts,conn_header = topic.decode(),int(ts.decode()),msgpack.unpackb(conn_header)
        ros_obj = deserialize_message(msg)
        try:
            rosfs.rosfs_batch_write([topic],[ros_obj],[ts],connection_header = [conn_header])
        except Exception as e:
            print(e)
            socket.close()
            exit(-1)

if __name__ == "__main__":
    context = zmq.Context()

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(edge_devices_ips)) as executor:
        futures = [executor.submit(persistence, ip, context) for ip in edge_devices_ips]

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Thread failed with exception: {e}")

    context.term()