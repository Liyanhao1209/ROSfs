import rosbag, genpy, genmsg
import zmq
import shutil, concurrent.futures, os, queue, pickle,threading
import signal
import sys

edge_devices_ips = [
    "172.17.0.2",
    # "192.168.10.54"
    # maybe more
]

ip2port = {
    "172.17.0.2": 5555
}

class ROSBagException(Exception):
    """
    Base class for exceptions in rosbag.
    """
    def __init__(self, value=None):
        self.value = value
        self.args = (value,)

    def __str__(self):
        return self.value

def get_message_type(info):
    try:
        message_type = genpy.dynamic.generate_dynamic(info.datatype, info.msg_def)[info.datatype]
        if (message_type._md5sum != info.md5sum):
            print('WARNING: For type [%s] stored md5sum [%s] does not match message definition [%s].\n  Try: "rosrun rosbag fix_msg_defs.py old_bag new_bag."' % (info.datatype, info.md5sum, message_type._md5sum), file=sys.stderr)
    except genmsg.InvalidMsgSpec:
        message_type = genpy.dynamic.generate_dynamic(info.datatype, "")[info.datatype]
        print('WARNING: For type [%s] stored md5sum [%s] has invalid message definition."' % (info.datatype, info.md5sum), file=sys.stderr)
    except genmsg.MsgGenerationException as ex:
        raise ROSBagException('Error generating datatype %s: %s' % (info.datatype, str(ex)))
    return message_type
    
# deserialize bytes array(ROS raw msg data) from edge device
def deserialize_message(raw_msg):
    raw_data, conn_info = raw_msg
    msg_class = get_message_type(conn_info)
    ros_msg = msg_class()
    ros_msg.deserialize(raw_data)
    return ros_msg

# producer
def persistence_prod(ip, stop_event):
    context = zmq.Context()
    # push/pull model
    socket = context.socket(zmq.PULL)
    socket.connect(f"tcp://{ip}:5555")
    socket.setsockopt(zmq.RCVTIMEO, 1000*60*60)

    while not stop_event.is_set():
        try:
            topic, msg, ts = socket.recv_multipart()
        except Exception as e:
            print(f"persis producer:{e}")
            continue
        cq[ip].put((topic, msg, ts))
        if msg == b'kill':
            socket.close()
            break
    
    context.term()

# consumer
def persistence_consume(ip, stop_event):
    # create rosfs backend
    rosfs_backend = f"./{ip}.bag"
    try:
        if os.path.exists(rosfs_backend):
            shutil.rmtree(rosfs_backend)
        rosbag.rosfs_timekv.create(rosfs_backend)
        rosfs = rosbag.Bag(rosfs_backend, 'rosfs')
    except Exception as e:
        print(f"persis consumer:{e}")
        return
    
    tps, objs, tss, conns = [], [], [], []
    while not stop_event.is_set():
        tps.clear()
        objs.clear()
        tss.clear()
        conns.clear()
        flag = False
        while not cq[ip].empty():
            topic, msg, ts = cq[ip].get()
            if msg == b'kill':
                flag = True
                break
            topic, raw_msg, ts = topic.decode(), pickle.loads(msg), pickle.loads(ts)
            ros_obj = deserialize_message(raw_msg)
            tps.append(topic)
            objs.append(ros_obj)
            tss.append(ts)
            conns.append(None)
        if not len(tps) == 0:
            try:
                rosfs.rosfs_batch_write(tps, objs, tss, connection_headers=conns)
            except Exception as e:
                print(e)
        if flag:
            return

if __name__ == "__main__":
    # create edge device consumer queue
    cq = {ip: queue.Queue() for ip in edge_devices_ips}
    stop_events = {ip: threading.Event() for ip in edge_devices_ips}

    def signal_handler(sig, frame):
        for event in stop_events.values():
            event.set()
        print('Exiting gracefully...')
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(edge_devices_ips) * 2) as executor:
        futures = []
        for ip in edge_devices_ips:
            futures.append(executor.submit(persistence_prod, ip, stop_events[ip]))
            futures.append(executor.submit(persistence_consume, ip, stop_events[ip]))

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Thread failed with {e}")