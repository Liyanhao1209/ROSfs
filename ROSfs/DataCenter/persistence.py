import rosbag, genpy, genmsg
import zmq
import shutil, concurrent.futures, os, queue, pickle, threading
import signal
import sys
import rospy

edge_devices_ips = [
    "172.19.0.2",
    "172.19.0.4"
    # maybe more
]

ip2port = {
    "172.19.0.2": 5555,
    "172.19.0.4": 5556
}

ip2bag = {
    "172.19.0.2": "ground.bag",
    "172.19.0.4": "aerial.bag"
}

max_batch = 10

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
    socket.connect(f"tcp://{ip}:{ip2port[ip]}")
    print(f"connected to tcp://{ip}:{ip2port[ip]}",flush=True)
    socket.setsockopt(zmq.RCVTIMEO, 1000*60*60)

    while not stop_event.is_set():
        try:
            topic, msg, ts = socket.recv_multipart()
            # print(f"receive msg from {ip}", flush=True)
        except Exception as e:
            print(f"persis producer:{e}", flush=True)
            continue
        cq[ip].put((topic, msg, ts))
        if msg == b'kill':
            socket.close()
            break

    context.term()

# consumer
def persistence_consume(ip, stop_event, publishers):
    while not stop_event.is_set():
        if cq[ip].empty():
            continue

        topic, msg, ts = cq[ip].get()
        if msg == b'kill':
            return

        topic, raw_msg, ts = topic.decode(), pickle.loads(msg), pickle.loads(ts)
        ros_obj = deserialize_message(raw_msg)

         # Create publisher if it doesn't exist
        if topic not in publishers:
            publishers[topic] = rospy.Publisher(topic, type(ros_obj), queue_size=1000)
            print(f"Created new publisher for topic {topic}", flush=True)

        # Publish the message
        publishers[topic].publish(ros_obj)
        print(f"Published message on topic {topic} from {ip}", flush=True)

if __name__ == "__main__":
    # Initialize ROS node in the main thread
    rospy.init_node('ros_publisher', anonymous=True)

    # create edge device consumer queue
    cq = {ip: queue.Queue() for ip in edge_devices_ips}
    stop_events = {ip: threading.Event() for ip in edge_devices_ips}

    # Create publishers in the main thread
    publishers = {}
    for ip in edge_devices_ips:
        publishers[ip] = {}

    def signal_handler(sig, frame):
        for event in stop_events.values():
            event.set()
        print('Exiting gracefully...', flush=True)
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(edge_devices_ips) * 2) as executor:
        futures = []
        for ip in edge_devices_ips:
            futures.append(executor.submit(persistence_prod, ip, stop_events[ip]))
            futures.append(executor.submit(persistence_consume, ip, stop_events[ip], publishers[ip]))

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Thread failed with {e}", flush=True)