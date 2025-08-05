import rosbag
import rosbag.rosfs_timekv
import rospy
import shutil

def deserialize_message(raw_msg):
    index, serialized_message, md5, pos, msg_class = raw_msg
    ros_msg = msg_class()
    ros_msg.deserialize(serialized_message)
    return ros_msg

if __name__ == "__main__":
    bag_backend = "/data/GroundAir/scripts/target/ga.bag"
    topic1 = "/aerial/rgb_image"
    topic2 = "/vehicle/rgb_image"

    bag = rosbag.Bag(bag_backend)
    raw1,raw2 = None,None
    t1,t2 = None,None

    for tp, m, t in bag.read_messages(topics=[topic1], raw=True):
        raw1 = m
        t1 = t
        break
    
    for tp, m, t in bag.read_messages(topics=[topic2], raw=True):
        raw2 = m
        t2 = t
        break

    o1,o2 = deserialize_message(raw1),deserialize_message(raw2)
    rosfs_target = "./test.bag"
    try:
        shutil.rmtree(rosfs_target)
    except Exception as e:
        print(e)
        
    rosbag.rosfs_timekv.create(rosfs_target)
    rosfs = rosbag.Bag(rosfs_target,'rosfs')
    rosfs.rosfs_batch_write([topic1],[o1],[t1],connection_headers = [None])
    rosfs.rosfs_batch_write([topic2],[o2],[t2],connection_headers = [None])