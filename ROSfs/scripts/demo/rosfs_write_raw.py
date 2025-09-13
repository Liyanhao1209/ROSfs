import rosbag
import rosbag.rosfs_timekv
import shutil

bag_backend = "/data/data/outdoor.bag"
target = './test.bag'
topic = ["/davis/left/image_raw"]

if __name__ == "__main__":
    bag = rosbag.Bag(bag_backend)
    tp,msg,ts,ch = None,None,None,None
    for topic,m,t,conn_header in bag.read_messages(raw=True,return_connection_header=True):
        tp,msg,ts,ch = topic,m,t,ch
        break
    # bag = rosbag.Bag('test.bag','w')
    # bag.write(tp,msg,ts,raw=True,connection_header=ch)
    try:
        shutil.rmtree(target)
    except Exception as e:
        ...

    try:
        rosbag.rosfs_timekv.create(target)
    except Exception as e:
        ...
    rosfs = rosbag.Bag(target,"rosfs")

    rosfs.rosfs_batch_write([tp],[msg],[ts],raw=True,connection_headers=[ch])