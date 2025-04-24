import sys
import rosbag
import time

if __name__ == "__main__":
    args = sys.argv
    
    src = args[1]
    target = args[2]
    
    print("ROSfs converting")
    st = time.time()
    try:
        rosbag.rosfs_timekv.create(target)
        
        rosfs_bag = rosbag.Bag(target,"rosfs")
        bag = rosbag.Bag(src)
        
        topics,msgs,ts,connection_headers = [],[],[],[]
        for topic,msg,t,conn_header in bag.read_messages(return_connection_header=True):
            for k,v in conn_header.items():
                if isinstance(v,bytes):
                    conn_header[k] = v.decode('utf-8')
            rosfs_bag.rosfs_batch_write(topics,msgs,ts,connection_headers)
    except Exception as e:
        print(f"Exception while converting:{e}")
    finally:
        bag.close()
    et = time.time()
    print(f"ROSfs convertion complete,time use:{et-st}")