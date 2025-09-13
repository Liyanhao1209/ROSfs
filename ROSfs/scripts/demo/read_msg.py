import rosbag,json,msgpack,pickle

# bag_backend = "/data/data/outdoor.bag"
bag_backend = "/data/GroundAir/scripts/target/ga_container.bag"

# topic = ["/davis/left/image_raw"]
topic = ["/aerial/rgb_image"]

if __name__ == "__main__":
    bag = rosbag.Bag(bag_backend,'rosfs')
    msg = None
    for tp,m,ts,conn_header in bag.read_messages(topic,raw = True,return_connection_header=True):
        msg = m
        break
    print(type(msg))
    se = pickle.dumps(msg)
    print(type(se))
    des = pickle.loads(se)
    print(type(des))
    
    print(msg[1])
    print(type(msg[1]))
    print(type(des[1]))
    print(des[1])
    
    print(type(ts))
    se = pickle.dumps(ts)
    print(type(se))
    des = pickle.loads(se)
    print(type(des))
    
    print(type(conn_header))
    se = pickle.dumps(conn_header)
    print(type(se))
    des = pickle.loads(se)
    print(type(des))
    
    print(conn_header)