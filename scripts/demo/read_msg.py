import rosbag,json,msgpack,pickle

# bag_backend = "/data/data/outdoor.bag"
bag_backend = "/data/GroundAir/scripts/aerial.bag"

# topic = ["/davis/left/image_raw"]
topic = ["/aerial/pose"]

if __name__ == "__main__":
    bag = rosbag.Bag(bag_backend)
    for tp,m,ts,conn_header in bag.read_messages(topic,raw = False,return_connection_header=True):
        print(m)
        print('---------------')