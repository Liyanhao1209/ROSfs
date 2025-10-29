import rosbag

# bag_backend = "/data/data/outdoor.bag"
bag_backend = "/data/GroundAir/scripts/vehicle.bag"

# topic = ["/davis/left/image_raw"]
topic = ["/vehicle/pose"]

if __name__ == "__main__":
    bag = rosbag.Bag(bag_backend)
    for tp,m,ts,conn_header in bag.read_messages(raw = False,return_connection_header=True):
        print(topic,type(m))
        print('---------------')