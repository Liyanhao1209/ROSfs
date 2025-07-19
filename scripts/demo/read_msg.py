import rosbag

bag_backend = "/data/data/outdoor.bag"

topic = ["/davis/left/image_raw"]

if __name__ == "__main__":
    bag = rosbag.Bag(bag_backend)
    for tp,msg,ts in bag.read_messages(topic):
        print(msg)
        break