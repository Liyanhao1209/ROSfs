import rosbag

# bag_backend = "/data/data/outdoor.bag"
bag_backend = "/data/GroundAir/scripts/target/ga.bag"

# topic = ["/davis/left/image_raw"]
topic = ["/aerial/rgb_image"]

if __name__ == "__main__":
    bag = rosbag.Bag(bag_backend)
    for tp,msg,ts in bag.read_messages(topic,raw=True):
        print(msg)
        break