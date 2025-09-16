import rosbag

bag_backend = "/data/GroundAir/Edge/172.17.0.2.bag"
# bag_backend = "/data/GroundAir/scripts/target/ga_container.bag"

topic = ["/aerial/pose"]
# topic = ["/aerial/rgb_image"]

if __name__ == "__main__":
    bag = rosbag.Bag(bag_backend,'rosfs')
    # print(bag.get_message_count(topic))
    # for tp,m,ts,conn_header in bag.read_messages_by_id(topic,0,10,raw=False,return_connection_header=True):
    #     print(tp,ts)
    #     print('-----------------')
    for tp,m,ts,conn_header in bag.read_messages_by_id(topic,10,10,raw=False,return_connection_header=True):
        ...
        # print(tp,ts)
        # print('-----------------')