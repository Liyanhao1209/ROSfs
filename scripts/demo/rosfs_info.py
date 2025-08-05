import rosbag

if __name__ == "__main__":
    rosfs = rosbag.Bag("../target/test.bag","rosfs")
    # print(type(rosfs.get_end_time()))
    print(type(rosfs.get_type_and_topic_info()))
    
    for item in rosfs.get_connections():
        print(item.topic)