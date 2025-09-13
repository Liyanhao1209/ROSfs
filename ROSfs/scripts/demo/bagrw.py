#!/usr/bin/env python
import rosbag

def extract_and_write_raw_data(input_bag_path, output_bag_path, topic_name):
    """
    从输入的bag文件中读取指定主题的第一条原始数据（raw data），并将其写入新的bag文件中。

    :param input_bag_path: 输入的bag文件路径
    :param output_bag_path: 输出的bag文件路径
    :param topic_name: 想要提取消息的主题名称
    """
    with rosbag.Bag(input_bag_path, 'r') as input_bag:
        for topic, msg, t, conn_header in input_bag.read_messages(topics=[topic_name], raw=True,return_connection_header=True):
            with rosbag.Bag(output_bag_path, 'w') as output_bag:
                output_bag.write(topic, msg, t, raw=True, connection_header=conn_header)
            print(f"成功提取并写入原始数据到 {output_bag_path}")
            return  

    print(f"未在主题 {topic_name} 中找到任何消息")

if __name__ == "__main__":
    input_bag_path = "/data/data/do.bag"  
    output_bag_path = "./rw.bag"  
    topic_name = "/camera/rgb/image_color"  

    extract_and_write_raw_data(input_bag_path, output_bag_path, topic_name)