import rospy,rosbag
from cv_bridge import CvBridge
import os,json,time
import argparse,cv2,tqdm

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", "-d", type=str, nargs = "+", help="dataset path", required=True)
    parser.add_argument("--output",'-o', type = str, nargs=1, help="output bag file path", required=True)
    
    args = parser.parse_args()
    datasets = args.dataset
    output = args.output[0]
    
    bridge = CvBridge()
    msg_buffer = []
    
    totcnt = 0
    for dpth in datasets:
        with open(os.path.join(dpth,"config.json"),'r',encoding='utf-8') as cfile:
            config  = json.load(cfile)
        for f in sorted(os.listdir(dpth)):
            assert type(f)==str
            if f.endswith(".png"):
                cv_img = cv2.imread(os.path.join(dpth,f),cv2.IMREAD_COLOR)
                ros_img_msg = bridge.cv2_to_imgmsg(cv_img,encoding='bgr8')
                ros_img_msg.header.stamp = rospy.Time.from_sec(time.time())
                ros_img_msg.header.frame_id = config['frame_id']
                msg_buffer.append((config['topic_name'],ros_img_msg))
                totcnt += 1
    
    print(f'total msg count: {totcnt}')
    tqdmbar = tqdm.tqdm(total=totcnt,desc='Writing RGB image')
    with rosbag.Bag(output,'w') as bag:
        for tp,msg in msg_buffer:
            bag.write(tp,msg,msg.header.stamp)
            tqdmbar.update(1)