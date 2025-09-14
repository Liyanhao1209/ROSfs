import rospy,rosbag
from cv_bridge import CvBridge
import os,json,time
import argparse,cv2,tqdm,numpy as np
from geometry_msgs.msg import Pose

import tf

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", "-d", type=str, nargs = "+", help="dataset path,including image and pose subdir", required=True)
    parser.add_argument("--output",'-o', type = str, nargs=1, help="output bag file path", required=True)
    
    args = parser.parse_args()
    datasets = args.dataset
    output = args.output[0]
    
    bridge = CvBridge()
    msg_buffer = []
    
    totcnt = 0
    for dpth in datasets:
        with open(os.path.join(dpth,'image',"config.json"),'r',encoding='utf-8') as icfile:
            image_config  = json.load(icfile)
        with open(os.path.join(dpth,'pose','config.json'),'r',encoding='utf-8') as pcfile:
            pose_config = json.load(pcfile)
        
        for f in sorted(os.listdir(os.path.join(dpth,'image'))):
            assert type(f)==str
            if f.endswith(".png"):
                fn,extension = os.path.splitext(f)
                
                image_time = rospy.Time.from_sec(time.time())
                cv_img = cv2.imread(os.path.join(dpth,'image',f),cv2.IMREAD_COLOR)
                ros_img_msg = bridge.cv2_to_imgmsg(cv_img,encoding='bgr8')
                ros_img_msg.header.stamp = image_time
                ros_img_msg.header.frame_id = image_config['frame_id']
                msg_buffer.append((image_config['topic_name'],ros_img_msg,image_time))
                totcnt += 1
                
                pose = np.load(os.path.join(dpth,'pose',f"{fn}.npy"))
                pose_time = rospy.Time.from_sec(time.time())
                homogeneous_matrix = pose
    
                position = homogeneous_matrix[:3, 3]
                rotation_matrix = homogeneous_matrix[:3, :3]
                quaternion = tf.transformations.quaternion_from_matrix(homogeneous_matrix)
                
                pose_msg = Pose()
                pose_msg.position.x = position[0]
                pose_msg.position.y = position[1]
                pose_msg.position.z = position[2]

                pose_msg.orientation.x = quaternion[0]
                pose_msg.orientation.y = quaternion[1]
                pose_msg.orientation.z = quaternion[2]
                pose_msg.orientation.w = quaternion[3]
                msg_buffer.append((pose_config['topic_name'],pose_msg,pose_time))
                
                totcnt += 1
                
                
    print(f'total msg count: {totcnt}')
    tqdmbar = tqdm.tqdm(total=totcnt,desc='Writing RGB image')
    with rosbag.Bag(output,'w') as bag:
        for tp,msg,ts in msg_buffer:
            bag.write(tp,msg,ts)
            tqdmbar.update(1)