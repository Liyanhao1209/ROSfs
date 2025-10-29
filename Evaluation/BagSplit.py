import rospy,rosbag
from cv_bridge import CvBridge
import os,time
import argparse,cv2,numpy as np
from geometry_msgs.msg import Pose

import tf

ap = "/aerial/pose"
ai = "/aerial/rgb_image"

vp = "/vehicle/pose"
vi = "/vehicle/rgb_image"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", "-d", type=str, nargs = 1, help="dataset path,including image and pose subdir", required=True)
    parser.add_argument("--output",'-o', type = str, nargs=1, help="output bag file path", required=True)
    parser.add_argument("--ttime",'-tt',type = int , nargs = 1 ,help = "total data collection time" , required=True)
    parser.add_argument('--partition','-p',type = int , nargs = 1 ,help="sub domain quantities", required=True)
    
    args = parser.parse_args()
    dataset = args.dataset[0]
    output = args.output[0]
    totaltime = args.ttime[0]
    partition = args.partition[0]
    
    bridge = CvBridge()
    
    aerial_img_pth = os.path.join(dataset,"aerial/image")
    aerial_pose_pth = os.path.join(dataset,"aerial/pose")
    vehicle_img_pth = os.path.join(dataset,"street/image")
    vehicle_pose_pth = os.path.join(dataset,"street/pose")
    
    def match_img_pose(img_pth,pose_pth):
        msg_buffer = []
        for img in os.listdir(img_pth):
            if not img.endswith(".png"):
                continue
            fn,extension = os.path.splitext(img)
            
            cv_img = cv2.imread(os.path.join(img_pth,img),cv2.IMREAD_COLOR)
            ros_img_msg = bridge.cv2_to_imgmsg(cv_img,encoding='bgr8')
            ros_img_msg.header.stamp = None
            ros_img_msg.header.frame_id = ""
            
            pose = np.load(os.path.join(pose_pth,f'{fn}.npy'))
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
            
            msg_buffer.append((ros_img_msg,pose_msg))
        
        return msg_buffer
    
    b1,b2 = match_img_pose(aerial_img_pth,aerial_pose_pth),match_img_pose(vehicle_img_pth,vehicle_pose_pth)
    p1,p2 = len(b1)//partition,len(b2)//partition
    step1,step2 = totaltime/len(b1),totaltime/len(b2)

    for i in range(partition):
        if not os.path.exists(output):
            os.makedirs(output)
        
        new_container_pth = os.path.join(output,f'GroundAir_split_{i+1}.bag')
        rosbag.rosfs_timekv.create(new_container_pth)
        with rosbag.Bag(new_container_pth,'rosfs') as rosfs:
            def write(handler,cnt,start_time,buffer,step,tp1,tp2,base):
                for i in range(cnt):
                    m1, m2 = buffer.pop()
                    # Calculate the time offset in seconds
                    offset_seconds = base * cnt * step + (i+1) * step
                    # Convert to a duration
                    duration = rospy.Duration(offset_seconds)
                    stamp = start_time + duration
                    m1.header.stamp = stamp
                    handler.write(tp1,m1,stamp)
                    handler.write(tp2,m2,stamp)
            
            start_time = rospy.Time.from_sec(time.time())
            write(rosfs,p1,start_time,b1,step1,ai,ap,i)
            write(rosfs,p2,start_time,b2,step2,vi,vp,i)