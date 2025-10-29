import zmq,os
import threading,pickle

from cv_bridge import CvBridge
import cv2

import tf
import numpy as np

import rosbag

global images,poses

def push_thread():
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.bind("tcp://*:5555") 

    handler = rosbag.Bag('../vehicle.bag')
    images,poses = [],[]
    cnt = 0
    for tp,m,ts,conn_header in handler.read_messages(raw=False,return_connection_header=True):
        if not cnt%2:
            images.append(m)
        else:
            poses.append(m)
        cnt += 1
    socket.send_multipart(
        (pickle.dumps(images),pickle.dumps(poses))
    )

    socket.close()
    context.term()

def pull_thread():
    global images,poses
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect("tcp://localhost:5555") 

    imgs,posses = socket.recv_multipart()
    images = pickle.loads(imgs)
    poses = pickle.loads(posses)

    socket.close()
    context.term()

def convert_image(image):
    cv = CvBridge()
    
    return cv.imgmsg_to_cv2(image,desired_encoding="mono8" if image.encoding is None else image.encoding)

def convert_pose(pose):
    position = [pose.position.x, pose.position.y, pose.position.z]
    quaternion = [pose.orientation.x, pose.orientation.y, pose.orientation.z, pose.orientation.w]
    
    rotation_matrix = tf.transformations.quaternion_matrix(quaternion)
    homogeneous_matrix = rotation_matrix
    homogeneous_matrix[:3, 3] = position
    
    return homogeneous_matrix

if __name__ == "__main__":
    push = threading.Thread(target=push_thread)
    pull = threading.Thread(target=pull_thread)

    print("Transferring data from robots to data center ...")
    push.start()
    pull.start()

    push.join()
    pull.join()
    
    print("Converting raw data to images and poses ...")
    try:
        if not os.path.exists('./dump_buffer'):
            os.makedirs('./dump_buffer')
    except Exception as e:
        print(f"{e}")
        exit(130)
    
    for i,(img,pos) in enumerate(zip(images,poses)):
            cv_image,ndarray_pose = convert_image(img),convert_pose(pos)
            np.save(f"./dump_buffer/{i}.npy",ndarray_pose)
            cv2.imwrite(f"./dump_buffer/{i}.png",cv_image)