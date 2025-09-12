import numpy as np
from geometry_msgs.msg import Pose
import tf

npy_pth = "/data/GroundAir/dataset/big_city_-04_-35/aerial/pose/a0000.npy"

if __name__ == "__main__":
    npy_data = np.load(npy_pth)
    print(npy_data)
    
    homogeneous_matrix = npy_data
    
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

    print("Pose message:")
    print("Position: x=%f, y=%f, z=%f" % (pose_msg.position.x, pose_msg.position.y, pose_msg.position.z))
    print("Orientation: x=%f, y=%f, z=%f, w=%f" % (pose_msg.orientation.x, pose_msg.orientation.y, pose_msg.orientation.z, pose_msg.orientation.w))

    pose = pose_msg