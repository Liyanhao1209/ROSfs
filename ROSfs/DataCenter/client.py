import pickle,time,concurrent.futures,sys,shutil,os
import zmq,tqdm
import genpy, genmsg

from cv_bridge import CvBridge
import cv2

import tf
import numpy as np

default_time_step = 0 # second

class ROSBagException(Exception):
    """
    Base class for exceptions in rosbag.
    """
    def __init__(self, value=None):
        self.value = value
        self.args = (value,)

    def __str__(self):
        return self.value

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
    

class Client:
    def __init__(self,server_ip,server_port,pose_topic,image_topic):
        self._ip_ = server_ip
        self._port_ = server_port
        
        self._pose_ = pose_topic
        self._image_ = image_topic
        
        self._zmqcontext_ = zmq.Context()
        self._zmqsocket_ =  self._zmqcontext_.socket(zmq.REQ)
        
        print(server_ip,server_port)
        self._zmqsocket_.connect(f"tcp://{server_ip}:{server_port}")
        
    def _get_message_type(self,info):
        try:
            message_type = genpy.dynamic.generate_dynamic(info.datatype, info.msg_def)[info.datatype]
            if (message_type._md5sum != info.md5sum):
                print('WARNING: For type [%s] stored md5sum [%s] does not match message definition [%s].\n  Try: "rosrun rosbag fix_msg_defs.py old_bag new_bag."' % (info.datatype, info.md5sum, message_type._md5sum), file=sys.stderr)
        except genmsg.InvalidMsgSpec:
            message_type = genpy.dynamic.generate_dynamic(info.datatype, "")[info.datatype]
            print('WARNING: For type [%s] stored md5sum [%s] has invalid message definition."' % (info.datatype, info.md5sum), file=sys.stderr)
        except genmsg.MsgGenerationException as ex:
            raise ROSBagException('Error generating datatype %s: %s' % (info.datatype, str(ex)))
        return message_type
    
    def _deserialize_message(self,raw_msg):
        raw_data, conn_info = raw_msg
        msg_class = self._get_message_type(conn_info)
        ros_msg = msg_class()
        ros_msg.deserialize(raw_data)
        return ros_msg
    
    # request | (x1,y1,z1) | (x2,y2,z2) |
    # response | images | poses | 
    def query_by_pose(self,pose_lb,pose_ub):
        self._zmqsocket_.send_multipart(
            (
                "query".encode(),
                pickle.dumps(pose_lb),
                pickle.dumps(pose_ub)   
            )
        )
        print(f"send query:{pose_lb},{pose_ub}")
        
        raw_images,raw_poses = self._zmqsocket_.recv_multipart()
        raw_images = pickle.loads(raw_images)
        raw_poses = pickle.loads(raw_poses)
        images,poses = [],[]
        for image in raw_images:
            images.append(self._deserialize_message(image))
        for pose in raw_poses:
            poses.append(self._deserialize_message(pose))
            
        return [images,poses]
    
    def kill(self):
        self._zmqsocket_.send_multipart((b'kill',b'kill',b'kill'))
    
    def close_session(self):
        self._zmqsocket_.close()
        self._zmqcontext_.destroy()
        
def client_query(ip,port,pose_topic,image_topic,query_file_path,time_step=default_time_step):
    client = Client(ip,port,pose_topic=pose_topic,image_topic=image_topic)
    
    with open(query_file_path,'r',encoding='utf-8') as qf:
        qs = qf.readlines()
        
        for q in qs:
            spatial_index = list(map(float,q.split(" ")))
            images,poses = client.query_by_pose(spatial_index[0:3],spatial_index[3:])
            time.sleep(time_step)
    
    client.close_session()
    
def query_regular():
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(edge_devices_ips)) as executor:
        futures = []
        for ip in edge_devices_ips:
            futures.append(
                executor.submit(client_query(ip,ip2port[ip],ip2topic[ip][0],ip2topic[ip][1],ip2query[ip]))
            )
        
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Thread failed with {e}")

class QueryFileIterator:
    def __init__(self,fpath):
        self._path_ = fpath
        with open(self._path_,'r',encoding='utf-8') as qf:
            self._query_ = qf.readlines()
        self._qptr = 0
    
    def get_fpth(self):
        return self._path_
    
    def isValid(self):
        return self._qptr < len(self._query_)
    
    def next(self):
        if not self.isValid():
            return None
        spatial_str = self._query_[self._qptr]
        self._qptr += 1
        
        return list(map(float,spatial_str.split()))
        
def query_trigger(dump_pth='./dump_buffer',startup_time=default_time_step):
    startup_bar = tqdm.tqdm(total=startup_time,desc="Server Startup")
    for _ in range(startup_time):
        time.sleep(1)
        startup_bar.update(1)
        
    try:
        shutil.rmtree(dump_pth)
    except Exception as e:
        print(f"Error {e} happened while query initializing")
    finally:
        os.makedirs(dump_pth)
    
    trigger_map = {
        ip2key[ip] : QueryFileIterator(ip2query[ip]) for ip in edge_devices_ips
    }
    
    clients_map = {
        ip2key[ip] : Client(ip,ip2port[ip],ip2topic[ip][0],ip2topic[ip][1]) for ip in edge_devices_ips
    }
    
    msg_cnt = 0
    keys = set(list(ip2key.values()))
    while True:
        key = input("input trigger\n")
        
        if key=="kill":
            break
        if key not in keys:
            print("invalid key")
            continue
        
        
        trigger = trigger_map[key]
        spatial_index = trigger.next()
        if spatial_index is None:
            print(f"end of query file {trigger.get_fpth()}")
            continue
        client = clients_map[key]
        images,poses = client.query_by_pose(spatial_index[0:3],spatial_index[3:])
        
        def convert_and_save(images,poses,msg_cnt):
            assert len(images)==len(poses)
            for image,pose in zip(images,poses):
                cv_image,ndarray_pose = convert_image(image),convert_pose(pose)
                np.save(f"{dump_pth}/{msg_cnt}.npy",ndarray_pose)
                cv2.imwrite(f"{dump_pth}/{msg_cnt}.png",cv_image)
                msg_cnt += 1
            
            return msg_cnt
        
        msg_cnt = convert_and_save(images,poses,msg_cnt=msg_cnt)
    
    for c in clients_map.values():
        c.kill()
        c.close_session()
            
if __name__ == "__main__":

    edge_devices_ips = [
        "172.19.0.2",
        "172.19.0.3"
    ]

    ip2port = {
        "172.19.0.2": 5555,
        "172.19.0.3": 5556
    }
    
    ip2topic = {
        "172.19.0.2": ["/aerial/rgb_image","/aerial/pose"],
        "172.19.0.3": ["/vehicle/rgb_image","/vehicle/pose"]
    }
    
    ip2query = {
        "172.19.0.2": "./172.19.0.2.query",
        "172.19.0.3": "./172.19.0.3.query"
    }
    
    ip2key = {
        "172.19.0.2" : "aerial",
        "172.19.0.3" : "vehicle"
    }
    
    # query_regular()
    query_trigger()