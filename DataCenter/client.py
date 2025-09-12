import pickle,time,concurrent.futures
import zmq,tqdm
import genpy, genmsg

default_time_step = 10 # second

class ROSBagException(Exception):
    """
    Base class for exceptions in rosbag.
    """
    def __init__(self, value=None):
        self.value = value
        self.args = (value,)

    def __str__(self):
        return self.value
    

class Client:
    def __init__(self,server_ip,server_port,pose_topic,image_topic):
        self._ip_ = server_ip
        self._port_ = server_port
        
        self._pose_ = pose_topic
        self._image_ = image_topic
        
        self._zmqcontext_ = zmq.Context()
        self._zmqsocket_ =  self._zmqcontext_.socket(zmq.REQ)
        
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
                self._pose_.decode(),
                self._image_.decode(),
                pickle.dumps(pose_lb),
                pickle.dumps(pose_ub)
            )
        )
        
        raw_images,raw_poses = pickle.dumps(self._zmqsocket_.recv_multipart())
        images,poses = [],[]
        for image in raw_images:
            images.append(self._deserialize_message(image))
        for pose in raw_poses:
            poses.append(self._deserialize_message(pose))
            
        return [images,poses]
    
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
        
    def isValid(self):
        return self._qptr < len(self._query_)
    
    def next(self):
        if not self.isValid():
            return None
        spatial_str = self._query_[self._qptr]
        self._qptr += 1
        
        return list(map(float,spatial_str))
        
        

def query_trigger(startup_time=default_time_step):
    startup_bar = tqdm.tqdm(total=startup_time,desc="Server Startup")
    for i in range(startup_time):
        time.sleep(1)
        startup_bar.update(1)
    
    trigger_map = {
        ip2key[ip] : QueryFileIterator(ip2query[ip]) for ip in edge_devices_ips
    }
    
    clients_map = {
        ip2key[ip] : Client(ip,ip2port[ip],ip2topic[ip][0],ip2topic[ip][1]) for ip in edge_devices_ips
    }
    
    keys = set(list(ip2key.values()))
    while True:
        key = input("input trigger")
        if key=="kill":
            break
        if key not in key:
            print("invalid key")
            continue
        trigger = trigger_map[key]
        spatial_index = trigger.next()
        client = clients_map[key]
        images,poses = client.query_by_pose(spatial_index[0:3],spatial_index[3:])
        print(len(images,poses))
    
    for c in clients_map.values():
        c.close_session()
            
if __name__ == "__main__":

    edge_devices_ips = [
        "172.17.0.2",
    ]

    ip2port = {
        "172.17.0.2": 5555
    }
    
    ip2topic = {
        "172.17.0.2": ["/aerial/rgb_image","/aerial/pose"]
    }
    
    ip2query = {
        "172.17.0.2": "./172.17.0.2.query"
    }
    
    ip2key = {
        "172.17.0.2" : "aerial"
    }
    
    # query_regular()
    query_trigger()