import socket,threading,pickle,sys,time
import zmq
import rosbag, genpy, genmsg

def get_local_ip():
    try:
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        return ip
    except Exception as e:
        print(f"Error: {e}")
        return None
    
class ROSBagException(Exception):
    """
    Base class for exceptions in rosbag.
    """
    def __init__(self, value=None):
        self.value = value
        self.args = (value,)

    def __str__(self):
        return self.value

class Server:
    def __init__(self,port,rosfs_backend,image_topic,pose_topic):
        self._port_ = port
        self._rosfspth_ = rosfs_backend
        self._image_ = image_topic
        self._pose_ = pose_topic
        
        self._zmqcontext_ = zmq.Context()
        self._zmqsocket_ = self._zmqcontext_.socket(zmq.REP)
        self._zmqsocket_.bind(f"tcp://*:{self._port_}")
        
        self._rwlatch_ = threading.Lock() # lock on tuple of (image,pos) buffer
        self._buffer_ = []
        self._msgidxptr_ = 0
        
        def start_reader_daemon():
            rdt = threading.Thread(target=self._reader_daemon)
            rdt.daemon = True
            rdt.start()
            
        start_reader_daemon()
        
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
        
    def _start_reading(self):
        rosfs_handler = rosbag.Bag(self._rosfspth_,'rosfs')
        reading_ub = min(rosfs_handler.get_message_count(self._image_),rosfs_handler.get_message_count(self._pose_))
        
        def read_by_id(topic,start_id,cnt,handler):
            return handler.read_messages_by_id([topic],start_id,cnt,raw=True,return_connection_header=False)
            
        read_cnt = reading_ub - 1 - self._msgidxptr_ + 1
        if read_cnt == 0:
            return
        for raw_image,raw_pose in zip(
            read_by_id(self._image_,self._msgidxptr_,read_cnt,rosfs_handler),
            read_by_id(self._pose_,self._msgidxptr_,read_cnt,rosfs_handler)
        ):
            _,image_data,_ = raw_image
            _,pose_data,_ = raw_pose
            with self._rwlatch_:
                self._buffer_.append(
                    (image_data,pose_data,self._deserialize_message(pose_data))
                )
            
        with self._rwlatch_:
            self._msgidxptr_ = self._msgidxptr_ + read_cnt - 1 + 1
        
        print(f"reader daemon:{read_cnt} msgs")
    
    def _reader_daemon(self):
        while True:
            self._start_reading()
            time.sleep(1)
    
    def _spatial_search(self,pose_lb,pose_ub):
        with self._rwlatch_:
            raw_images,raw_poses = [],[]
            for raw_image,raw_pose,pose_obj in self._buffer_:
                x,y,z = pose_obj.position.x,pose_obj.position.y,pose_obj.position.z
                x1,y1,z1,x2,y2,z2 = pose_lb[0],pose_lb[1],pose_lb[2],pose_ub[0],pose_ub[1],pose_ub[2],
                if x1<=x<=x2 and y1<=y<=y2 and z1<=z<=z2:
                    raw_images.append(raw_image)
                    raw_poses.append(raw_pose)
            
            return (raw_images,raw_poses)
            
    def response(self):
        while True:
            try:
                cmd,raw_pose_lb,raw_pose_ub = self._zmqsocket_.recv_multipart()
            except Exception as e:
                print(f"Error {e} happened while server responsing")
            if cmd==b'kill':
                self.close_session()
                break
            pose_lb,pose_ub = pickle.loads(raw_pose_lb),pickle.loads(raw_pose_ub)
            print(f"[ROS INFO]:receive query {cmd},{pose_lb},{pose_ub}")
            
            raw_images,raw_poses = self._spatial_search(pose_lb,pose_ub)
            self._zmqsocket_.send_multipart(
                (
                    pickle.dumps(raw_images),
                    pickle.dumps(raw_poses)
                )
            )
    
    def close_session(self):
        self._zmqsocket_.close()
        self._zmqcontext_.destroy()
        
if __name__ == "__main__":
    ip2port = {
        "172.17.0.2": 5555
    }
    
    ip2topic = {
        "172.17.0.2": ["/aerial/rgb_image","/aerial/pose"]
    }
    
    local_ip = get_local_ip()
    server = Server(ip2port[local_ip],'./172.17.0.2.bag',ip2topic[local_ip][0],ip2topic[local_ip][1])
    
    server.response()