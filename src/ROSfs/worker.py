import rosbag.bag as bag

import zmq

import logging,os,pickle
from enum import Enum

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class ROSfsWorkerException(Exception):
    def __init__(self, value=None):
        self.value = value
        
    def __str__(self):
        return self.value

class worker_cmd(Enum):
    CMD_MOUNT = "wmount"
    CMD_KILL = "wkill"
    ACK_BAD_MOUNT = "mount error"
    ACK = "wack"
    INVALID_CMD = "invalid cmd"

class VoidBackendException(ROSfsWorkerException):
    def __init__(self, value=None):
        super().__init__(value)
        
class DeprecateBackendException(ROSfsWorkerException):
    def __init__(self, value=None):
        super().__init__(value)

class ROSfsWorker:
    def __init__(self,zmq_port,zmq_context):
        self._path_ = None
        self._port_ = zmq_port
        
        self._zmqcontext_ = zmq_context
        self._zmqsocket_ = self._zmqcontext_.socket(zmq.REP)
        self._zmqsocket_.bind(f"tcp://*:{self._port_}")
        
        self._running_ = True
    
    def mount(self,bag_backend):
        self._path_ = bag_backend
            
    
    def is_valid(self):
        return os.path.exists(self._path_)
    
    def get_handler(self):
        if not os.path.exists(self._path_) or not os.path.isdir(self._path_):
            raise VoidBackendException(
                f"ROSfs backend {self._path_} does not exist or is not a directory"
            )
        
        return bag.Bag(self._path_,'rosfs')
    
    def listen(self):
        while self._running_:
            args = self._zmqsocket_.recv_multipart()
            cmd = pickle.loads(args[0])
            
            if cmd == worker_cmd.CMD_MOUNT:
                if len(args)<2:
                    self._zmqsocket_.send_multipart((pickle.dumps(worker_cmd.ACK_BAD_MOUNT)))
                else:
                    backend = pickle.loads(args[1])
                    self.mount(bag_backend=backend)
                    self._zmqsocket_.send_multipart((pickle.dumps(worker_cmd.ACK)))
            elif cmd == worker_cmd.CMD_KILL:
                self._zmqsocket_.send_multipart((pickle.dumps(worker_cmd.ACK)))
                self.close()
            else:
                self._zmqsocket_.send_multipart((pickle.dumps(worker_cmd.INVALID_CMD)))
                
            
            
    
    def close(self):
        self._zmqsocket_.close()
    
    