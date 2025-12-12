from .dhcp import dhcp

import zmq
from threading import Lock
import pickle
from .worker import ROSfsWorkerException,worker_cmd
from .dhcp import dhcp

class MissConnectedException(ROSfsWorkerException):
    def __init__(self, value=None):
        super().__init__(value)
        
class DHCPAllocateException(ROSfsWorkerException):
    def __init__(self, value=None):
        super().__init__(value)

class ROSfsClient:
    def __init__(self):
        self._latch_ = Lock()
        
        self._zmqcontext_ = zmq.Context()
        self._workers_ = {}
        self._dhcpsockets_ = {}
        self._dhcp_ = {}
        self._workersockets_ = {}
    
    def connect2dhcp(self,dhcp_ip,dhcp_port):
        if dhcp_ip in self._dhcp_:
            return
        socket = self._zmqcontext_.socket(zmq.REQ)
        socket.connect(f"tcp://{dhcp_ip}:{dhcp_port}")
        with self._latch_:
            self._dhcp_[dhcp_ip] = dhcp_port
            self._dhcpsockets_[dhcp_ip] = socket
    
    def allocate(self,ip):
        if ip not in self._dhcp_:
            raise MissConnectedException(f"Could not connect to dhcp server on {ip}")
        with self._latch_:
            socket:zmq.Socket = self._dhcpsockets_[ip]
            socket.send_multipart((pickle.dumps(dhcp.CMD_ALLOCATE)))
            
        res = socket.recv_multipart()
        cmd = pickle.loads(res[0])
        if cmd==dhcp.CMD_ACK:
            worker_port = pickle.loads(res[1])
            with self._latch_:
                if ip not in self._workers_:
                    self._workers_[ip] = []
                self._workers_[ip].append(worker_port)
                wsocket = self._zmqcontext_.socket(zmq.REQ)
                wsocket.connect(f"tcp://{ip}:{worker_port}")
                self._workersockets_[(ip,worker_port)] = wsocket
        else:
            raise DHCPAllocateException(f"DHCP Allocate Error Code:{cmd}")
        
        return worker_port
    
    def mount(self,ip,port,bag_backend):
        if ip not in self._dhcp_ or ip not in self._workers_ or port not in self._workers_[ip]:
            raise MissConnectedException(f"Could not connect to dhcp server or workers on {ip}:{port}")
        
        socket:zmq.Socket = self._workersockets_[(ip,port)]
        socket.send_multipart(
            (pickle.dumps(worker_cmd.CMD_MOUNT),pickle.dumps(bag_backend))
        )
        