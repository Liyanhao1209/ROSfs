from .dhcp import dhcp
import zmq
from threading import Lock
import pickle
import rospy
from .worker import ROSfsWorkerException, worker_cmd

class MissConnectedException(ROSfsWorkerException): pass
class DHCPAllocateException(ROSfsWorkerException): pass
class RemoteReadException(ROSfsWorkerException): pass

class ROSfsClient:
    def __init__(self):
        self._latch_ = Lock()
        self._zmqcontext_ = zmq.Context()
        self._dhcpsockets_ = {}     # ip -> REQ socket
        self._workersockets_ = {}   # (ip, port) -> PAIR socket
    
    def connect2dhcp(self, dhcp_ip, dhcp_port):
        if dhcp_ip in self._dhcpsockets_: return
        
        # DHCP 仍然使用 REQ-REP
        socket = self._zmqcontext_.socket(zmq.REQ)
        socket.connect(f"tcp://{dhcp_ip}:{dhcp_port}")
        with self._latch_:
            self._dhcpsockets_[dhcp_ip] = socket
    
    def allocate(self, ip):
        if ip not in self._dhcpsockets_:
            raise MissConnectedException(f"Not connected to DHCP on {ip}")
        
        with self._latch_:
            socket = self._dhcpsockets_[ip]
            socket.send_multipart([pickle.dumps(dhcp.CMD_ALLOCATE)])
            
            res = socket.recv_multipart()
            cmd = pickle.loads(res[0])
            
            if cmd == dhcp.CMD_ACK:
                worker_port = pickle.loads(res[1])
                
                # 建立到 Worker 的 PAIR 连接
                wsocket = self._zmqcontext_.socket(zmq.PAIR)
                wsocket.connect(f"tcp://{ip}:{worker_port}")
                self._workersockets_[(ip, worker_port)] = wsocket
                
                return worker_port
            else:
                raise DHCPAllocateException(f"DHCP Error: {cmd}")
    
    def mount(self, ip, port, bag_backend):
        if (ip, port) not in self._workersockets_:
            raise MissConnectedException("Worker not connected")
        
        socket = self._workersockets_[(ip, port)]
        socket.send_multipart([
            pickle.dumps(worker_cmd.CMD_MOUNT),
            pickle.dumps(bag_backend)
        ])
        
        res = socket.recv_multipart()
        cmd = pickle.loads(res[0])
        if cmd != worker_cmd.ACK:
            # 尝试读取错误信息
            msg = pickle.loads(res[1]) if len(res) > 1 else "Unknown"
            raise ROSfsWorkerException(f"Mount failed: {msg}")

    def read_messages(self, ip, port, topics, start_time, end_time):
        """
        [Generator] Time-based query
        Yields: (topic, data_bytes, timestamp, datatype)
        """
        payload = [
            pickle.dumps(worker_cmd.CMD_READ_TIME),
            pickle.dumps(topics),
            pickle.dumps(start_time),
            pickle.dumps(end_time)
        ]
        yield from self._stream_request(ip, port, payload)

    def read_messages_by_id(self, ip, port, topics, start_id, cnt):
        """
        [Generator] ID-based query
        """
        payload = [
            pickle.dumps(worker_cmd.CMD_READ_ID),
            pickle.dumps(topics),
            pickle.dumps(start_id),
            pickle.dumps(cnt)
        ]
        yield from self._stream_request(ip, port, payload)

    def _stream_request(self, ip, port, multipart_payload):
        if (ip, port) not in self._workersockets_:
            raise MissConnectedException("Worker not connected")
        
        socket = self._workersockets_[(ip, port)]
        
        # 1. 发送请求
        socket.send_multipart(multipart_payload)
        
        # 2. 等待 ACK 确认请求合法
        ack_res = socket.recv_multipart()
        ack_cmd = pickle.loads(ack_res[0])
        
        if ack_cmd == worker_cmd.ACK_ERROR:
            msg = pickle.loads(ack_res[1])
            raise RemoteReadException(f"Server Error: {msg}")
        elif ack_cmd != worker_cmd.ACK:
            raise RemoteReadException(f"Unexpected ACK: {ack_cmd}")
            
        # 3. 进入流接收循环
        while True:
            res = socket.recv_multipart()
            cmd = pickle.loads(res[0])
            
            if cmd == worker_cmd.ACK_STREAM_DATA:
                # 解包数据：(topic, datatype, data_bytes, timestamp_float)
                data_tuple = pickle.loads(res[1])
                # 转换为用户友好的格式，这里把 timestamp float 转回 rospy.Time (如果需要)
                yield data_tuple 
                
            elif cmd == worker_cmd.ACK_STREAM_END:
                break
                
            elif cmd == worker_cmd.ACK_ERROR:
                msg = pickle.loads(res[1])
                raise RemoteReadException(f"Stream Error: {msg}")
            else:
                break