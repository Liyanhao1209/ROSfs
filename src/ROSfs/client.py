from .dhcp import dhcp
import zmq
from threading import Lock
import pickle
import rospy
from .worker import ROSfsWorkerException, worker_cmd
import logging

logging.basicConfig(level=logging.INFO,
                    format='[Client] %(asctime)s - %(levelname)s - %(message)s')

class MissConnectedException(ROSfsWorkerException): pass
class DHCPAllocateException(ROSfsWorkerException): pass
class RemoteReadException(ROSfsWorkerException): pass

class ROSfsClient:
    def __init__(self, timeout_ms=5000):
        """
        初始化 ROSfs Client
        
        Args:
            timeout_ms: 请求超时时间（毫秒），默认5秒
        """
        self._latch_ = Lock()
        self._zmqcontext_ = zmq.Context()
        self._dhcpsockets_ = {}     # ip -> REQ socket
        self._workersockets_ = {}   # (ip, port) -> PAIR socket
        self._timeout_ms_ = timeout_ms
    
    def connect2dhcp(self, dhcp_ip, dhcp_port=None):
        """
        连接到 DHCP Scheduler
        
        Args:
            dhcp_ip: DHCP 服务器 IP
            dhcp_port: DHCP 服务器端口，默认使用 dhcp.DEFAULT_PORT
        """
        if dhcp_port is None:
            dhcp_port = dhcp.DEFAULT_PORT.value
            
        if dhcp_ip in self._dhcpsockets_: 
            return
        
        # DHCP 使用 REQ-REP 模式
        socket = self._zmqcontext_.socket(zmq.REQ)
        socket.setsockopt(zmq.RCVTIMEO, self._timeout_ms_)
        socket.setsockopt(zmq.SNDTIMEO, self._timeout_ms_)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(f"tcp://{dhcp_ip}:{dhcp_port}")
        
        with self._latch_:
            self._dhcpsockets_[dhcp_ip] = socket
        
        logging.info(f"Connected to DHCP at {dhcp_ip}:{dhcp_port}")
    
    def allocate(self, ip):
        """
        请求 DHCP 分配一个专用 Worker
        
        Args:
            ip: DHCP 服务器 IP
            
        Returns:
            分配的 Worker 端口号
        """
        if ip not in self._dhcpsockets_:
            raise MissConnectedException(f"Not connected to DHCP on {ip}")
        
        with self._latch_:
            socket = self._dhcpsockets_[ip]
            try:
                socket.send_multipart([pickle.dumps(dhcp.CMD_ALLOCATE)])
                res = socket.recv_multipart()
            except zmq.Again:
                raise DHCPAllocateException(f"DHCP request timeout")
            
            cmd = pickle.loads(res[0])
            
            if cmd == dhcp.CMD_ACK:
                worker_port = pickle.loads(res[1])
                
                # 建立到 Worker 的 PAIR 连接
                wsocket = self._zmqcontext_.socket(zmq.PAIR)
                wsocket.setsockopt(zmq.RCVTIMEO, self._timeout_ms_)
                wsocket.setsockopt(zmq.SNDTIMEO, self._timeout_ms_)
                wsocket.setsockopt(zmq.LINGER, 0)
                wsocket.connect(f"tcp://{ip}:{worker_port}")
                self._workersockets_[(ip, worker_port)] = wsocket
                
                logging.info(f"Allocated worker at {ip}:{worker_port}")
                return worker_port
            else:
                raise DHCPAllocateException(f"DHCP Error: {cmd}")
    
    def deallocate(self, ip, port):
        """
        释放 Worker 连接
        
        Args:
            ip: 服务器 IP
            port: Worker 端口
        """
        key = (ip, port)
        with self._latch_:
            if key in self._workersockets_:
                try:
                    self._workersockets_[key].close()
                except:
                    pass
                del self._workersockets_[key]
            
            # 通知 DHCP 释放端口
            if ip in self._dhcpsockets_:
                try:
                    socket = self._dhcpsockets_[ip]
                    socket.send_multipart([
                        pickle.dumps(dhcp.CMD_DEALLOCATE),
                        pickle.dumps(port)
                    ])
                    socket.recv_multipart()  # 等待 ACK
                except:
                    pass
        
        logging.info(f"Deallocated worker at {ip}:{port}")
    
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
    
    def disconnect(self, ip=None):
        """
        断开与指定 IP 的 DHCP 连接，如果 ip=None 则断开所有连接
        
        Args:
            ip: 要断开的服务器 IP，None 表示全部
        """
        with self._latch_:
            if ip is None:
                # 关闭所有连接
                for sock in self._dhcpsockets_.values():
                    try:
                        sock.close()
                    except:
                        pass
                self._dhcpsockets_.clear()
                
                for sock in self._workersockets_.values():
                    try:
                        sock.close()
                    except:
                        pass
                self._workersockets_.clear()
            else:
                # 只关闭指定 IP 的连接
                if ip in self._dhcpsockets_:
                    try:
                        self._dhcpsockets_[ip].close()
                    except:
                        pass
                    del self._dhcpsockets_[ip]
                
                # 关闭该 IP 上的所有 worker 连接
                keys_to_remove = [k for k in self._workersockets_ if k[0] == ip]
                for key in keys_to_remove:
                    try:
                        self._workersockets_[key].close()
                    except:
                        pass
                    del self._workersockets_[key]
    
    def close(self):
        """关闭客户端，释放所有资源"""
        self.disconnect()
        try:
            self._zmqcontext_.term()
        except:
            pass
        logging.info("ROSfs Client closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False