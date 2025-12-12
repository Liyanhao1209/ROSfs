from .worker import ROSfsWorkerException, ROSfsWorker, worker_cmd
import zmq
from enum import Enum
import abc, threading, pickle, logging, signal

class dhcp(Enum):
    DEFAULT_PORT = "5555" 
    CMD_KILL = "skill" 
    CMD_ALLOCATE = "sallocate"
    CMD_DEALLOCATE = "sdeallocate"
    CMD_ACK = "sack"
    INVALID_CMD = "invalid cmd"

# ... (Exception 类和 Allocator 类保持不变，可以直接复制你原来的代码) ...
# 为了节省篇幅，这里只展示修改后的 DHCP_Scheduler 类

class DHCP_Scheduler:
    def __init__(self, dhcp_options):
        self._port_ = int(dhcp_options.port)
        self._zmqcontext_ = zmq.Context()
        # DHCP 依然是 REP 模式
        self._zmqsocket_ = self._zmqcontext_.socket(zmq.REP)
        self._zmqsocket_.bind(f"tcp://*:{self._port_}")
        
        self._latch_ = threading.Lock()
        self._allocator_ = dhcp_options.allocator
        self._clients_ = {} # port -> worker instance
        self._running_ = True

    def listen(self):
        logging.info(f"DHCP listening on {self._port_}")
        while self._running_:
            try:
                # 使用 polling 防止 recv 永久阻塞导致无法响应 signal
                if self._zmqsocket_.poll(1000) == 0:
                    continue
                    
                args = self._zmqsocket_.recv_multipart()
                cmd = pickle.loads(args[0])
                
                if cmd == dhcp.CMD_ALLOCATE:
                    new_port = self._allocate()
                    if new_port:
                        self._zmqsocket_.send_multipart([
                            pickle.dumps(dhcp.CMD_ACK), 
                            pickle.dumps(new_port)
                        ])
                    else:
                        self._zmqsocket_.send_multipart([pickle.dumps(dhcp.INVALID_CMD)])
                        
                elif cmd == dhcp.CMD_DEALLOCATE:
                    if len(args) > 1:
                        port = pickle.loads(args[1])
                        self._deallocate(port)
                    self._zmqsocket_.send_multipart([pickle.dumps(dhcp.CMD_ACK)])
                else:
                    self._zmqsocket_.send_multipart([pickle.dumps(dhcp.INVALID_CMD)])
                    
            except Exception as e:
                logging.error(f"DHCP Loop Error: {e}")
                
        self._cleanup()

    def _allocate(self, *args, **kwargs):
        with self._latch_:
            # 1. 分配端口
            new_port = self._allocator_.allocate_new_port(args, kwargs)
            if new_port in self._clients_:
                return None
            
            try:
                # 2. 创建 Worker (PAIR 模式)
                worker = ROSfsWorker(new_port, self._zmqcontext_)
                
                # 3. 启动线程运行 worker.listen()
                t = threading.Thread(target=worker.listen, daemon=True)
                t.start()
                
                self._clients_[new_port] = worker
                logging.info(f"Allocated Worker on port {new_port}")
                return new_port
            except Exception as e:
                logging.error(f"Allocation failed: {e}")
                self._allocator_.delete_port(new_port)
                return None
            
    def _deallocate(self, port):
        with self._latch_:
            if port in self._clients_:
                worker = self._clients_[port]
                # 优雅关闭：发送 KILL 命令（虽然是 PAIR，也可以通过共享内存标志位关闭，
                # 但这里我们直接调 close 销毁 socket 触发 ContextTerminated 异常退出）
                worker._running_ = False 
                worker.close()
                del self._clients_[port]
                self._allocator_.delete_port(port)

    def _cleanup(self):
        with self._latch_:
            for port in list(self._clients_.keys()):
                self._deallocate(port)
        self._zmqsocket_.close()
        self._zmqcontext_.destroy()