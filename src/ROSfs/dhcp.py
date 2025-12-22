from .worker import ROSfsWorkerException, ROSfsWorker, worker_cmd
import zmq
from enum import Enum
import abc, threading, pickle, logging, signal

logging.basicConfig(level=logging.INFO,
                    format='[DHCP] %(asctime)s - %(levelname)s - %(message)s')

class dhcp(Enum):
    DEFAULT_PORT = 5555  # 改为整数类型，便于计算
    CMD_KILL = "skill" 
    CMD_ALLOCATE = "sallocate"
    CMD_DEALLOCATE = "sdeallocate"
    CMD_ACK = "sack"
    INVALID_CMD = "invalid cmd"


class DHCPException(ROSfsWorkerException):
    """DHCP 相关异常"""
    pass


class PortAllocator:
    """
    端口分配器 - 管理可用端口池
    采用简单的递增策略，从 base_port + 1 开始分配
    """
    def __init__(self, base_port, max_clients=100):
        self._base_port_ = base_port
        self._max_clients_ = max_clients
        self._allocated_ports_ = set()
        self._lock_ = threading.Lock()
        # 端口范围: [base_port + 1, base_port + max_clients]
        self._next_port_ = base_port + 1
    
    def allocate_new_port(self, *args, **kwargs):
        """分配一个新端口"""
        with self._lock_:
            # 寻找可用端口
            for _ in range(self._max_clients_):
                if self._next_port_ not in self._allocated_ports_:
                    port = self._next_port_
                    self._allocated_ports_.add(port)
                    self._next_port_ = self._base_port_ + 1 + \
                        ((self._next_port_ - self._base_port_) % self._max_clients_)
                    return port
                self._next_port_ = self._base_port_ + 1 + \
                    ((self._next_port_ - self._base_port_) % self._max_clients_)
            
            raise DHCPException("No available ports in pool")
    
    def delete_port(self, port):
        """释放端口"""
        with self._lock_:
            self._allocated_ports_.discard(port)
    
    def get_allocated_ports(self):
        """获取已分配端口列表"""
        with self._lock_:
            return list(self._allocated_ports_)


class DHCPOptions:
    """DHCP 调度器配置选项"""
    def __init__(self, port=None, allocator=None, max_clients=100):
        self.port = port if port else dhcp.DEFAULT_PORT.value
        self.allocator = allocator if allocator else PortAllocator(self.port, max_clients)


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
                # 2. 创建 ready event 用于同步
                ready_event = threading.Event()
                
                # 3. 创建 Worker (PAIR 模式)
                worker = ROSfsWorker(new_port, self._zmqcontext_, ready_event)
                
                # 4. 启动线程运行 worker.listen()
                t = threading.Thread(target=worker.listen, daemon=True)
                t.start()
                
                # 5. 等待 Worker 准备就绪（最多等 5 秒）
                if not ready_event.wait(timeout=5.0):
                    logging.warning(f"Worker on port {new_port} took too long to start")
                
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
        """清理所有资源"""
        # 先停止所有 worker
        with self._latch_:
            for port, worker in list(self._clients_.items()):
                try:
                    worker._running_ = False
                    worker.close()
                    self._allocator_.delete_port(port)
                except Exception as e:
                    logging.error(f"Error cleaning up worker on port {port}: {e}")
            self._clients_.clear()
        
        # 关闭 DHCP socket
        try:
            self._zmqsocket_.close()
        except Exception:
            pass
        
        # 销毁 context（需要等待一小段时间确保 worker 线程退出）
        try:
            self._zmqcontext_.term()
        except Exception:
            pass
        
        logging.info("DHCP Scheduler cleaned up.")
    
    def stop(self):
        """停止调度器"""
        self._running_ = False
    
    def get_active_workers(self):
        """获取当前活跃的 worker 端口列表"""
        with self._latch_:
            return list(self._clients_.keys())