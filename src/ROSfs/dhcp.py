from .worker import ROSfsWorkerException,ROSfsWorker

import zmq
from enum import Enum
import abc,threading,pickle,logging,signal

class dhcp(Enum):
    DEFAULT_PORT = "5555" 
    CMD_KILL = "skill" # should never call
    CMD_ALLOCATE = "sallocate"
    CMD_DEALLOCATE = "sdeallocate"
    CMD_ACK = "sack"
    INVALID_CMD = "invalid cmd"

class DHCP_Exception(ROSfsWorkerException):
    def __init__(self, value=None):
        super().__init__(value)
        
class PortInUseException(ROSfsWorkerException):
    def __init__(self, value=None):
        super().__init__(value)

class PortNotExistException(ROSfsWorkerException):
    def __init__(self, value=None):
        super().__init__(value)

class DHCPAllocator(abc.ABC):
    def __init__(self,DHCP_Scheduler_PORT=dhcp.DEFAULT_PORT):
        self._scheduler_port_ = DHCP_Scheduler_PORT
    
    def allocate_new_port(self,*args,**kwargs):
        raise NotImplementedError()
    
    def delete_port(self,port):
        raise NotImplementedError()

class TrivialAllocator(DHCPAllocator):
    def __init__(self, DHCP_Scheduler_PORT=dhcp.DEFAULT_PORT):
        super().__init__(DHCP_Scheduler_PORT)
        
        self._latch_ = threading.Lock()
        self._ports_allocated_ = []
        self._cur_port_ = DHCP_Scheduler_PORT + 1
    
    def allocate_new_port(self, *args, **kwargs):
        with self._latch_:
            ans = self._cur_port_
            self._ports_allocated_.append(self._cur_port_)
            self._cur_port_ += 1
            
        return ans
    
    def delete_port(self,port):
        with self._latch_:
            self._ports_allocated_.remove(port)

class DHCP_Options:
    def __init__(self,
                 allocator:DHCPAllocator=TrivialAllocator(),
                 port=dhcp.DEFAULT_PORT
        ):
        self.allocator = allocator
        self.port = port

class DHCP_Scheduler:
    def __init__(self,dhcp_options:DHCP_Options):
        self._port_ = dhcp_options.port
        
        try:
            self._zmqcontext_ = zmq.Context()
            self._zmqsocket_ = self._zmqcontext_.socket(zmq.REP)
            self._zmqcontext_.bind(f"tcp://*:{self._port_}")
        except Exception as e:
            raise DHCP_Exception(f"Exception {e} happens when initializing DHCP_Scheduler")
        
        self._latch_ = threading.Lock()
        self._allocator_ = dhcp_options.allocator
        self._clients_ = {}
        
        self._running_ = True
        signal.signal(signal.SIGINT, self._handle_sigint_)
        signal.signal(signal.SIGTERM, self._handle_sigint_)
    
    def _handle_sigint_(self, signum, frame):
        logging.info("Received signal, shutting down...")
        self._running_ = False
    
    def listen(self):
        while self._running_:
            try:
                args = self._zmqsocket_.recv_multipart()
                cmd = pickle.loads(args[0])
                
                if cmd == dhcp.CMD_ALLOCATE:
                    new_port = self._allocate()
                    self._zmqsocket_.send_multipart(
                        (pickle.dumps(dhcp.CMD_ACK), pickle.dumps(new_port))
                    )
                elif cmd == dhcp.CMD_DEALLOCATE:
                    if len(args) < 2:
                        self._zmqsocket_.send_multipart(
                            (pickle.dumps(dhcp.CMD_ACK))
                        )
                    else:
                        port = pickle.loads(args[1])
                        if port in self._clients_:
                            self._deallocate(port)
                            self._zmqsocket_.send_multipart(
                                (pickle.dumps(dhcp.CMD_ACK))
                            )
                else:
                    self._zmqsocket_.send_multipart(
                        (pickle.dumps(dhcp.INVALID_CMD))
                    )
            except Exception as e:
                logging.error(f"Error in listen loop: {e}")
                break

        self._close()
                

    def _allocate(self,*args,**kwargs):
        # allocate a port
        # start a rosfs server on the port
        with self._latch_:
            new_port = self._allocator_.allocate_new_port(args,kwargs)
            if new_port in self._clients_:
                raise PortInUseException(f"Port {new_port} already in use...")
            try:
                worker = ROSfsWorker(new_port,self._zmqcontext_)
            except Exception as e:
                logging.error(f"{e},delete allocated port {new_port} from allocator")
                self._allocator_.delete_port(new_port)
                return None
            
            self._clients_[new_port] = worker
            
    def _deallocate(self,port):
        with self._latch_:
            if port not in self._clients_:
                raise PortNotExistException("Specified port does not exist")
            worker:ROSfsWorker = self._clients_[port]
            worker.close()
            del self._clients_[port]
            self._allocator_.delete_port(port)
                
            
    def _close(self):
        self._zmqsocket_.close()
        self._zmqcontext_.destroy()