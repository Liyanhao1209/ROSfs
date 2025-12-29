import rosbag.bag as bag
import rospy
import zmq
import logging, os, pickle, threading
from enum import Enum

logging.basicConfig(level=logging.INFO,
                    format='[Worker] %(asctime)s - %(levelname)s - %(message)s')

class ROSfsWorkerException(Exception):
    def __init__(self, value=None):
        self.value = value
    def __str__(self):
        return str(self.value)

class worker_cmd(Enum):
    CMD_MOUNT = "wmount"
    CMD_KILL = "wkill"
    CMD_READ_TIME = "wread_time"
    CMD_READ_ID = "wread_id"
    
    # 响应状态码
    ACK = "wack"
    ACK_STREAM_DATA = "wstream_data" # 数据包
    ACK_STREAM_END = "wstream_end"   # 流结束信号
    ACK_ERROR = "werror"
    INVALID_CMD = "invalid cmd"

class ROSfsWorker:
    def __init__(self, zmq_port, zmq_context, ready_event=None):
        """
        初始化 Worker
        
        Args:
            zmq_port: 监听端口
            zmq_context: ZMQ Context（可以与 DHCP 共享）
            ready_event: 可选的 threading.Event，当 Worker 准备好接收连接时会 set
        """
        self._path_ = None
        self._port_ = zmq_port
        self._zmqcontext_ = zmq_context
        self._ready_event_ = ready_event
        
        # 使用 PAIR 模式，适合一对一独占连接
        self._zmqsocket_ = self._zmqcontext_.socket(zmq.PAIR)
        self._zmqsocket_.setsockopt(zmq.LINGER, 0)
        
        # 设置接收超时，便于检查 _running_ 标志
        self._zmqsocket_.setsockopt(zmq.RCVTIMEO, 1000)  # 1秒超时
        
        self._zmqsocket_.bind(f"tcp://*:{self._port_}")
        
        self._running_ = True
        self._bag_handler_ = None
        self._closed_ = False

    def mount(self, bag_backend):
        self._path_ = bag_backend
        if self._bag_handler_:
            self._bag_handler_.close()
            self._bag_handler_ = None

    def get_handler(self):
        if not self._path_ or not os.path.exists(self._path_):
            raise ROSfsWorkerException(f"Backend path invalid: {self._path_}")
        
        return bag.Bag(self._path_,'rosfs')
        
        # if self._bag_handler_ is None:
        #     # 假设 bag.Bag 支持 allow_unindexed=True (根据你的描述是魔改版)
        #     # 如果是标准版 rosbag，参数可能是 allow_unindexed
        #     try:
        #         self._bag_handler_ = bag.Bag(self._path_, 'r', allow_unindexed=True)
        #     except TypeError:
        #         # 兼容标准 rosbag
        #         self._bag_handler_ = bag.Bag(self._path_, 'r')
        # return self._bag_handler_

    def listen(self):
        logging.info(f"Worker started listening on port {self._port_} (Mode: PAIR Streaming)")
        
        # 通知 DHCP Scheduler，Worker 已准备好
        if self._ready_event_:
            self._ready_event_.set()
        
        while self._running_:
            try:
                # 阻塞接收命令（带超时）
                try:
                    args = self._zmqsocket_.recv_multipart()
                except zmq.Again:
                    # 超时，继续循环检查 _running_
                    continue
                    
                if not args: 
                    continue
                
                cmd = pickle.loads(args[0])

                if cmd == worker_cmd.CMD_MOUNT:
                    self._handle_mount(args)
                elif cmd == worker_cmd.CMD_READ_TIME:
                    self._handle_read(args, mode='time')
                elif cmd == worker_cmd.CMD_READ_ID:
                    self._handle_read(args, mode='id')
                elif cmd == worker_cmd.CMD_KILL:
                    self._zmqsocket_.send_multipart([pickle.dumps(worker_cmd.ACK)])
                    self._running_ = False
                else:
                    self._send_error("Unknown Command")

            except zmq.ContextTerminated:
                logging.info(f"Worker on port {self._port_}: Context terminated")
                break
            except zmq.ZMQError as e:
                if not self._running_:
                    break
                logging.error(f"Worker Loop ZMQ Error: {e}")
            except Exception as e:
                logging.error(f"Worker Loop Error: {e}")
                try:
                    self._send_error(str(e))
                except:
                    pass
        
        logging.info(f"Worker on port {self._port_} stopped.")
        self.close()

    def _handle_mount(self, args):
        try:
            if len(args) < 2:
                raise ValueError("Missing bag path")
            backend = pickle.loads(args[1])
            self.mount(backend)
            self._zmqsocket_.send_multipart([pickle.dumps(worker_cmd.ACK)])
        except Exception as e:
            self._send_error(f"Mount failed: {e}")

    def _handle_read(self, args, mode):
        """核心流式处理逻辑"""
        try:
            topics = pickle.loads(args[1])
            handler = self.get_handler()
            gen = None

            if mode == 'time':
                # 客户端传来的是相对时间（相对于 bag 开始的秒数）
                relative_start = pickle.loads(args[2])
                relative_end = pickle.loads(args[3])
                
                # 获取 bag 文件的绝对起始时间
                bag_start_time = handler.get_start_time()  # 返回 float (秒)
                
                # 转换为绝对时间
                abs_start_time = rospy.Time.from_sec(bag_start_time + relative_start)
                abs_end_time = rospy.Time.from_sec(bag_start_time + relative_end)
                
                logging.info(f"Reading messages: bag_start={bag_start_time:.3f}, "
                           f"relative=[{relative_start}, {relative_end}], "
                           f"absolute=[{abs_start_time.to_sec():.3f}, {abs_end_time.to_sec():.3f}]")
                
                gen = handler.read_messages(topics, abs_start_time, abs_end_time, raw=True)
            elif mode == 'id':
                start_id = pickle.loads(args[2])
                cnt = pickle.loads(args[3])
                gen = handler.read_messages_by_id(topics, start_id, cnt, raw=True)

            # 1. 先发送 ACK，告知客户端准备接收流
            self._zmqsocket_.send_multipart([pickle.dumps(worker_cmd.ACK)])

            # 2. 循环读取并推送数据 (Streaming)
            # raw=True 返回: (topic, msg_tuple, t)
            # msg_tuple 是 (datatype, data, md5sum, position, pytype)
            # 我们只传输必要数据以减少带宽： topic, datatype, data, timestamp
            count = 0
            if gen:
                for msg in gen:
                    topic, raw_tuple, t = msg
                    datatype, data_bytes, md5sum, pos, pytype = raw_tuple
                    
                    # 构建轻量级 payload
                    payload = (topic, datatype, data_bytes, t.to_sec())
                    
                    self._zmqsocket_.send_multipart([
                        pickle.dumps(worker_cmd.ACK_STREAM_DATA),
                        pickle.dumps(payload)
                    ])
                    count += 1
            
            logging.info(f"Streamed {count} messages.")

            # 3. 发送结束信号
            self._zmqsocket_.send_multipart([pickle.dumps(worker_cmd.ACK_STREAM_END)])

        except Exception as e:
            logging.error(f"Read Error: {e}")
            # 如果流中间出错，发送错误帧
            self._send_error(str(e))

    def _send_error(self, msg):
        try:
            self._zmqsocket_.send_multipart([
                pickle.dumps(worker_cmd.ACK_ERROR), 
                pickle.dumps(msg)
            ])
        except zmq.ZMQError:
            pass  # socket 可能已关闭

    def close(self):
        """关闭 Worker，释放资源"""
        if self._closed_:
            return
        self._closed_ = True
        self._running_ = False
        
        if self._bag_handler_:
            try:
                self._bag_handler_.close()
            except:
                pass
            self._bag_handler_ = None
        
        try:
            self._zmqsocket_.close()
        except:
            pass