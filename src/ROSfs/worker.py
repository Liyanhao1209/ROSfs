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
    def __init__(self, zmq_port, zmq_context):
        self._path_ = None
        self._port_ = zmq_port
        self._zmqcontext_ = zmq_context
        
        # 优化：使用 PAIR 模式，适合一对一独占连接，支持单向流式推送
        self._zmqsocket_ = self._zmqcontext_.socket(zmq.PAIR)
        self._zmqsocket_.bind(f"tcp://*:{self._port_}")
        
        # 设置 linger 为 0，避免关闭时 socket 卡死
        self._zmqsocket_.setsockopt(zmq.LINGER, 0)
        
        self._running_ = True
        self._bag_handler_ = None

    def mount(self, bag_backend):
        self._path_ = bag_backend
        if self._bag_handler_:
            self._bag_handler_.close()
            self._bag_handler_ = None

    def get_handler(self):
        if not self._path_ or not os.path.exists(self._path_):
            raise ROSfsWorkerException(f"Backend path invalid: {self._path_}")
        
        if self._bag_handler_ is None:
            # 假设 bag.Bag 支持 allow_unindexed=True (根据你的描述是魔改版)
            # 如果是标准版 rosbag，参数可能是 allow_unindexed
            try:
                self._bag_handler_ = bag.Bag(self._path_, 'r', allow_unindexed=True)
            except TypeError:
                # 兼容标准 rosbag
                self._bag_handler_ = bag.Bag(self._path_, 'r')
        return self._bag_handler_

    def listen(self):
        logging.info(f"Started listening on port {self._port_} (Mode: PAIR Streaming)")
        
        while self._running_:
            try:
                # 阻塞接收命令
                args = self._zmqsocket_.recv_multipart()
                if not args: continue
                
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
                break
            except Exception as e:
                logging.error(f"Worker Loop Error: {e}")
                self._send_error(str(e))
        
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
                start_time = rospy.Time.from_sec(pickle.loads(args[2]))
                end_time = rospy.Time.from_sec(pickle.loads(args[3]))
                gen = handler.read_messages(topics, start_time, end_time, raw=True)
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
        self._zmqsocket_.send_multipart([
            pickle.dumps(worker_cmd.ACK_ERROR), 
            pickle.dumps(msg)
        ])

    def close(self):
        if self._bag_handler_:
            self._bag_handler_.close()
        self._zmqsocket_.close()