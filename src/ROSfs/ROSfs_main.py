import sys
import signal
import optparse
import logging
import zmq

try:
    from UserDict import UserDict  # Python 2.x
except ImportError:
    from collections import UserDict  # Python 3.x

# 引入项目内部模块
from .worker import ROSfsWorker
from .dhcp import dhcp as dhcp_enums

# 配置日志格式
logging.basicConfig(level=logging.INFO,
                    format='[ROSfs] %(asctime)s - %(levelname)s - %(message)s')

class ROSfsCmds(UserDict):
    def __init__(self):
        UserDict.__init__(self)
        self._description = {}
        self['help'] = self.help_cmd

    def add_cmd(self, name, function, description):
        self[name] = function
        self._description[name] = description
        
    def get_valid_cmds(self):
        str_out = "Available subcommands:\n"
        for k in sorted(self.keys()):
            str_out += "   %s  " % k
            if k in self._description.keys():
                str_out +="\t%s" % self._description[k]
            str_out += "\n"
        return str_out

    def help_cmd(self, argv):
        argv = [a for a in argv if a != '-h' and a != '--help']

        if len(argv) == 0:
            print('Usage: rosfs <subcommand> [options] [args]')
            print()
            print("ROSfs: A distributed middleware for robot storage.")
            print()
            print(self.get_valid_cmds())
            print()
            return

        cmd = argv[0]
        if cmd in self:
            self[cmd](['-h'])
        else:
            print("Unknown command: '%s'" % cmd, file=sys.stderr)
            print(self.get_valid_cmds(), file=sys.stderr)

def worker_cmd(argv):
    parser = optparse.OptionParser(usage="rosfs worker -p PORT",
                                   description="Start a ROSfs worker node to serve local bag data.",
                                   formatter=optparse.IndentedHelpFormatter())
    
    parser.add_option("-p", "--port", dest="port", default="5555", 
                      action="store", help="Specify the worker binding port")
    
    (options, args) = parser.parse_args(argv)
    
    port = int(options.port)
    logging.info(f"Initializing ROSfs Worker on port {port}...")

    # 1. 创建 ZMQ Context
    context = zmq.Context()
    worker = None

    # 2. 定义信号处理函数 (Ctrl+C)
    def signal_handler(signum, frame):
        logging.info("\nReceived Signal (Ctrl+C). Shutting down worker...")
        if worker:
            # 停止运行标志
            worker._running_ = False
            # 关闭 Socket，这将导致 Client 端连接中断 (ContextTerminated/ZMQError)
            # 从而通知 Client 服务已停止
            worker.close()
            # 销毁 Context 确保退出
            context.term()
        sys.exit(0)

    # 注册信号
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # 3. 实例化并启动 Worker
        worker = ROSfsWorker(zmq_port=port, zmq_context=context)
        worker.listen() # 这是一个阻塞循环
    except zmq.error.ZMQError as e:
        logging.error(f"ZMQ Error during startup: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Worker crashed: {e}")
        sys.exit(1)

def ROSfsmain(argv=None):
    cmds = ROSfsCmds()
    # 注册 worker 命令
    cmds.add_cmd('worker', worker_cmd, "Start a worker daemon on this node.")
    
    if argv is None:
        argv = sys.argv

    # 处理 rosfs worker -h 的情况
    if '-h' in argv or '--help' in argv:
        # 如果是 rosfs -h (无子命令)，保留 help
        if len(argv) <= 2: 
             pass
        else:
             # 如果是 rosfs worker -h，剔除 -h 让子命令自己处理
             argv = [a for a in argv if a != '-h' and a != '--help']
             argv.insert(2, '-h') # 插入到子命令参数位置

    if len(argv) > 1:
        cmd = argv[1]
    else:
        cmd = 'help'

    try:
        if cmd in cmds:
            cmds[cmd](argv[2:])
        else:
            cmds['help']([cmd])
    except KeyboardInterrupt:
        pass