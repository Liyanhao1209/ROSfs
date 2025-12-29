import sys
import signal
import optparse
import logging

try:
    from UserDict import UserDict  # Python 2.x
except ImportError:
    from collections import UserDict  # Python 3.x

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
    """
    启动 DHCP Scheduler 服务，监听指定端口。
    当 Client 发起 allocate 请求时，DHCP 会为其分配一个专用 Worker 端口。
    """
    # 延迟导入，避免循环依赖
    import zmq
    from .dhcp import DHCP_Scheduler, DHCPOptions
    
    parser = optparse.OptionParser(
        usage="rosfs worker -p PORT [-m MAX_CLIENTS]",
        description="Start a ROSfs DHCP Scheduler that manages worker allocation for clients.",
        formatter=optparse.IndentedHelpFormatter()
    )
    
    parser.add_option("-p", "--port", dest="port", default="5555", 
                      action="store", help="Specify the DHCP scheduler binding port (default: 5555)")
    parser.add_option("-m", "--max-clients", dest="max_clients", default="100",
                      action="store", help="Maximum number of concurrent client workers (default: 100)")
    
    (options, args) = parser.parse_args(argv)
    
    port = int(options.port)
    max_clients = int(options.max_clients)
    
    logging.info(f"Initializing ROSfs DHCP Scheduler on port {port}...")
    logging.info(f"Max concurrent clients: {max_clients}")
    logging.info(f"Worker port range: {port + 1} - {port + max_clients}")

    # 创建 DHCP 配置选项
    dhcp_options = DHCPOptions(port=port, max_clients=max_clients)
    scheduler = None

    # 定义信号处理函数 (Ctrl+C)
    def signal_handler(signum, frame):
        logging.info("\nReceived Signal (Ctrl+C). Shutting down DHCP Scheduler...")
        if scheduler:
            scheduler.stop()
        sys.exit(0)

    # 注册信号
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # 实例化并启动 DHCP Scheduler
        scheduler = DHCP_Scheduler(dhcp_options)
        scheduler.listen()  # 这是一个阻塞循环
    except zmq.error.ZMQError as e:
        logging.error(f"ZMQ Error during startup: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"DHCP Scheduler crashed: {e}")
        import traceback
        traceback.print_exc()
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