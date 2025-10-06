from .worker import DHCP_Exception,ROSfsWorker,dhcp,DHCP_Options,DHCP_Scheduler

try:
    from UserDict import UserDict  # Python 2.x
except ImportError:
    from collections import UserDict  # Python 3.x

import optparse
import sys

class ROSfsCmds(UserDict):
    def __init__(self):
        UserDict.__init__(self)
        self._description = {}
        self['help'] = self.help_cmd

    def add_cmd(self, name, function, description):
        self[name] = function
        self._description[name] = description
        
    def get_valid_cmds(self):
        str = "Available subcommands:\n"
        for k in sorted(self.keys()):
            str += "   %s  " % k
            if k in self._description.keys():
                str +="\t%s" % self._description[k]
            str += "\n"
        return str

    def help_cmd(self,argv):
        argv = [a for a in argv if a != '-h' and a != '--help']

        if len(argv) == 0:
            print('Usage: rosbag <subcommand> [options] [args]')
            print()
            print("A bag is a file format in ROS for storing ROS message data. The rosbag command can record, replay and manipulate bags.")
            print()
            print(self.get_valid_cmds())
            print('For additional information, see http://wiki.ros.org/rosbag')
            print()
            return

        cmd = argv[0]
        if cmd in self:
            
            
            self[cmd](['-h'])
        else:
            print("Unknown command: '%s'" % cmd, file=sys.stderr)
            print(self.get_valid_cmds(), file=sys.stderr)

def worker_cmd(argv):
    parser = optparse.OptionParser(usage="rosbag record TOPIC1 [TOPIC2 TOPIC3 ...]",
                                   description="Record a bag file with the contents of specified topics.",
                                   formatter=optparse.IndentedHelpFormatter())
    parser.add_option("-p","--port",dest="port",default=dhcp.DEFAULT_PORT,action="store",help="specify the dhcp shceduler port")
    
    (options,args) = parser.parse_args(argv)
    
    dhcp_port = options.port
    scheduler = DHCP_Scheduler(DHCP_Options(port=dhcp_port))
    
    scheduler.listen()
    
    
def ROSfsmain(argv=None):
    cmds = ROSfsCmds()
    cmds.add_cmd('worker',worker_cmd,"Start a worker union on this node.")
    
    if argv is None:
        argv = sys.argv

    if '-h' in argv or '--help' in argv:
        argv = [a for a in argv if a != '-h' and a != '--help']
        argv.insert(1, 'help')

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