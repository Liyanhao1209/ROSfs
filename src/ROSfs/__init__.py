# ROSfs - Distributed Robot Storage Middleware
# 
# This package provides a messaging library for distributed bag file access
# across multiple robot nodes.

# 延迟导入，避免循环依赖
# 当 rosbag.bag 导入 tag_manager，而 tag_manager 导入 rosfs_timekv 时
# 如果此时 ROSfs 已经在导入 rosbag.bag，就会形成循环

def __getattr__(name):
    """延迟导入模块属性"""
    if name == 'ROSfsWorker':
        from .worker import ROSfsWorker
        return ROSfsWorker
    elif name == 'ROSfsWorkerException':
        from .worker import ROSfsWorkerException
        return ROSfsWorkerException
    elif name == 'worker_cmd':
        from .worker import worker_cmd
        return worker_cmd
    elif name == 'DHCP_Scheduler':
        from .dhcp import DHCP_Scheduler
        return DHCP_Scheduler
    elif name == 'DHCPOptions':
        from .dhcp import DHCPOptions
        return DHCPOptions
    elif name == 'PortAllocator':
        from .dhcp import PortAllocator
        return PortAllocator
    elif name == 'DHCPException':
        from .dhcp import DHCPException
        return DHCPException
    elif name == 'dhcp':
        from .dhcp import dhcp
        return dhcp
    elif name == 'ROSfsClient':
        from .client import ROSfsClient
        return ROSfsClient
    elif name == 'MissConnectedException':
        from .client import MissConnectedException
        return MissConnectedException
    elif name == 'DHCPAllocateException':
        from .client import DHCPAllocateException
        return DHCPAllocateException
    elif name == 'RemoteReadException':
        from .client import RemoteReadException
        return RemoteReadException
    elif name == 'ROSfsmain':
        from .ROSfs_main import ROSfsmain
        return ROSfsmain
    raise AttributeError(f"module 'ROSfs' has no attribute '{name}'")

__all__ = [
    # Worker
    'ROSfsWorker',
    'ROSfsWorkerException', 
    'worker_cmd',
    # DHCP
    'DHCP_Scheduler',
    'DHCPOptions',
    'PortAllocator',
    'DHCPException',
    'dhcp',
    # Client
    'ROSfsClient',
    'MissConnectedException',
    'DHCPAllocateException',
    'RemoteReadException',
    # Main
    'ROSfsmain',
]