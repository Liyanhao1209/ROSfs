#!/usr/bin/env python3
"""
ROSfs Client 远程访问示例脚本

此脚本演示如何使用 ROSfs Client 连接到远程节点（172.17.0.3 和 172.17.0.4），
读取远程 bag 文件中的消息。

前提条件：
1. 远程节点上需要运行 ROSfs 服务：
   在 172.17.0.3 上运行: rosfs --serve
   在 172.17.0.4 上运行: rosfs --serve
   
2. 远程节点上有可访问的 bag 文件

使用方法：
   python rosfs_client_example.py --ip 172.17.0.3 --bag /data/test.bag
   python rosfs_client_example.py --ip 172.17.0.3 --ip2 172.17.0.4 --multi
"""

import sys
import os

# 添加 ROSfs 模块路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from ROSfs import ROSfsClient, MissConnectedException, DHCPAllocateException, RemoteReadException


def basic_remote_read(ip, bag_path, topics=None, start_time=0.0, end_time=10.0):
    """
    基础示例：连接远程节点并读取 bag 消息
    
    Args:
        ip: 远程节点 IP (如 "172.17.0.3")
        bag_path: 远程 bag 文件路径
        topics: 要读取的 topic 列表，None 表示全部
        start_time: 相对起始时间（秒，相对于 bag 文件开始记录的时间）
        end_time: 相对结束时间（秒，相对于 bag 文件开始记录的时间）
    """
    # 1. 创建客户端
    client = ROSfsClient(timeout_ms=10000)
    port = None
    
    try:
        # 2. 连接到远程 DHCP 调度器（默认端口 5555）
        client.connect2dhcp(ip)
        print(f"[✓] 已连接到 DHCP: {ip}:5555")
        
        # 3. 请求分配一个 Worker 进程
        port = client.allocate(ip)
        print(f"[✓] 已分配 Worker 端口: {port}")
        
        # 4. 挂载远程 bag 文件
        client.mount(ip, port, bag_path)
        print(f"[✓] 已挂载远程 bag: {bag_path}")
        
        # 5. 读取消息（生成器模式）
        print(f"\n读取消息 (topics={topics}, time=[{start_time}, {end_time}]):")
        print("-" * 50)
        
        count = 0
        for topic, datatype, data_bytes, timestamp in client.read_messages(
            ip, port, topics, start_time, end_time
        ):
            count += 1
            print(f"  [{count}] {topic}")
            print(f"      类型: {datatype}")
            print(f"      时间: {timestamp:.6f}s")
            print(f"      数据大小: {len(data_bytes)} bytes")
            
            # 限制输出数量
            if count >= 10:
                print("  ... (更多消息省略)")
                break
        
        print("-" * 50)
        print(f"共读取 {count} 条消息")
        
        # 6. 释放 Worker
        client.deallocate(ip, port)
        port = None
        print(f"[✓] 已释放 Worker")
        
    except MissConnectedException as e:
        print(f"[✗] 连接错误: {e}")
    except DHCPAllocateException as e:
        print(f"[✗] 分配错误: {e}")
    except RemoteReadException as e:
        print(f"[✗] 读取错误: {e}")
    except Exception as e:
        print(f"[✗] 错误: {type(e).__name__}: {e}")
    finally:
        if port is not None:
            try:
                client.deallocate(ip, port)
            except:
                pass
        client.close()


def multi_node_read(nodes_config):
    """
    多节点示例：同时连接多个远程节点
    
    Args:
        nodes_config: 节点配置列表 [{"ip": "172.17.0.3", "bag": "/data/a.bag"}, ...]
    """
    with ROSfsClient(timeout_ms=10000) as client:
        workers = []
        
        try:
            # 连接所有节点
            for cfg in nodes_config:
                ip, bag = cfg["ip"], cfg["bag"]
                
                client.connect2dhcp(ip)
                port = client.allocate(ip)
                client.mount(ip, port, bag)
                
                workers.append({"ip": ip, "port": port, "bag": bag})
                print(f"[✓] {ip}:{port} -> {bag}")
            
            # 从每个节点读取数据
            for w in workers:
                print(f"\n=== {w['ip']}:{w['port']} ===")
                count = 0
                for topic, dtype, data, ts in client.read_messages(
                    w["ip"], w["port"], None, 0.0, 5.0
                ):
                    count += 1
                    if count <= 5:
                        print(f"  {topic}: {dtype} @ {ts:.3f}s")
                print(f"  共 {count} 条消息")
            
        finally:
            for w in workers:
                try:
                    client.deallocate(w["ip"], w["port"])
                except:
                    pass


def read_by_id_example(ip, bag_path, start_id=0, count=10):
    """
    按 ID 读取消息示例
    """
    with ROSfsClient(timeout_ms=10000) as client:
        client.connect2dhcp(ip)
        port = client.allocate(ip)
        
        try:
            client.mount(ip, port, bag_path)
            
            print(f"按 ID 读取: start_id={start_id}, count={count}")
            for topic, dtype, data, ts in client.read_messages_by_id(
                ip, port, None, start_id, count
            ):
                print(f"  {topic} @ {ts:.3f}s ({len(data)} bytes)")
                
        finally:
            client.deallocate(ip, port)


# ============================================================
# 简化的使用示例（复制粘贴即可使用）
# ============================================================

def simple_example():
    """
    最简单的使用示例 - 复制这段代码即可开始使用
    """
    from ROSfs import ROSfsClient
    
    # 配置
    REMOTE_IP = "172.17.0.3"      # 远程节点 IP
    BAG_PATH = "/data/test.bag"   # 远程 bag 文件路径
    
    # 创建客户端并连接
    client = ROSfsClient(timeout_ms=10000)
    client.connect2dhcp(REMOTE_IP)
    port = client.allocate(REMOTE_IP)
    client.mount(REMOTE_IP, port, BAG_PATH)
    
    # 读取消息
    for topic, datatype, data_bytes, timestamp in client.read_messages(
        REMOTE_IP, port,
        topics=None,        # None = 所有 topics
        start_time=0.0,     # 相对起始时间（秒，相对于 bag 开始）
        end_time=10.0       # 相对结束时间（秒，相对于 bag 开始）
    ):
        print(f"{topic}: {datatype} @ {timestamp}s")
    
    # 清理
    client.deallocate(REMOTE_IP, port)
    client.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="ROSfs Client 远程访问示例",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 基础用法 - 连接单个节点
  python %(prog)s --ip 172.17.0.3 --bag /data/test.bag
  
  # 指定 topic 和时间范围
  python %(prog)s --ip 172.17.0.3 --bag /data/test.bag --topics /odom /imu --start 0 --end 30
  
  # 连接多个节点
  python %(prog)s --ip 172.17.0.3 --ip2 172.17.0.4 --bag /data/test.bag --multi
  
  # 按 ID 读取
  python %(prog)s --ip 172.17.0.3 --bag /data/test.bag --by-id --start-id 0 --count 20
        """
    )
    
    parser.add_argument("--ip", default="172.17.0.3", help="远程节点 IP")
    parser.add_argument("--ip2", default="172.17.0.4", help="第二个节点 IP（用于多节点模式）")
    parser.add_argument("--bag", default="/data/test.bag", help="远程 bag 文件路径")
    parser.add_argument("--topics", nargs="*", default=None, help="要读取的 topics")
    parser.add_argument("--start", type=float, default=0.0, help="起始时间（秒）")
    parser.add_argument("--end", type=float, default=10.0, help="结束时间（秒）")
    parser.add_argument("--multi", action="store_true", help="多节点模式")
    parser.add_argument("--by-id", action="store_true", help="按 ID 读取模式")
    parser.add_argument("--start-id", type=int, default=0, help="起始消息 ID")
    parser.add_argument("--count", type=int, default=10, help="读取消息数量")
    parser.add_argument("--simple", action="store_true", help="运行最简示例")
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("ROSfs Client 远程访问示例")
    print("=" * 60)
    
    if args.simple:
        simple_example()
    elif args.multi:
        nodes = [
            {"ip": args.ip, "bag": args.bag},
            {"ip": args.ip2, "bag": args.bag},
        ]
        multi_node_read(nodes)
    elif args.by_id:
        read_by_id_example(args.ip, args.bag, args.start_id, args.count)
    else:
        basic_remote_read(args.ip, args.bag, args.topics, args.start, args.end)
