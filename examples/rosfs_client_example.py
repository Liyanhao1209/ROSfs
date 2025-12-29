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
    读取远程 bag 中的图像消息并保存为 PNG 文件
    """
    import io
    import numpy as np
    from PIL import Image
    from ROSfs import ROSfsClient
    
    # 配置
    REMOTE_IP = "172.17.0.4"                            # 远程节点 IP
    BAG_PATH = "/data/data/calibration.bag"    # 远程 bag 文件路径
    IMAGE_TOPIC = "/alphasense/cam0/image_raw"          # 图像 topic
    OUTPUT_DIR = "./output_images"                      # 输出目录
    
    # 创建输出目录
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 创建客户端并连接
    client = ROSfsClient(timeout_ms=30000) 
    port = None
    
    try:
        client.connect2dhcp(REMOTE_IP)
        port = client.allocate(REMOTE_IP)
        client.mount(REMOTE_IP, port, BAG_PATH)
        print(f"[✓] 已连接到 {REMOTE_IP}, 挂载 {BAG_PATH}")
        
        # 读取图像消息
        count = 0
        for topic, datatype, data_bytes, timestamp in client.read_messages(
            REMOTE_IP, port,
            topics=[IMAGE_TOPIC],
            start_time=0.0,
            end_time=10.0       # 读取前 10 秒的图像
        ):
            count += 1
            print(topic,datatype,len(data_bytes),timestamp)
            # 解析 sensor_msgs/Image 消息
            # raw 模式下 data_bytes 是序列化的消息数据
            try:
                img = deserialize_image(data_bytes, datatype)
                if img is not None:
                    # 保存为 PNG
                    output_path = os.path.join(OUTPUT_DIR, f"frame_{count:06d}_{timestamp:.3f}.png")
                    img.save(output_path)
                    print(f"[{count}] 保存: {output_path} ({img.size[0]}x{img.size[1]})")
                else:
                    print(f"[{count}] 跳过: 无法解析图像 @ {timestamp:.3f}s")
            except Exception as e:
                print(f"[{count}] 错误: {e}")
            
            # 限制数量（可选）
            if count >= 100:
                print("已达到最大数量限制")
                break
        
        print(f"\n完成! 共保存 {count} 张图像到 {OUTPUT_DIR}")
        
    except Exception as e:
        print(f"错误: {e}")
    finally:
        if port is not None:
            client.deallocate(REMOTE_IP, port)
        client.close()


def deserialize_image(data_bytes, datatype):
    """
    反序列化 ROS 图像消息为 PIL Image
    
    支持的格式:
    - sensor_msgs/Image
    - sensor_msgs/CompressedImage
    """
    import io
    import struct
    import numpy as np
    from PIL import Image
    
    if 'CompressedImage' in datatype:
        # CompressedImage: 直接是 JPEG/PNG 数据
        # 格式: header + format(string) + data
        # 简化处理：尝试找到图像数据
        try:
            # 跳过 header 和 format 字符串，尝试解码
            # CompressedImage 的 data 字段通常在末尾
            img = Image.open(io.BytesIO(data_bytes))
            return img
        except:
            # 尝试查找 JPEG/PNG 魔数
            jpeg_start = data_bytes.find(b'\xff\xd8\xff')
            png_start = data_bytes.find(b'\x89PNG')
            
            if jpeg_start >= 0:
                img = Image.open(io.BytesIO(data_bytes[jpeg_start:]))
                return img
            elif png_start >= 0:
                img = Image.open(io.BytesIO(data_bytes[png_start:]))
                return img
        return None
    
    elif 'Image' in datatype:
        # sensor_msgs/Image 格式:
        # header (序列化的 std_msgs/Header)
        # height (uint32)
        # width (uint32)  
        # encoding (string)
        # is_bigendian (uint8)
        # step (uint32)
        # data (uint8[])
        
        try:
            offset = 0
            
            # 跳过 header (seq + stamp + frame_id)
            # seq: uint32
            offset += 4
            # stamp: uint32 + uint32
            offset += 8
            # frame_id: uint32(len) + string
            frame_id_len = struct.unpack('<I', data_bytes[offset:offset+4])[0]
            offset += 4 + frame_id_len
            
            # height, width
            height = struct.unpack('<I', data_bytes[offset:offset+4])[0]
            offset += 4
            width = struct.unpack('<I', data_bytes[offset:offset+4])[0]
            offset += 4
            
            # encoding: uint32(len) + string
            encoding_len = struct.unpack('<I', data_bytes[offset:offset+4])[0]
            offset += 4
            encoding = data_bytes[offset:offset+encoding_len].decode('utf-8')
            offset += encoding_len
            
            # is_bigendian
            is_bigendian = struct.unpack('<B', data_bytes[offset:offset+1])[0]
            offset += 1
            
            # step
            step = struct.unpack('<I', data_bytes[offset:offset+4])[0]
            offset += 4
            
            # data: uint32(len) + raw pixels
            data_len = struct.unpack('<I', data_bytes[offset:offset+4])[0]
            offset += 4
            pixel_data = data_bytes[offset:offset+data_len]
            
            # 根据 encoding 转换为图像
            img = convert_ros_image_to_pil(pixel_data, width, height, encoding)
            return img
            
        except Exception as e:
            print(f"解析 Image 失败: {e}")
            return None
    
    return None


def convert_ros_image_to_pil(pixel_data, width, height, encoding):
    """
    将 ROS 图像数据转换为 PIL Image
    """
    import numpy as np
    from PIL import Image
    
    encoding = encoding.lower()
    
    if encoding in ['mono8', '8uc1']:
        # 灰度图
        arr = np.frombuffer(pixel_data, dtype=np.uint8).reshape((height, width))
        return Image.fromarray(arr, mode='L')
    
    elif encoding in ['mono16', '16uc1']:
        # 16位灰度图
        arr = np.frombuffer(pixel_data, dtype=np.uint16).reshape((height, width))
        # 归一化到 8 位
        arr = (arr / 256).astype(np.uint8)
        return Image.fromarray(arr, mode='L')
    
    elif encoding in ['rgb8', '8uc3']:
        arr = np.frombuffer(pixel_data, dtype=np.uint8).reshape((height, width, 3))
        return Image.fromarray(arr, mode='RGB')
    
    elif encoding == 'rgba8':
        arr = np.frombuffer(pixel_data, dtype=np.uint8).reshape((height, width, 4))
        return Image.fromarray(arr, mode='RGBA')
    
    elif encoding in ['bgr8']:
        arr = np.frombuffer(pixel_data, dtype=np.uint8).reshape((height, width, 3))
        # BGR -> RGB
        arr = arr[:, :, ::-1]
        return Image.fromarray(arr, mode='RGB')
    
    elif encoding == 'bgra8':
        arr = np.frombuffer(pixel_data, dtype=np.uint8).reshape((height, width, 4))
        # BGRA -> RGBA
        arr = arr[:, :, [2, 1, 0, 3]]
        return Image.fromarray(arr, mode='RGBA')
    
    elif encoding in ['bayer_rggb8', 'bayer_bggr8', 'bayer_gbrg8', 'bayer_grbg8']:
        # Bayer 格式，简单处理为灰度
        arr = np.frombuffer(pixel_data, dtype=np.uint8).reshape((height, width))
        return Image.fromarray(arr, mode='L')
    
    else:
        # 未知格式，尝试作为灰度图处理
        print(f"未知编码格式: {encoding}, 尝试作为灰度图处理")
        try:
            arr = np.frombuffer(pixel_data, dtype=np.uint8).reshape((height, width))
            return Image.fromarray(arr, mode='L')
        except:
            return None


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
