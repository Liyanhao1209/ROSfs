import zmq
import pickle

def pull_data():
    # 创建一个ZMQ上下文
    context = zmq.Context()
    
    # 创建一个PULL套接字
    socket = context.socket(zmq.PULL)
    
    # 连接到node2的PUSH端（假设node2的IP为172.19.0.4，端口为5556）
    socket.connect("tcp://172.19.0.4:5556")
    
    print("PULL端已启动，等待接收数据...")
    
    try:
        while True:
            # 接收数据
            serialized_data = socket.recv()
            
            # 反序列化数据
            data = pickle.loads(serialized_data)
            
            print(f"已接收数据: {data}")
    except KeyboardInterrupt:
        print("PULL端已停止")
    finally:
        # 关闭套接字和上下文
        socket.close()
        context.term()

if __name__ == "__main__":
    pull_data()