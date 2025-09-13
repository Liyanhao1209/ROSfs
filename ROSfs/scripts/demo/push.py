import zmq
import time
import pickle

def push_data():
    # 创建一个ZMQ上下文
    context = zmq.Context()
    
    # 创建一个PUSH套接字
    socket = context.socket(zmq.PUSH)
    
    # 绑定到本地端口（假设端口为5556）
    socket.bind("tcp://*:5556")
    
    print("PUSH端已启动，等待发送数据...")
    
    try:
        while True:
            # 模拟发送数据
            data = {
                "timestamp": time.time(),
                "message": "Hello from node2"
            }
            
            # 序列化数据
            serialized_data = pickle.dumps(data)
            
            # 发送数据
            socket.send(serialized_data)
            
            print(f"已发送数据: {data}")
            
            # 每5秒发送一次
            time.sleep(5)
    except KeyboardInterrupt:
        print("PUSH端已停止")
    finally:
        # 关闭套接字和上下文
        socket.close()
        context.term()

if __name__ == "__main__":
    push_data()