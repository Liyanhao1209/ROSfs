import zmq

def main():
    # 创建一个 ZeroMQ 上下文
    context = zmq.Context()

    # 创建一个请求套接字
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://172.17.0.2:5555")  # 连接到服务器端口 5555

    print("Client is running...")

    # 发送请求
    socket.send(b"Hello from client")

    # 接收响应
    message = socket.recv()
    print(f"Received reply: {message.decode()}")

if __name__ == "__main__":
    main()