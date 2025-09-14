import zmq
import time

def main():
    # 创建一个 ZeroMQ 上下文
    context = zmq.Context()

    # 创建一个响应套接字
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5555")  # 绑定到端口 5555

    print("Server is running...")

    while True:
        # 等待客户端的请求
        message = socket.recv()
        print(f"Received request: {message.decode()}")

        # 模拟一些处理时间
        time.sleep(1)

        # 发送响应
        socket.send(b"Hello from server")

if __name__ == "__main__":
    main()