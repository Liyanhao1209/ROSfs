import zmq
import threading
import time

# PUSH 端逻辑
def push_thread():
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.bind("tcp://*:5555")  # 绑定到端口 5555

    print("PUSH 端已启动，开始发送数据...")

    for i in range(10):
        message = f"消息 {i + 1}"
        print(f"发送: {message}")
        socket.send_string(message)
        time.sleep(1)

    # 发送结束信号
    socket.send_string("结束")
    print("发送结束信号，PUSH 端退出。")

    socket.close()
    context.term()

# PULL 端逻辑
def pull_thread():
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect("tcp://localhost:5555")  # 连接到端口 5555

    print("PULL 端已启动，等待接收数据...")

    while True:
        message = socket.recv_string()
        print(f"接收到: {message}")

        # 如果收到结束信号，退出
        if message == "结束":
            print("接收到结束信号，PULL 端退出。")
            break

    socket.close()
    context.term()

# 主函数
if __name__ == "__main__":
    # 创建两个线程
    push = threading.Thread(target=push_thread)
    pull = threading.Thread(target=pull_thread)

    # 启动线程
    push.start()
    pull.start()

    # 等待线程结束
    push.join()
    pull.join()

    print("所有线程已结束。")