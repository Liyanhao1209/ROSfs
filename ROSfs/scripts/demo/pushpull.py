import zmq
import threading

def push_thread():
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.bind("tcp://*:5555") 

    socket.send_multipart([b'kill',b'kill'])

    socket.close()
    context.term()

def pull_thread():
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect("tcp://localhost:5555") 

    while True:
        message1,message2 = socket.recv_multipart()

        if message1 == b"kill" and message2==b'kill':
            break

    socket.close()
    context.term()

if __name__ == "__main__":
    push = threading.Thread(target=push_thread)
    pull = threading.Thread(target=pull_thread)

    push.start()
    pull.start()

    push.join()
    pull.join()