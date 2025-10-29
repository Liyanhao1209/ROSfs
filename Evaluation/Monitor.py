import argparse,pickle,os,time,json,concurrent.futures
import zmq

import rosbag

ap = "/aerial/pose"
ai = "/aerial/rgb_image"

vp = "/vehicle/pose"
vi = "/vehicle/rgb_image"

def push(bag_backend:str):
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.bind("tcp://*:5555")
    
    for i,rosfs_dir in enumerate(os.listdir(bag_backend)):
        pth = os.path.join(bag_backend,rosfs_dir)
        handler = rosbag.Bag(pth,'rosfs')
        start_time = handler.get_start_time()
        
        for tp1,m1,ts1,tp2,m2,ts2 in zip(
            handler.read_messages([ai],raw=True,return_connection_header=False),
            handler.read_messages([ap],raw=True,return_connection_header=False)
        ):
            socket.send_multipart(
                (
                    pickle.dumps([m1,m2]),
                    pickle.dumps(ts2-start_time),
                    pickle.dumps(time.time()),
                    pickle.dumps(i)
                )
            )
        for tp1,m1,ts1,tp2,m2,ts2 in zip(
            handler.read_messages([vi],raw=True,return_connection_header=False),
            handler.read_messages([vp],raw=True,return_connection_header=False)
        ):
            socket.send_multipart(
                (
                    pickle.dumps([m1,m2]),
                    pickle.dumps(ts2-start_time),
                    pickle.dumps(time.time()),
                    pickle.dumps(i)
                )
            )
    
    socket.send_multipart(
        b'kill',b'kill',b'kill',b'kill'
    )
    socket.close()
    context.term()
                
def pull(total_time:int,partition:int,bag_backend:str):
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect("tcp://localhost:5555") 
    
    time_sequence = []
    while True:
        receive_time = time.time()
        msg,collect_time,send_time,serial = socket.recv_multipart()
        if msg == b'kill':
            break
        msg = pickle.loads(msg)

        collect_time = pickle.loads(collect_time)
        send_time = pickle.loads(send_time)
        serial = pickle.loads(serial)
        
        time_sequence.append(
            (collect_time,receive_time-send_time + (serial+1)*total_time/partition,len(msg[0])+len(msg[1]))
        )
    
    socket.close()
    context.term()
    
    with open(os.path.join(bag_backend,'monitor.aoi')) as f:
        data = {
            "delta": 0,
            "time_sequence": time_sequence
        }
        
        json.dump(data,f,indent=4)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bag",'-b',type = str, nargs = 1,help = "input bag file",required=True)
    parser.add_argument("--ttime",'-tt',type = int , nargs = 1 ,help = "total data collection time" , required=True)
    parser.add_argument('--partition','-p',type = int , nargs = 1 ,help="sub domain quantities", required=True)
    
    args = parser.parse_args()
    bag_path = args.bag[0]
    totaltime = args.ttime[0]
    partition = args.partition[0]
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        futures.append(executor.submit(pull,totaltime,partition,bag_path))
        futures.append(executor.submit(push,bag_path))
        
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Thread failed with {e}")