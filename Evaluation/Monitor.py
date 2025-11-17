import argparse,pickle,os,time,json,concurrent.futures
import zmq
import threading

import rospy,rosbag

ap = "/aerial/pose"
ai = "/aerial/rgb_image"
vp = "/vehicle/pose"
vi = "/vehicle/rgb_image"

time_sequence = []
time_sequence_lock = threading.Lock()

def time_to_epoch(time_obj):
    return time_obj.secs + time_obj.nsecs * 1e-9

def push_aerial(bag_backend:str):
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.bind("tcp://*:5555")
    
    for i,rosfs_dir in enumerate(sorted(os.listdir(bag_backend))):
        print(f"aerial:{rosfs_dir},{i}")
        pth = os.path.join(bag_backend,rosfs_dir)
        handler = rosbag.Bag(pth,'rosfs')
        if not i:
            start_time = handler.get_start_time()
            # print(f"aerial start_time:{start_time}")
        
        for tup1,tup2 in zip(
            handler.read_messages([ai],raw=True,return_connection_header=False),
            handler.read_messages([ap],raw=True,return_connection_header=False)
        ):
            tp1,m1,ts1 = tup1
            tp2,m2,ts2 = tup2
            collect_time = ts2-rospy.Duration(start_time)
            # print(f"aerial_collect_time_{tp1}_{tp2}:{time_to_epoch(collect_time)}")
            socket.send_multipart(
                (
                    pickle.dumps([m1,m2]),
                    pickle.dumps(collect_time),
                    pickle.dumps(time.time()),
                    pickle.dumps(i),
                    pickle.dumps("aerial")
                )
            )
    
    print("aerial kill")
    socket.send_multipart(
        (b'kill',b'kill',b'kill',b'kill',b'kill')
    )
    socket.close()
    context.term()

def push_vehicle(bag_backend:str):
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.bind("tcp://*:5556")
    
    for i,rosfs_dir in enumerate(sorted(os.listdir(bag_backend))):
        print(f"vehicle:{rosfs_dir},{i}")
        pth = os.path.join(bag_backend,rosfs_dir)
        handler = rosbag.Bag(pth,'rosfs')
        if not i:
            start_time = handler.get_start_time()
            # print(f"vehicle start_time:{start_time}")
        
        for tup1,tup2 in zip(
            handler.read_messages([vi],raw=True,return_connection_header=False),
            handler.read_messages([vp],raw=True,return_connection_header=False)
        ):
            tp1,m1,ts1 = tup1
            tp2,m2,ts2 = tup2
            collect_time = ts2-rospy.Duration(start_time)
            # print(f"vehicle_collect_time_{tp1}_{tp2}:{time_to_epoch(collect_time)}")
            socket.send_multipart(
                (
                    pickle.dumps([m1,m2]),
                    pickle.dumps(collect_time),
                    pickle.dumps(time.time()),
                    pickle.dumps(i),
                    pickle.dumps("vehicle")
                )
            )
    
    print("vehicle kill")
    socket.send_multipart(
        (b'kill',b'kill',b'kill',b'kill',b'kill')
    )
    socket.close()
    context.term()

def stat_bytes(msg):
    res = 0
    for i in msg:
        try:
            res += len(i)
        except Exception as e:
            res += len(str(i))
    return res

def pull_aerial(total_time:int, partition:int, bag_backend:str):
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect("tcp://localhost:5555") 
    
    while True:
        receive_time = time.time()
        msg, collect_time, send_time, serial, topic_group = socket.recv_multipart()
        if msg == b'kill':
            break
            
        msg = pickle.loads(msg)
        collect_time = pickle.loads(collect_time)
        send_time = pickle.loads(send_time)
        serial = pickle.loads(serial)
        topic_group = pickle.loads(topic_group)
        
        latency = max(0,receive_time - send_time) + (serial + 1) * total_time / partition
        # print(f"collect_time:{time_to_epoch(collect_time)},delta:{receive_time-send_time},serial:{serial},latency:{latency}")
        data_size = stat_bytes(msg[0]) + stat_bytes(msg[1])
        
        entry = (
            time_to_epoch(collect_time),
            latency,
            data_size,
            topic_group
        )
        
        with time_sequence_lock:
            time_sequence.append(entry)
    
    socket.close()
    context.term()

def pull_vehicle(total_time:int, partition:int, bag_backend:str):
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect("tcp://localhost:5556") 
    
    while True:
        receive_time = time.time()
        msg, collect_time, send_time, serial, topic_group = socket.recv_multipart()
        if msg == b'kill':
            break
            
        msg = pickle.loads(msg)
        collect_time = pickle.loads(collect_time)
        send_time = pickle.loads(send_time)
        serial = pickle.loads(serial)
        topic_group = pickle.loads(topic_group)
        
        latency = max(0,receive_time - send_time) + (serial + 1) * total_time / partition
        data_size = stat_bytes(msg[0]) + stat_bytes(msg[1])
        
        entry = (
            time_to_epoch(collect_time),
            latency,
            data_size,
            topic_group
        )
        
        with time_sequence_lock:
            time_sequence.append(entry)
    
    socket.close()
    context.term()

def sort_time_sequence():
    with time_sequence_lock:
        time_sequence.sort(key=lambda x: (x[0], x[1], x[2]))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bag",'-b',type = str, nargs = 1,help = "input bag file",required=True)
    parser.add_argument("--ttime",'-tt',type = int , nargs = 1 ,help = "total data collection time" , required=True)
    parser.add_argument('--partition','-p',type = int , nargs = 1 ,help="sub domain quantities", required=True)
    
    args = parser.parse_args()
    bag_path = args.bag[0]
    totaltime = args.ttime[0]
    partition = args.partition[0]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        futures.append(executor.submit(pull_aerial, totaltime, partition, bag_path))
        futures.append(executor.submit(pull_vehicle, totaltime, partition, bag_path))
        futures.append(executor.submit(push_aerial, bag_path))
        futures.append(executor.submit(push_vehicle, bag_path))
        
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Thread failed with {e}")
    
    print("All threads completed, sorting time_sequence...")
    sort_time_sequence()
    
    with open(os.path.join(bag_path,f'{partition}.aoi'),'w') as f:
        data = {
            "delta": 0,
            "time_sequence": time_sequence
        }
        json.dump(data, f, indent=4)
    
    print(f"Total entries in time_sequence: {len(time_sequence)}")
    print("Data saved to monitor.aoi")