import rosbag
import time,sys,random

tps = {
        'lcamera':"/davis/left/camera_info",
        'levent':"/davis/left/events",
        'limage':"/davis/left/image_raw",
        'limu':"/davis/left/imu",
        'revent':"/davis/right/events",
        'rimu':"/davis/right/imu",
        'point_cloud':"/velodyne_point_cloud",
        'cust_imu':"/visensor/cust_imu",
        'imu':"/visensor/imu",
        'lcamera2':"/visensor/left/camera_info",
        'limage2':"/visensor/left/image_raw",
        'rcamera2':"/visensor/right/camera_info",
        'rimage2':"/visensor/right/image_raw"
    }

groups = [
    tps["limage"],
    tps["limage2"],
    tps["imu"],
    [    tps["limage"],
    tps["limage2"]],
    [    tps["limage"],
    tps["limage2"],
    tps["imu"]]
]

if __name__ == "__main__":
    print('Usage: python3 qh_test.py /absolute_path/to/your/bag(or rosfs container)')
    print('Make Sure your bag has recorded more than 110s msg data since we\'use a 0-100s random offset')
    bak = sys.argv[1]
    rosfs_bag = rosbag.Bag(bak,'rosfs')
    for g in groups:
        cst = rosfs_bag.get_start_time()
        qst,qduration = random.randint(0,100)+cst,random.randint(1,8)
        qet = qst+qduration
        st = time.time()
        cnt = 0
        for _,_,_ in rosfs_bag.read_messages(g,qst,qet):
            cnt += 1
        et = time.time()
        print(et-st,cnt)