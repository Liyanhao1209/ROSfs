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
    print('Usage: python3 qt_test.py /absolute_path/to/your/bag(or rosfs container)')
    print('Make Sure your bag has recorded more than 12s msg data since we\'use a 0-12s random offset')
    bak = sys.argv[1]
    rosfs_bag = rosbag.Bag(bak,'rosfs')
    for g in groups:
        cet = rosfs_bag.get_end_time()
        qduration = random.randint(0,12)
        qst,qet = cet-qduration,cet
        st = time.time()
        cnt = 0
        for _,_,_ in rosfs_bag.read_messages(g,qst,qet):
            cnt += 1
        et = time.time()
        print(et-st,cnt)