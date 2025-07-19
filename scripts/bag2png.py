import rosbag
from cv_bridge import CvBridge
import os
import argparse,cv2,tqdm

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bag",'-b',type = str, nargs = 1,help = "input bag file",required=True)
    parser.add_argument("--output",'-o', type = str, nargs=1, help="output png directory path", required=True)
    
    args = parser.parse_args()
    bag_path = args.bag[0]
    output = args.output[0]
    
    try:
        os.mkdir(output)
    except Exception as e:
        print(e)
    
    bag = rosbag.Bag(bag_path)
    bridge = CvBridge()
    
    bar = tqdm.tqdm(total=bag.get_message_count(),desc="writing rgb images")
    
    for tp,msg,ts in bag.read_messages():
        try:
            assert type(tp)==str
            dirname = os.path.join(output,tp.replace("/","_"))
            if not os.path.exists(dirname):
                os.mkdir(dirname)
        except Exception as e:
            ...
        
        cv_image = bridge.imgmsg_to_cv2(msg, desired_encoding="mono8" if msg.encoding is None else msg.encoding)
        cv2.imwrite(os.path.join(dirname,f"{ts}.png"),cv_image)
        bar.update(1)