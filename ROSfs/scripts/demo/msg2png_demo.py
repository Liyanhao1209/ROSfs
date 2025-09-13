from sensor_msgs.msg import Image
from cv_bridge import CvBridge
import cv2
import re

msg = Image()
msg.header.seq = 484
msg.header.stamp.secs = 1506119776
msg.header.stamp.nsecs = 372422853
msg.header.frame_id = ''
msg.height = 260
msg.width = 346
msg.encoding = "mono8"
msg.is_bigendian = False
msg.step = 346

with open('./davis_image_raw.txt', 'r') as f:
    content = f.read()
data_str = re.search(r'data: \[(.*?)\]', content, re.DOTALL).group(1)
msg.data = bytes(list(map(int, data_str.replace('\n', '').split(','))))

bridge = CvBridge()
cv_image = bridge.imgmsg_to_cv2(msg, desired_encoding="mono8")

cv2.imwrite("output.png", cv_image)