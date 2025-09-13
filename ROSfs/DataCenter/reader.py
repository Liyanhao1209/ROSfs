from abc import ABC, abstractmethod
from typing import List,Any
import os,time,concurrent.futures

import rosbag
from cv_bridge import CvBridge
import cv2

class Converter(ABC):
    def __init__(self):
        ...
    
    def convert(self,msg:Any):
        raise NotImplementedError()
      
class PngConverter(Converter):
    def __init__(self):
        self._inner = CvBridge()
    
    def convert(self, msg):
        return self._inner.imgmsg_to_cv2(msg,desired_encoding="mono8" if msg.encoding is None else msg.encoding)
        
class BagReader(ABC):
    def __init__(self,path:str,converter:Converter):
        self._path = path
        self._converter = converter
    
    @abstractmethod
    def read_messages(self,topics:List[str],start_offset:float,end_offset:float,raw:bool=False,return_connection_header:bool=False):
        raise NotImplementedError()
    
    @abstractmethod
    def _get_handler(self)->Any:
        raise NotImplementedError()
    
    @abstractmethod
    def get_topic_lists(self)->List[str]:
        raise NotImplementedError()
    
    @abstractmethod
    def get_start_time(self)->float:
        raise NotImplementedError()
    
    @abstractmethod
    def get_end_time(self)->float:
        raise NotImplementedError()
    
    def get_duration(self)->float:
        return self.get_end_time() - self.get_start_time()
    
class ROSfsReader(BagReader):
    def __init__(self, path, converter):
        super().__init__(path, converter)
        
    def valid(self):
        return self._get_handler() is not None
    
    def empty(self):
        return self.get_start_time is None
    
    def _get_handler(self):
        try:
            return rosbag.Bag(self._path,'rosfs')
        except Exception as e:
            return None
    
    def get_start_time(self):
        try:
            return self._get_handler().get_start_time()
        except Exception as e:
            return None
    
    def get_end_time(self):
        try:
            return self._get_handler().get_end_time()
        except Exception as e:
            return None
    
    def get_topic_lists(self):
        try:
            return [tp.topic for tp in self._get_handler().get_connections()]
        except Exception as e:
            return None
    
    def read_messages(self,topics, start_offset, end_offset, raw = False, return_connection_header = False):
        try:
            return self._get_handler().read_messages(topics,start_offset,end_offset,raw,return_connection_header)
        except Exception as e:
            return None
    
    def read_msg2png(self,topics,start_offset,end_offset,raw=False,return_connection_header=False,save_pth=None)->List[Any]:
        try:
            res = []
            for topic,msg,timestamp,*_ in self.read_messages(topics,start_offset,end_offset,raw,return_connection_header):
                if topic in ["/rosout","/rosout_agg"]:
                    continue
                res.append(
                    (topic,self._converter.convert(msg),timestamp)
                )

            if save_pth:
                if not os.path.exists(save_pth):
                    os.makedirs(save_pth)
                for topic,cv_image,timestamp in res:
                    image_pth = os.path.join(save_pth,f"{topic.replace('/','')}_{timestamp}.png")
                    cv2.imwrite(image_pth,cv_image)
        
            return res
        except Exception as e:
            print(e,flush=True)
            return None
        

if __name__ == "__main__":
    # demo here
    
    def read_and_save(src:str,target:str):
        rosfs_handler = ROSfsReader(src,PngConverter())
        while not rosfs_handler.valid() or rosfs_handler.empty():
            print("waiting for valid",flush=True)
            time.sleep(1)
        
        st = 0
        cnt = 0
        while cnt<=5:
            et = rosfs_handler.get_end_time()
            if et is None:
                time.sleep(1)
                continue
            topics = rosfs_handler.get_topic_lists()
            res = rosfs_handler.read_msg2png(rosfs_handler.get_topic_lists(),st,et+1,save_pth=target)
            print(f"{st},{et},{topics}")
            st = et
            print(f"read png {len(res)}",flush=True)
            cnt += 1
            time.sleep(0.5)
            
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(read_and_save,"/workspace/ROSfs/DataCenter/record.bag","/workspace/ROSfs/DataCenter/record_image"),
            # executor.submit(read_and_save,"/workspace/ROSfs/DataCenter/172.19.0.4.bag","/workspace/ROSfs/DataCenter/172.19.0.4.image")
        ]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(e,flush=True)