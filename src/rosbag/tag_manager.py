from __future__ import print_function

import os
import sys
import time
from rosbag import rosfs_timekv

class Entry:
    def __init__(self, conn, path, time_range=(0, 0)):
        self.conn = conn
        self.path = path
        self.time_range = time_range  # time range is the offset range in brick file, get this info from

    def __str__(self):
        return str(self.conn) + ": " + self.path + ": " + f"[{self.time_range[0]},{self.time_range[1]}]"


class TagManager:
    def __init__(self, option):
        self._option = (
            option  # single container mode or Box mode or remote container mode
        )
        self._tag_map = {}

    def open(self, file_name):
        if self._option in ["bora", "rosfs"]:
            self._container = file_name + "/"
            self.create_entries()
        else:
            sys.exit("Invalid option")

    def create_entries(self):
        splited_files = os.listdir(self._container)
        for f in splited_files:
            if "time_index" in f:
                continue
            # get topic number and its position in path string
            tag, tag_pos = self._extract_attr(f)
            entry = Entry(tag, self._container + f, (0, 0))

            if tag in self._tag_map:
                self._tag_map[tag].append(entry)
            else:
                self._tag_map[tag] = [entry]

    def topic_query(self, tags):
        result = []
        for tag in tags:
            entry = self._tag_map[tag]
            result.extend(entry)
        return result

    def time_query(self, tags, start_sec, end_sec):
        result = []
        tag_ranges = rosfs_timekv.query(
            self._container, tags, int(start_sec),  int(end_sec)
        )
        for tag, (start_off, end_off) in tag_ranges.items():
            entry = self._tag_map[tag]
            entry[0].time_range = (start_off, end_off)
            result.extend(entry)
        return result
    
    def timekv_insert(self,topic,sec,nsec,msg_type,md5sum,msg_def,msg,connection_header, raw=False):
        rosfs_timekv.insert(self._container,topic,sec,nsec,msg_type,md5sum,msg_def,msg,connection_header, raw)

    def batch_timekv_insert(self,topics,secs,nsecs,msg_types,md5sums,msg_defs,msgs,connection_headers, raw=False):
        rosfs_timekv.batch_insert(self._container,topics,secs,nsecs,msg_types,md5sums,msg_defs,msgs,connection_headers,raw)
    
    def _extract_attr(self, path):
        attr_pos = path.find(".")
        attr = int(path[:attr_pos])
        return attr, attr_pos
