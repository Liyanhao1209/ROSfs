#ifndef ROSBAG_OFFLOADER_H
#define ROSBAG_OFFLOADER_H

#include <iostream>
#include <string>
#include <vector>
#include <set>
#include <unordered_map>
#include <numeric>

#include <linux/wireless.h>
#include <sys/ioctl.h>

#include <boost/thread.hpp>
#include <boost/circular_buffer.hpp>

namespace rosbag {

// class level {
//     int signal_strength;
//     int 
// }

class SignalBuffer {
public:
    SignalBuffer() = default;
    SignalBuffer(int capacity): 
    buffer(boost::circular_buffer<int>(capacity)), buffer_size(capacity) {}

    void insert(int level) { 
        buffer.push_back(level);
        update_avglevel();
    }
    
    int get_cur_level() { return avg_level; }
    void print_cur_level() { std::cout<<"avg_level: "<<avg_level<<std::endl;}
    void print_buffer() { 
        for (auto level : buffer) {
            std::cout<<level<<std::endl;
        }
    }

private:
    
    boost::circular_buffer<int> buffer;
    int buffer_size;
    
    int avg_level;       // calculated by sum of buffer / buffer capacity
    
    void update_avglevel() {
        if (buffer.size() == buffer.capacity()) {
            int sum = std::accumulate(buffer.begin(), buffer.end(), 0);
            avg_level =  sum / buffer_size;
            // std::cout<<"sum "<<sum<<std::endl;
            // std::cout<<"avg "<<avg_level<<std::endl;
        }             
    }
};


class Offloader 
{

public:
    Offloader() = default;
    // Offloader(std::set<std::string> topics);
    Offloader(std::string iw_name, std::set<std::string> topics, int buffer_size, int signal_thr);

    // Offloader(boost::shared_ptr<rosbag::Recorder> recorder);

    void insertTopic(const std::string topic);
    void updateTopics(std::set<std::string> topics);
    void printTopics();
    void selectTopics();

    std::string getSelectedTopics();
    int getCurSignalLevel();
    void startMonitor();
    void stopMonitor();
    // void printBuffer();

private:
    
    struct signalInfo {
        char ssid[33];
        // int bitrate;
        int level;
    };    
        
    enum process_option { 
        GET_LATEST,
        COMPRESSS,
    }; 
    
    struct topic_strategy {
        std::string topic_name;
        process_option option;
    };
    

    bool is_monitoring;                 // is the monitor thread running ?
    std::string iw_name_;               // wireless iterface name
    int signal_threshold;               // threshold of WiFi rssi

    boost::thread monitor_thread;
    
    // boost::circular_buffer<int> recent_levels;
    SignalBuffer db_buffer;
    signalInfo *curSignal;

    std::unordered_map<std::string, int> topic_weight;
    std::set<std::string> selected_topics;

    void getWifiInfo(const char *iwname);
    void parseTopics();
};


} // namespace rosbag


#endif
