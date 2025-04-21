#include <string>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

#include <boost/thread/xtime.hpp>
#include <boost/chrono.hpp>

#include "rosbag/offloader.h"



namespace rosbag {


Offloader::Offloader(std::string iw_name, std::set<std::string> topics, int buffer_size, int signal_thr) :
is_monitoring(false), 
iw_name_(iw_name), 
signal_threshold(signal_thr),
db_buffer(SignalBuffer(buffer_size)),
curSignal(new signalInfo())
{
    for (auto t : topics) {
        topic_weight[t] = 0;
    }
};


void Offloader::insertTopic(const std::string topic) {
    topic_weight[topic] = 0;
    if (topic.find("image") != std::string::npos || topic.find("lidar") != std::string::npos) {
        topic_weight[topic] = 5;
    } else {
        topic_weight[topic] = 1;
    }
}

void Offloader::updateTopics(std::set<std::string> topics) {
    // _topics = topics;
    for (auto t : topics) {
        topic_weight[t] = 0;
    }
    parseTopics();
}

void Offloader::printTopics() {
    for (auto kv : topic_weight) {
        std::cout<<kv.first <<" "<<kv.second<<std::endl;
    }
}

void Offloader::startMonitor() { 
    is_monitoring = true;
    monitor_thread = boost::thread([this]() {
        // boost::posix_time::seconds sec_time(0.5);
        while (is_monitoring == true) {
            getWifiInfo(iw_name_.c_str());
            boost::this_thread::sleep_for(boost::chrono::milliseconds(100));
        }
    });
}

int Offloader::getCurSignalLevel(){ 
    return curSignal->level;
}

void Offloader::stopMonitor() { 
    is_monitoring = false;
    // printBuffer();
    db_buffer.print_cur_level();
    monitor_thread.join();
}


// return a concatenated
std::string Offloader::getSelectedTopics() { 
    std::string topics = "";
    for (auto t : selected_topics) {
        topics += t + " "; 
    }
    return topics;
}


/* Thanks to the blog of Adam Hodges: 
   http://blog.ajhodges.com/2011/10/using-ioctl-to-gather-wifi-information.html */
void Offloader::getWifiInfo(const char *iwname) { 
    iwreq req;
    strcpy(req.ifr_name, iwname);

    //have to use a socket for ioctl
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    //make room for the iw_statistics object
    req.u.data.pointer = (iw_statistics *)malloc(sizeof(iw_statistics));
    req.u.data.length = sizeof(iw_statistics);

    // this will gather the signal strength
    if(ioctl(sockfd, SIOCGIWSTATS, &req) == -1) {
        //die with error, invalid interface
        std::cerr<<"Invalid interface"<<std::endl;        
    }
    else if(((iw_statistics *)req.u.data.pointer)->qual.updated & IW_QUAL_DBM) {
        //signal is measured in dBm and is valid for us to use
        int cur_level = ((iw_statistics *)req.u.data.pointer)->qual.level - 256;
        db_buffer.insert(cur_level);
        // recent_levels.push_back(cur_level);
        curSignal->level = cur_level;
        // curSignal->level=((iw_statistics *)req.u.data.pointer)->qual.level - 256;
    }

    //SIOCGIWESSID for ssid
    char buffer[32];
    memset(buffer, 0, 32);
    req.u.essid.pointer = buffer;
    req.u.essid.length = 32;
    //this will gather the SSID of the connected network
    if (ioctl(sockfd, SIOCGIWESSID, &req) == -1) { 
        //die with error, invalid interface
        return;
    }
    else {
        memcpy(&curSignal->ssid, req.u.essid.pointer, req.u.essid.length);
        memset(&curSignal->ssid[req.u.essid.length], 0, 1);
    }

    //SIOCGIWRATE for bits/sec (convert to mbit)
    // //this will get the bitrate of the link
    // if (ioctl(sockfd, SIOCGIWRATE, &req) == -1) {
    //     std::cerr<<"bitratefail"<<std::endl;        
    //     return;
    // } else {
    //     memcpy(&bitrate, &req.u.bitrate, sizeof(int));
    //     curSignal->bitrate=bitrate/1000000;
    // }
}

void Offloader::parseTopics() { 
    for (auto tw : topic_weight) {
        if (tw.first.find("image") != std::string::npos || tw.first.find("lidar") != std::string::npos) {
            tw.second = 5;
        } else {
            tw.second = 1;
        }
    }
}

void Offloader::selectTopics() { 
    for (auto tw: topic_weight) {
        if (tw.second > 1 && db_buffer.get_cur_level() > signal_threshold) {
            selected_topics.emplace(tw.first);
        }
    }
}


} // namespace rosbag
