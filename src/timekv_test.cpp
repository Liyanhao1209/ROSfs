#include "rosbag/KeyValueStore.h"
#include "rosbag/container.h"

#include <cstdlib>
#include <iostream>

#include <chrono>
#include <fcntl.h>
#include <limits.h>
#include <string.h>
#include <unistd.h>

#include "rosbag/datatype.h"

int main(int argc, char **argv) {
    if (argc < 2) {
        std::cerr << "Not enough arguments" << std::endl;
        return EXIT_FAILURE;
    }

    if (strcmp(argv[2],"check")==0){
        std::cout<<"check "<<argv[1]<<" time_index"<<std::endl;
        int dir = open(argv[1],O_PATH);
        auto index = rosbag::KeyValueStore<rosbag::ROSTimeStamp,rosbag::TIData>::open(dir,"time_index",0x1000);
        std::cout<<"Initiating iterator"<<std::endl;
        auto iter = index.lower_bound(rosbag::ROSTimeStamp{0,0});
        std::cout<<"Iterator is ready"<<std::endl;
        uint64_t cnt = 0;
        for(auto cur = iter.next();cur!=nullptr;cur=iter.next()){
            cnt += 1;
            auto entry = static_cast<const rosbag::kvPair*>(cur);
            std::cout<<"Key="<<entry->ts<<";Value="<<"conn="<<entry->data.conn<<",offset="<<entry->data.offset<<std::endl;
        }
        std::cout<<"total k/v pairs cnt="<<cnt<<std::endl;
        return 0;
    }

    // open the container path
    int dir = open(argv[1], O_PATH);



    if (strcmp(argv[2], "insert") == 0) {
        auto index =
            rosbag::KeyValueStore<uint64_t,uint64_t>::create(dir, "test_index", 0x1000);
        auto start = std::chrono::steady_clock::now();
        for (uint64_t i = 0; i < 1000000; i++) {
            index.insert(i, 2*i);
        }
        auto end = std::chrono::steady_clock::now();
        std::chrono::duration<double> insert_time = end - start;
        std::cout << "Insert 1000000 keys took " << insert_time.count()
                  << " seconds" << std::endl;
        if (index.get_fd() == -1) {
            close(dir);
            return EXIT_FAILURE;
        }
    }

    else if (strcmp(argv[2], "lookup") == 0) {
        uint64_t insertion = 1000000;
        std::cout<<"ready for "<<insertion<<"insertion"<<std::endl;
        auto index =
            rosbag::KeyValueStore<uint64_t,uint64_t>::create(dir, "test_index", 0x1000);
        for (uint64_t i = 0; i < 1000000; i++) {
            index.insert(i, i);
            
        }
        auto start = std::chrono::steady_clock::now();
        auto iter = index.lower_bound(0);
        uint64_t cnt = 0;
        for(auto cur = iter.next();cur!=nullptr;cur = iter.next()){
            cnt += 1;
        }
        auto end = std::chrono::steady_clock::now();
        std::chrono::duration<double> lookup_time = end - start;
        std::cout << "Lookup all keys from 1000000 keys took "
                  << lookup_time.count() << " seconds" << std::endl;
        if (index.get_fd() == -1) {
            close(dir);
            return EXIT_FAILURE;
        }
    }
    else if (strcmp(argv[2],"beat_int")==0){
        auto index =
            rosbag::KeyValueStore<uint64_t,uint64_t>::create(dir, "test_index", 0x1000);
        index.print_b_plus_tree();
        for (uint64_t i = 0; i < 500000; i++) {
            index.insert(i,2*i);
            // index.print_b_plus_tree();
        }
        for (uint64_t i = 500000; i < 1000000; i++) {
            index.insert(i,2*i);
        }
        // index.print_b_plus_tree();
        auto iter = index.lower_bound(0);
        uint64_t cnt = 0;
        for(auto cur = iter.next();cur!=nullptr;cur=iter.next()){
            auto entry = static_cast<const rosbag::multiKey*>(cur);
            std::cout<<"Key="<<entry->key1<<";Value="<<entry->key2<<std::endl;
            cnt += 1;
        }
        std::cout<<"total k/v pairs cnt="<<cnt<<std::endl;
    }
    else if (strcmp(argv[2],"beat")==0){
        auto index =
            rosbag::KeyValueStore<rosbag::multiKey,rosbag::multiValue>::create(dir, "test_index", 0x1000);
        index.print_b_plus_tree();
        for (uint64_t i = 0; i < 500000; i++) {
            index.insert(rosbag::multiKey{1,i},rosbag::multiValue{1,2*i});
            // index.print_b_plus_tree();
        }
        for (uint64_t i = 0; i < 500000; i++) {
            index.insert(rosbag::multiKey{2,i},rosbag::multiValue{2,2*i});
        }
        // index.print_b_plus_tree();
        auto iter = index.lower_bound(rosbag::multiKey{0,0});
        uint64_t cnt = 0;
        for(auto cur = iter.next();cur!=nullptr;cur=iter.next()){
            auto entry = static_cast<const rosbag::Entry*>(cur);
            std::cout<<"Key="<<entry->k<<";Value="<<entry->v<<std::endl;
            cnt += 1;
        }
        std::cout<<"total k/v pairs cnt="<<cnt<<std::endl;
    }

    return EXIT_SUCCESS;
}