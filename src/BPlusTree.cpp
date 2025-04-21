#include "rosbag/BPlusTree.h"

#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <errno.h>
#include <cstring>

namespace rosbag{
    /// @brief find the largest key which is smaller than a given key
    /// @param key the given key
    /// @return the index of the target key
    // template<typename KeyT>
    // size_t NodeBlock<KeyT>::lower_bound(KeyT key){
    //     size_t l=0,r=get_count(),ans = -1;
    //     size_t mid;

    //     while (l<r){
    //         mid = (l+r)/2;
    //         auto mid_k = *get_elem_key(mid);
    //         if ( mid_k < key ){
    //             ans = mid;
    //             l = mid + 1;
    //         }else{
    //             r = mid;
    //         }
    //     }

    //     return ans;
    // }

    // template<typename KeyT>
    // void NodeBlock<KeyT>::insert(KeyT key,const void *value){
    //     size_t i = get_count();
    //     for (;i>0;i--){
    //         auto key_ptr = get_elem_key(i-1);
    //         if (*key_ptr > key){
    //             memmove(reinterpret_cast<uint8_t*>(key_ptr)+get_pair_size(),key_ptr,get_pair_size());
    //         // }else if(*key_ptr == key){
    //         //     abort();
    //         }else{
    //             break;
    //         }
    //     }

    //     auto kptr = get_elem_key(i);
    //     *kptr = key;
    //     memcpy(reinterpret_cast<uint8_t*>(kptr)+sizeof(uint64_t),value,value_size_);
    // }

    // template<typename KeyT>
    // void NodeBlock<KeyT>::print_node(){
    //     std::cout<<"entry count="<<get_count()<<"/"<<get_tot_entry_count()<<"    ";
    //     auto entry_count = get_count();

    //     if (!IsLeafBlock()){
    //         for ( size_t i=0;i<entry_count;i++){
    //             std::cout<<"Pointer "<<i<<"="<<*static_cast<uint64_t *>(get_elem_value(i))<<";";
    //             std::cout<<"Key "<<i<<"="<<*get_elem_key(i)<<";";
    //         }
    //         std::cout<<"Pointer "<<get_count()<<"="<<*static_cast<uint64_t *>(get_elem_value(get_count()))<<"\t";
    //     }else{
    //         for (size_t i=0;i<entry_count;i++){
    //             std::cout<<"Key "<<i<<"="<<*get_elem_key(i)<<";";
    //             std::cout<<"Value "<<i<<"="<<*static_cast<uint64_t *>(get_elem_value(i))<<";";
    //         }
    //         std::cout<<"\t";
    //     }
    // }

    bool TimeIndexLock::try_read_lock() {
        struct flock fl;
        fl.l_type = F_RDLCK;
        fl.l_whence = SEEK_SET;
        fl.l_start = 0;
        fl.l_len = 0; // lock the entire file
        return (fcntl(fd_, F_SETLKW, &fl) == 0);
    }

    bool TimeIndexLock::try_write_lock() {
        struct flock fl;
        fl.l_type = F_WRLCK;
        fl.l_whence = SEEK_SET;
        fl.l_start = 0;
        fl.l_len = 0;
        return (fcntl(fd_, F_SETLKW, &fl) == 0);
    }

    void TimeIndexLock::unlock() {
        struct flock fl;
        fl.l_type = F_UNLCK;
        fl.l_whence = SEEK_SET;
        fl.l_start = 0;
        fl.l_len = 0;
        fcntl(fd_, F_SETLK, &fl);
    }
}