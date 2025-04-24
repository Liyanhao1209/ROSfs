#ifndef B_PLUS_TREE_H
#define B_PLUS_TREE_H


#include <iostream>
#include <cassert>

#include <sys/mman.h>
#include <unistd.h>
#include <cstring>

namespace rosbag {

/// The file header for verify, stores some meta data for the key value
/// store.  Occupies the first file block.
class IndexHeader {
    public:
        uint64_t magic;
        uint64_t block_size;
        uint64_t key_size;
        uint64_t value_size;
        uint64_t root_addr;
};

enum class BlockType {
    LeafBlock=0,
    InnerBlock
};

class NodeHeader {
    private:
        uint64_t entry_count_{0};
        BlockType block_type_{BlockType::InnerBlock};
        uint64_t in_file_addr_{0};

    public:
        auto GetEntryCount()->uint64_t { return entry_count_; }
        auto SetEntryCount(uint64_t entry_count)->void { entry_count_ = entry_count; }
        auto IsLeafBlock()->bool { return block_type_==BlockType::LeafBlock; }
        auto SetBlockType(BlockType bt)->void { block_type_ = bt; }
        auto GetInFileAddr()->uint64_t { return in_file_addr_; }
        auto SetInFileAddr(uint64_t in_file_addr)->void { in_file_addr_ = in_file_addr; }
};

class LeafHeader: public NodeHeader {
    private:
        uint64_t prev_sibling_addr_{0};
        uint64_t next_sibling_addr_{0};

    public:
        auto GetPrevSiblingLeaf()->uint64_t { return prev_sibling_addr_; }
        auto GetNextSiblingLeaf()->uint64_t { return next_sibling_addr_; }
        auto SetPrevSiblingLeaf(uint64_t psa)->void {prev_sibling_addr_=psa;}
        auto SetNextSiblingLeaf(uint64_t nsa)->void {next_sibling_addr_=nsa;}
};

/// The class for accessing one node, holds the address of the
/// node in memory and metadata for the key value store.
template<typename KeyT>
class NodeBlock {
    private:
        NodeHeader *header_;
        size_t key_size_;
        size_t value_size_;
        size_t block_size_;
    
    public:
        NodeBlock(NodeHeader *header,size_t key_size,size_t value_size,size_t block_size)
            : header_{header}, key_size_{key_size}, value_size_{value_size}, block_size_(block_size)
            {}
        
        NodeHeader *get_header() {return header_;}

        bool IsLeafBlock() { return header_->IsLeafBlock(); } 
        void SetBlockType(BlockType bt) {header_->SetBlockType(bt);}

        uint64_t get_in_file_addr() { return header_->GetInFileAddr(); }
        void set_in_file_addr(uint64_t ifa) {header_->SetInFileAddr(ifa);}

        size_t get_count() { return header_->GetEntryCount(); }
        void set_count(uint64_t count) { header_->SetEntryCount(count); }

        size_t get_pair_size() const { return key_size_+value_size_; }
        
        size_t get_tot_entry_count() const 
            { return (block_size_-(header_->IsLeafBlock()?sizeof(LeafHeader):(sizeof(NodeHeader)+value_size_))) / get_pair_size();}
        bool is_full() { return header_->GetEntryCount() >= get_tot_entry_count(); }

        KeyT *get_elem_key(size_t idx) {
            void *res = nullptr;
            if (header_->IsLeafBlock()) {
                res = reinterpret_cast<uint8_t *>(header_) + sizeof(LeafHeader) + idx * get_pair_size();
            } else {
                res = reinterpret_cast<uint8_t *>(header_) + sizeof(NodeHeader) + (idx + 1) * value_size_ + idx * key_size_ ;
            }

            return reinterpret_cast<KeyT *>(res);
        }

        KeyT *first_key() { return get_elem_key(0); }
        KeyT *last_key() { return get_elem_key(get_count()-1); }

        void *get_elem_value(size_t idx) {
            if (header_->IsLeafBlock()) {
                return reinterpret_cast<uint8_t *>(header_) + sizeof(LeafHeader) + (idx * get_pair_size() + key_size_);
            } else {
                return reinterpret_cast<uint8_t *>(header_) + sizeof(NodeHeader) + idx * get_pair_size();
            }
        }

        void *get_elem_ptr(size_t idx) {
            // it makes no sense to get k/v pair when the node is an inner node
            // since the number of keys is 1 less than the number of values in an B+ Tree inner node
            assert(IsLeafBlock());
            return reinterpret_cast<uint8_t *>(header_) + sizeof(LeafHeader) + get_pair_size() * idx;
        }

        /// @brief find the largest key which is smaller than a given key
        /// @param key the given key
        /// @return the index of the target key
        size_t lower_bound(KeyT key){
            size_t l=0,r=get_count(),ans = -1;
            size_t mid;

            while (l<r){
                mid = (l+r)/2;
                auto mid_k = *get_elem_key(mid);
                if ( mid_k < key ){
                    ans = mid;
                    l = mid + 1;
                }else{
                    r = mid;
                }
            }

            return ans;
        };

        /// @brief find the smallest key which is larger than the given key
        /// @param key the given key
        /// @return the index of the target key
        size_t upper_bound(KeyT key){
            size_t l=0,r=get_count(),ans=get_count();
            size_t mid;

            while(l<r){
                mid = (l+r)/2;
                auto mid_k = *get_elem_key(mid);
                if (mid_k > key) {
                    ans = mid;
                    r = mid;
                }else{
                    l = mid+1;
                }
            }

            return ans;
        }

        void insert(KeyT key,const void *value){
            size_t i = get_count();
            for (;i>0;i--){
                auto key_ptr = get_elem_key(i-1);
                if (*key_ptr > key){
                    memmove(reinterpret_cast<uint8_t*>(key_ptr)+get_pair_size(),key_ptr,get_pair_size());
                // }else if(*key_ptr == key){
                //     abort();
                }else{
                    break;
                }
            }

            auto kptr = get_elem_key(i);
            *kptr = key;
            memcpy(reinterpret_cast<uint8_t*>(kptr)+sizeof(KeyT),value,value_size_);
        };

        void print_node(){
            std::cout<<"entry count="<<get_count()<<"/"<<get_tot_entry_count()<<"    ";
            auto entry_count = get_count();

            if (!IsLeafBlock()){
                for ( size_t i=0;i<entry_count;i++){
                    std::cout<<"Pointer "<<i<<"="<<*static_cast<uint64_t *>(get_elem_value(i))<<";";
                    std::cout<<"Key "<<i<<"="<<*get_elem_key(i)<<";";
                }
                std::cout<<"Pointer "<<get_count()<<"="<<*static_cast<uint64_t *>(get_elem_value(get_count()))<<"\t";
            }else{
                for (size_t i=0;i<entry_count;i++){
                    std::cout<<"Key "<<i<<"="<<*get_elem_key(i)<<";";
                    // std::cout<<"Value "<<i<<"="<<*static_cast<uint64_t *>(get_elem_value(i))<<";";
                }
                std::cout<<"\t";
            }
        };
};

class TimeIndexLock{
    private:
        int fd_;
    
    public:
        TimeIndexLock(int fd) : fd_{fd}{};
        bool try_read_lock();
        bool try_write_lock();
        void unlock();
};

}

#endif