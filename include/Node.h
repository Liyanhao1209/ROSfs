#ifndef NODE_H
#define NODE_H

#include <cstdint>
#include <iostream>
#include <cassert>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>

#include "Type.h"

namespace SpatialStorage {

    const uint64_t INVALID_ROOT_ADDR = 0;

    template <typename KeyT, typename ValueT>
    struct KeyValuePair {
        KeyT            key;
        ValueT          value;

        KeyValuePair() = default;
        KeyValuePair(const KeyValuePair& other) = default;
        
        KeyValuePair(KeyValuePair&& other) noexcept
            : key(std::move(other.key)), value(std::move(other.value)) {}
        
        KeyValuePair& operator=(const KeyValuePair& other) = default;
        
        KeyValuePair& operator=(KeyValuePair&& other) noexcept {
            if (this != &other) {
                key = std::move(other.key);
                value = std::move(other.value);
            }
            return *this;
        }
        
        KeyValuePair(const KeyT& k, const ValueT& v) : key(k), value(v) {}
        
        KeyValuePair(KeyT&& k, ValueT&& v) : key(std::move(k)), value(std::move(v)) {}
        
        KeyValuePair(const KeyT& k, ValueT&& v) : key(k), value(std::move(v)) {}
        
        KeyValuePair(KeyT&& k, const ValueT& v) : key(std::move(k)), value(v) {}
        
        ~KeyValuePair() = default;
    };

    class IndexHeader {
        public:
            uint64_t    Dimensions;
            uint64_t    key_size;
            uint64_t    value_size;
            uint64_t    block_size;
            uint64_t    root_addr;

    };

    enum class BlockType {
        LeafBlock=0,
        InnerBlock
    };

    class NodeHeader {
        private:
            BlockType   block_type_;
            uint64_t    entry_count_{0};
            uint64_t    in_file_addr_{INVALID_ROOT_ADDR};

        public:
            NodeHeader(BlockType blocktype,uint64_t entry_count,uint64_t in_file_addr)
                :block_type_{blocktype}, entry_count_{entry_count}, in_file_addr_{in_file_addr}
            {}

            auto GetEntryCount()->uint64_t { return entry_count_; }
            auto SetEntryCount(uint64_t entry_count)->void { entry_count_ = entry_count; }
            auto IsLeafBlock()->bool { return block_type_==BlockType::LeafBlock; }
            auto SetBlockType(BlockType bt)->void { block_type_ = bt; }
            auto GetInFileAddr()->uint64_t { return in_file_addr_; }
            auto SetInFileAddr(uint64_t in_file_addr)->void { in_file_addr_ = in_file_addr; }
    };

    template<typename KeyT, typename ValueT>
    class NodeHandler {
    private:
        NodeHeader* header_;
        uint64_t    key_size_;
        uint64_t    value_size_;
        uint64_t    block_size_;
        uint64_t    dimensions_;

    public:
        NodeHandler(NodeHeader *header, size_t key_size, size_t value_size, size_t block_size, size_t dimensions)
            : header_{header}, key_size_{key_size}, value_size_{value_size}, block_size_(block_size), dimensions_(dimensions)
        {}

        NodeHeader *get_header() {return header_;}
        void set_header(NodeHeader* header) {*header_ = *header;}

        bool IsLeafBlock() { return header_->IsLeafBlock(); } 
        void SetBlockType(BlockType bt) {header_->SetBlockType(bt);}

        uint64_t get_in_file_addr() { return header_->GetInFileAddr(); }
        void set_in_file_addr(uint64_t ifa) {header_->SetInFileAddr(ifa);}

        uint64_t get_count() { return header_->GetEntryCount(); }
        void set_count(uint64_t count) { header_->SetEntryCount(count); }

        uint64_t get_pair_size() const { return key_size_ + value_size_; }

        uint64_t get_entry_capacity() const {
            return (block_size_ - sizeof(NodeHeader)) / get_pair_size();
        }

        bool is_full() { return header_->GetEntryCount() >= get_entry_capacity(); }

        KeyType<KeyT> get_elem_key(uint64_t idx) {
            assert(idx < get_count());
            
            uint8_t *elem_ptr = reinterpret_cast<uint8_t *>(header_) + 
                                sizeof(NodeHeader) + 
                                idx * get_pair_size();
            
            KeyT *data_ptr = reinterpret_cast<KeyT*>(elem_ptr); 
            
            std::vector<KeyT> data(dimensions_ * 2);
            for (uint64_t i = 0; i < dimensions_ * 2; ++i) {
                data[i] = data_ptr[i];
            }
            
            return KeyType<KeyT>(data);
        }

        ValueT get_elem_value(uint64_t idx) {
            assert(idx < get_count());
            
            uint8_t *value_ptr = reinterpret_cast<uint8_t *>(header_) + 
                                sizeof(NodeHeader) + 
                                (idx * get_pair_size() + key_size_);
            
            return *reinterpret_cast<ValueT*>(value_ptr);
        }

        void *get_elem_ptr(uint64_t idx) {
            return reinterpret_cast<uint8_t *>(header_) + sizeof(NodeHeader) + idx * get_pair_size();
        }

        KeyValuePair<KeyType<KeyT>, ValueT> get_elem_pair(uint64_t idx) {
            return KeyValuePair<KeyType<KeyT>, ValueT>(get_elem_key(idx), get_elem_value(idx));
        }

        void set_elem_key(KeyType<KeyT> *modify_key, uint64_t idx) {
            assert(idx < get_count());
            assert(modify_key != nullptr);
            assert(modify_key->size() == dimensions_ * 2);
            
            uint8_t *elem_ptr = reinterpret_cast<uint8_t *>(header_) + 
                                sizeof(NodeHeader) + 
                                idx * get_pair_size();

            KeyT *data_ptr = reinterpret_cast<KeyT *>(elem_ptr);
            for (size_t i = 0; i < dimensions_ * 2; ++i) {
                data_ptr[i] = (*modify_key)[i];
            }
        }

        void set_elem_value(const ValueT& value, uint64_t idx) {
            assert(idx < get_count());
            
            uint8_t *value_ptr = reinterpret_cast<uint8_t *>(header_) + 
                                sizeof(NodeHeader) + 
                                (idx * get_pair_size() + key_size_);
            
            *reinterpret_cast<ValueT*>(value_ptr) = value;
        }

        void delete_elem_key(uint64_t idx){
            auto count = get_count();
            if (idx >= count) return;
            
            uint8_t* dest_ptr = reinterpret_cast<uint8_t*>(header_) + 
                            sizeof(NodeHeader) + idx * get_pair_size();
            uint8_t* src_ptr = dest_ptr + get_pair_size();
            size_t move_size = (count - idx - 1) * get_pair_size();
            
            if (move_size > 0) {
                memmove(dest_ptr, src_ptr, move_size);
            }
            
            set_count(count - 1);
        }

        void insert(KeyValuePair<KeyType<KeyT>, ValueT>& kvp) {
            auto capacity = get_entry_capacity();
            auto entry_count = get_count();
            assert(entry_count < capacity);
            assert(kvp.key.size() == dimensions_ * 2);
            
            uint8_t *insert_ptr = reinterpret_cast<uint8_t *>(header_) + 
                                sizeof(NodeHeader) + 
                                entry_count * get_pair_size();

            // 写入 key
            KeyT *data_ptr = reinterpret_cast<KeyT *>(insert_ptr);
            for (size_t i = 0; i < dimensions_ * 2; ++i) {
                data_ptr[i] = kvp.key[i];
            }
            
            // 写入 value
            uint8_t *value_ptr = insert_ptr + key_size_;
            *reinterpret_cast<ValueT*>(value_ptr) = kvp.value;
            
            set_count(entry_count + 1);
        }

        void clear() {
            set_count(0);
        }
    };
}

#endif