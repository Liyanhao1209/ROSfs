#ifndef R_TREE_H
#define R_TREE_H

#include <cstdint>
#include <cassert>
#include <cstring>
#include <iostream>
#include <optional>
#include <deque>
#include <list>
#include <queue>
#include <fcntl.h>
#include <unordered_set>

#include <unistd.h>
#include <zconf.h>
#include <utility>

#include "FileCache.h"
#include "Node.h"
#include "Type.h"

namespace SpatialStorage {
    const uint64_t PAGE_UNIT = 0x1000;
    const uint64_t INDEX_HEADER_ADDR = 0x1000;

    enum class SearchMode {
        overlap=0,
        comprise
    };

    template<typename KeyT, typename ValueT>
    class Context {
        public:
            std::deque<std::pair<uint64_t, uint64_t>> path;  // <in_file_address, entry_index>
    };

    template<typename KeyT, typename ValueT>
    class RTree {
        public:
            static RTree<KeyT, ValueT> create(
                int dir,
                const char *name,
                uint64_t key_size,
                uint64_t value_size,
                uint64_t block_size,
                uint64_t dimensions) 
            {
                int index = openat(dir, name, O_RDWR | O_CREAT | O_EXCL,
                    S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);  
                if (index == -1)
                    return RTree<KeyT, ValueT>{index,key_size,value_size,block_size,dimensions};
                if (ftruncate(index, block_size) == -1) {
                    close(index);
                    return RTree<KeyT, ValueT>{index,key_size,value_size,block_size,dimensions};
                }

                RTree<KeyT, ValueT> ret{index,key_size,value_size,block_size,dimensions};

                if (ret.get_fd() != -1) {
                    *ret.get_header() = IndexHeader {
                        .Dimensions = dimensions,
                        .key_size = key_size,
                        .value_size = value_size,
                        .block_size = block_size,
                        .root_addr = INVALID_ROOT_ADDR,
                    };
                }

                return ret;
            }

            static RTree<KeyT, ValueT> open(
                int dir,
                const char *name,
                uint64_t key_size,
                uint64_t value_size,
                uint64_t block_size,
                uint64_t dimensions) 
            {
                int index = openat(dir, name, O_RDWR);
                if (index == -1)
                    return RTree<KeyT, ValueT>{index,key_size,value_size,block_size,dimensions};
                
                RTree<KeyT, ValueT> ret{index,key_size,value_size,block_size,dimensions};
                if (ret.get_fd() != -1) {
                    auto header = ret.get_header();

                    if (
                        header->key_size != key_size ||
                        header->value_size != value_size ||
                        header->block_size != block_size ||
                        header->Dimensions != dimensions
                    )
                        return RTree<KeyT, ValueT>{index,key_size,value_size,block_size,dimensions};
                }

                return ret;
            }

            int get_fd() { return index_.get_fd(); }

        
        private:
            MMAPCache::FileCache    index_;
            uint64_t                key_size_;
            uint64_t                value_size_;
            uint64_t                block_size_;
            uint64_t                dimensions_;

            RTree(
                int index,
                uint64_t key_size,
                uint64_t value_size,
                uint64_t block_size,
                uint64_t dimensions
            ): index_{index,block_size},key_size_{key_size},value_size_{value_size},block_size_{block_size},dimensions_{dimensions} {
                assert(block_size>0 && block_size % PAGE_UNIT == 0);
            }

            void collect_leaf_entries(NodeHandler<KeyT, ValueT>* handler, 
                             std::vector<KeyValuePair<KeyType<KeyT>, ValueT>>& entries) {
                if (handler->IsLeafBlock()) {
                    for (uint64_t i = 0; i < handler->get_count(); ++i) {
                        entries.push_back(handler->get_elem_pair(i));
                    }
                } else {
                    for (uint64_t i = 0; i < handler->get_count(); ++i) {
                        auto child_value = handler->get_elem_value(i);
                        auto child_addr = *reinterpret_cast<const uint64_t*>(&child_value);
                        auto child_handler = get_node_handler(child_addr);
                        collect_leaf_entries(&child_handler, entries);
                    }
                }
            }

            void print_node_recursive(NodeHandler<KeyT, ValueT>* handler, int depth) {
                std::string indent(depth * 2, ' ');
                
                std::cout << indent;
                if (depth == 0) {
                    std::cout << "ROOT ";
                } else {
                    std::cout << (handler->IsLeafBlock() ? "LEAF " : "INNER");
                }
                
                std::cout << "[addr: 0x" << std::hex << handler->get_in_file_addr() 
                        << std::dec << ", entries: " << handler->get_count() 
                        << "/" << handler->get_entry_capacity() << "]";
                
                if (handler->get_count() > 0) {
                    auto node_mbr = get_node_mbr(handler);
                    std::cout << " NodeMBR: ";
                    print_mbr_simple(node_mbr);
                }
                std::cout << std::endl;
                
                for (uint64_t i = 0; i < handler->get_count(); ++i) {
                    std::string entry_indent = indent + "  ";
                    
                    if (!handler->IsLeafBlock()) {
                        auto key = handler->get_elem_key(i);
                        std::cout << entry_indent << "Entry " << i << ": MBR";
                        print_mbr_simple(key);
                        
                        auto child_value = handler->get_elem_value(i);
                        auto child_addr = *reinterpret_cast<const uint64_t*>(&child_value);
                        std::cout << " -> Child[0x" << std::hex << child_addr << std::dec << "]" << std::endl;
                        
                        auto child_handler = get_node_handler(child_addr);
                        print_node_recursive(&child_handler, depth + 1);
                        
                    } else {
                        auto key = handler->get_elem_key(i);
                        std::cout << entry_indent << "KV " << i << ": MBR";
                        print_mbr_simple(key);
                        
                        auto value = handler->get_elem_value(i);
                        if (value_size_ > 0) {
                            std::cout << " -> Value[";
                            if (value_size_ == sizeof(uint64_t)) {
                                uint64_t value_data = *reinterpret_cast<const uint64_t*>(&value);
                                std::cout << value_data;
                            } else {
                                std::cout << "0x" << std::hex << reinterpret_cast<uint64_t>(&value) << std::dec;
                            }
                            std::cout << "]";
                        }
                        std::cout << std::endl;
                    }
                }
            }
            
            void print_mbr_simple(const KeyType<KeyT>& mbr) {
                auto size = mbr.size();
                if (size == 0) {
                    std::cout << "[]";
                    return;
                }
                
                std::cout << "[";
                for (size_t i = 0; i < size / 2; ++i) {
                    if (i > 0) std::cout << " ";
                    std::cout << "(" << mbr[i] << "," << mbr[i + size / 2] << ")";
                }
                std::cout << "]";
            }
            
            template<typename T = void> T *get_address(uint64_t address) {
                auto ret = static_cast<T *>(index_.get_block(address));

                if (ret == nullptr) {
                    abort();
                }

                return ret;
            }

            size_t get_block_size() const { return block_size_; }

            IndexHeader *get_header() { return get_address<IndexHeader>(0);}

            uint64_t get_root_addr() {
                auto index_header = get_header();
                return index_header->root_addr;
            }

            NodeHandler<KeyT, ValueT> get_node_handler(uint64_t address) {
                NodeHeader *header = get_address<NodeHeader>(address);

                return NodeHandler<KeyT, ValueT>{header, key_size_, header->IsLeafBlock() ? value_size_ : sizeof(uint64_t), block_size_, dimensions_};
            }

            // Allocate one new block.
            uint64_t allocate_block(){
                uint64_t block = index_.get_size();
                if (!index_.truncate(block + get_block_size())) {
                    abort();
                }
                return block;
            }
            
            void search(
                const KeyType<KeyT>& key,
                NodeHandler<KeyT, ValueT> *handler,
                std::vector<KeyValuePair<KeyType<KeyT>, ValueT>> *res,
                SearchMode mode) 
            {
                auto entry_cnt = handler->get_count();
                for(uint64_t i=0;i<entry_cnt;i++){
                    auto mbr = handler->get_elem_key(i);
                    if (handler->IsLeafBlock()){
                        if (
                        (mode==SearchMode::overlap && mbr.IsOverlap(key)) ||
                        (mode==SearchMode::comprise && key>=mbr)
                        ) {
                            KeyValuePair<KeyType<KeyT>, ValueT> kvp = handler->get_elem_pair(i);
                            res->push_back(kvp);
                        }
                    }
                    else{
                        if (
                            (mode==SearchMode::overlap && mbr.IsOverlap(key)) ||
                            (mode==SearchMode::comprise && mbr.IsOverlap(key))
                        ) {
                            auto child_value = handler->get_elem_value(i);
                            auto next_addr = *reinterpret_cast<const uint64_t*>(&child_value);
                            auto next_handler = get_node_handler(next_addr);
                            search(key,&next_handler,res,mode);
                        }
                    }
                    
                }
            }

            NodeHandler<KeyT, ValueT> *ChooseLeaf(
                const KeyType<KeyT>& key,
                NodeHandler<KeyT, ValueT>* handler,
                Context<KeyT, ValueT>* ctx
            ) {
                if (handler->IsLeafBlock()) {
                    std::pair<uint64_t, uint64_t> pth(handler->get_in_file_addr(), 0);
                    ctx->path.push_back(pth);
                    return handler;
                }

                KeyT *min_enlarge = nullptr;
                uint64_t min_enlarge_index = 0;

                auto entry_cnt = handler->get_count();
                for (uint64_t i=0;i<entry_cnt;i++){
                    auto mbr = handler->get_elem_key(i);
                    KeyT enlarge = mbr.enlargement(key);
                    if (min_enlarge==nullptr || enlarge < *min_enlarge) {
                        min_enlarge = &enlarge;
                        min_enlarge_index = i;
                    }
                }
                
                std::pair<uint64_t, uint64_t> pth(handler->get_in_file_addr(), min_enlarge_index);
                ctx->path.push_back(pth);
                auto child_value = handler->get_elem_value(min_enlarge_index);
                auto next_addr = *reinterpret_cast<const uint64_t*>(&child_value);
                auto next_handler = get_node_handler(next_addr);

                return ChooseLeaf(key,&next_handler,ctx);
            }

            NodeHandler<KeyT, ValueT> *FindLeaf(
                Context<KeyT, ValueT> *ctx,
                NodeHandler<KeyT, ValueT> * cur_handler,
                KeyType<KeyT> *key
            ) {
                if (cur_handler->IsLeafBlock()) {
                    for(uint64_t i=0;i<cur_handler->get_count();i++){
                        if(*key==cur_handler->get_elem_key(i)){
                            std::pair<uint64_t, uint64_t> pth(cur_handler->get_in_file_addr(), i);
                            ctx->path.push_back(pth);
                            return cur_handler;
                        }
                    }
                    return nullptr;
                }

                for (uint64_t i=0;i<cur_handler->get_count();i++){
                    if(cur_handler->get_elem_key(i)>=*key){
                        auto child_value = cur_handler->get_elem_value(i);
                        uint64_t next_addr = *reinterpret_cast<const uint64_t*>(&child_value);
                        NodeHandler<KeyT, ValueT> next_node = get_node_handler(next_addr);
                        std::pair<uint64_t, uint64_t> pth(cur_handler->get_in_file_addr(), i);
                        ctx->path.push_back(pth);
                        auto res = FindLeaf(ctx,&next_node,key);
                        if(res==nullptr){
                            ctx->path.pop_back();
                        }
                        else{
                            return res;
                        }
                    }
                }

                return nullptr;
            }
            
            void modify_parent_entry_mbr(
                Context<KeyT, ValueT> *ctx,
                KeyType<KeyT>* modify_key = nullptr
            ) {
                if (ctx->path.empty()){
                    return;
                }
                std::pair<uint64_t, uint64_t> handler_entry = ctx->path.back();
                uint64_t cur_addr = handler_entry.first;
                uint64_t parent_entry_id = handler_entry.second;
                ctx->path.pop_back();

                auto cur_handler = get_node_handler(cur_addr);
                KeyType<KeyT> old_mbr = get_node_mbr(&cur_handler);

                if(modify_key!=nullptr){
                    cur_handler.set_elem_key(modify_key, parent_entry_id);
                }
                KeyType<KeyT> new_modify_key = get_node_mbr(&cur_handler);
                if(old_mbr!=new_modify_key){
                    modify_parent_entry_mbr(ctx,&new_modify_key);
                }
            }

            KeyType<KeyT> get_node_mbr(NodeHandler<KeyT, ValueT>* handler) {
                auto first_key = handler->get_elem_key(0);
                KeyType<KeyT> *mbr = new KeyType<KeyT>(first_key);

                for(uint64_t i=1;i<handler->get_count();i++){
                    mbr->mbr_enlarge(handler->get_elem_key(i));
                }

                return *mbr;
            }

            void split(
                Context<KeyT, ValueT> *ctx,
                KeyValuePair<KeyType<KeyT>, ValueT>& insert_kvp,
                KeyType<KeyT>* modify_key = nullptr
            ) {
                std::pair<uint64_t, uint64_t> handler_entry = ctx->path.back();
                uint64_t cur_addr = handler_entry.first;
                uint64_t parent_entry_id = handler_entry.second;
                ctx->path.pop_back();

                auto cur_handler = get_node_handler(cur_addr);

                if (modify_key!=nullptr){
                    cur_handler.set_elem_key(modify_key, parent_entry_id);
                }

                // not full,install(maybe modify)
                if(!cur_handler.is_full()){
                    cur_handler.insert(insert_kvp);
                    auto new_mbr = get_node_mbr(&cur_handler);
                    if (modify_key == nullptr) {
                        modify_key = new KeyType<KeyT>(new_mbr); 
                    } else {
                        *modify_key = new_mbr;
                    }
                    
                    // propagate till root
                    modify_parent_entry_mbr(ctx,modify_key);
                    return;
                }

                // full,split
                auto new_addr = allocate_block();
                NodeHeader *new_header = get_address<NodeHeader>(new_addr);

                // set node header
                BlockType block_type = cur_handler.IsLeafBlock()?BlockType::LeafBlock:BlockType::InnerBlock;
                *new_header = NodeHeader{block_type,0,new_addr};
                auto seeds = pickseed(&cur_handler, insert_kvp);
                auto& partition1 = seeds.first;  
                auto& partition2 = seeds.second;
                NodeHandler<KeyT, ValueT> new_node_handler = get_node_handler(new_addr);

                // reinsert the original node and maintain the modified mbr
                cur_handler.clear();
                KeyType<KeyT> *mbr2 = new KeyType<KeyT>(partition2[0].key);
                for(uint64_t i=0;i<partition2.size();i++) {
                    cur_handler.insert(partition2[i]);
                    mbr2->mbr_enlarge(partition2[i].key);
                }

                // insert k/v pair and maintain the mbr
                KeyType<KeyT> *mbr1 = new KeyType<KeyT>(partition1[0].key);
                for(uint64_t i =0;i<partition1.size();i++){
                    new_node_handler.insert(partition1[i]);
                    mbr1->mbr_enlarge(partition1[i].key);
                }

                if (cur_handler.get_in_file_addr()==get_root_addr()){
                    uint64_t new_root_addr = allocate_block();
                    NodeHeader *new_root_header = get_address<NodeHeader>(new_root_addr);
                    *new_root_header = NodeHeader{BlockType::InnerBlock,0,new_root_addr};

                    NodeHandler<KeyT, ValueT> root_handler = get_node_handler(new_root_addr);
                    auto cur_in_file_addr = cur_handler.get_in_file_addr();
                    
                    KeyValuePair<KeyType<KeyT>, ValueT> kvp2{*mbr1, new_addr};
                    KeyValuePair<KeyType<KeyT>, ValueT> kvp1{*mbr2, cur_in_file_addr};

                    root_handler.insert(kvp1);
                    root_handler.insert(kvp2);

                    get_header()->root_addr = new_root_addr;

                    return;
                }

                // traceback and adjust the tree
                // insert the new mbr entry and modify the parent mbr entry
                KeyValuePair<KeyType<KeyT>, ValueT> kvp{*mbr1, new_addr};
                split(ctx,kvp,mbr2);
            }

            std::pair<
                std::vector<KeyValuePair<KeyType<KeyT>, ValueT>>, 
                std::vector<KeyValuePair<KeyType<KeyT>, ValueT>> 
            >
            pickseed(NodeHandler<KeyT, ValueT> *handler, KeyValuePair<KeyType<KeyT>, ValueT>& kvp) {
                std::vector<KeyValuePair<KeyType<KeyT>, ValueT>> whole;
                auto entry_count = handler->get_count();

                for(uint64_t i = 0; i < entry_count; i++) {
                    whole.push_back(handler->get_elem_pair(i));  
                }

                whole.push_back(kvp);  

                size_t partition1 = 0;
                size_t partition2 = 1;
                KeyT min_waste = whole[0].key.enlargement(whole[1].key) - 
                                whole[0].key.area() - 
                                whole[1].key.area();

                for(size_t i = 0; i < whole.size(); i++) {
                    for(size_t j = i + 1; j < whole.size(); j++) {
                        const KeyType<KeyT>& mbr1 = whole[i].key;
                        const KeyType<KeyT>& mbr2 = whole[j].key;

                        KeyT waste = mbr1.enlargement(mbr2) - mbr1.area() - mbr2.area();
                        if (waste > min_waste) {
                            min_waste = waste;
                            partition1 = i;
                            partition2 = j;
                        }
                    }
                }

                std::vector<KeyValuePair<KeyType<KeyT>, ValueT>> res1;
                std::vector<KeyValuePair<KeyType<KeyT>, ValueT>> res2;

                res1.push_back(whole[partition1]);
                res2.push_back(whole[partition2]);

                whole.erase(whole.begin()+partition2);
                whole.erase(whole.begin()+partition1);

                KeyType<KeyT> mbr1(res1[0].key);
                KeyType<KeyT> mbr2(res2[0].key);

                while(!whole.empty()){
                    KeyT maxdiff = static_cast<KeyT>(0);
                    KeyT exp1,exp2;
                    size_t pick;
                    for(size_t i = 0; i < whole.size(); i++) {
                        const KeyType<KeyT>& mbr = whole[i].key;
                        KeyT expansion1 = mbr1.enlargement(mbr) - mbr1.area();
                        KeyT expansion2 = mbr2.enlargement(mbr) - mbr2.area();

                        KeyT expansion_diff = std::abs(expansion1-expansion2);
                        if ( expansion_diff >= maxdiff) {
                            pick = i;
                            maxdiff = expansion_diff;
                            exp1 = expansion1; exp2 = expansion2;
                        }
                    }
                    const KeyType<KeyT>& mbr = whole[pick].key;
                    if (exp1<exp2){
                        res1.push_back(whole[pick]);
                        mbr1.mbr_enlarge(mbr);
                    }
                    else{
                        res2.push_back(whole[pick]);
                        mbr2.mbr_enlarge(mbr);
                    }
                    whole.erase(whole.begin()+pick);
                }

                return std::make_pair(res1, res2);
            }


        public:
            std::vector<KeyValuePair<KeyType<KeyT>, ValueT>> Overlap_Search(const KeyType<KeyT>& key){
                assert(key.size()/2==this->dimensions_);
                std::vector<KeyValuePair<KeyType<KeyT>, ValueT>> res;
                auto root_handler = get_node_handler(get_root_addr());
                search(key,&root_handler,&res,SearchMode::overlap);

                return res;
            }

            std::vector<KeyValuePair<KeyType<KeyT>, ValueT>> Comprise_Search(const KeyType<KeyT>& key){
                assert(key.size()/2==this->dimensions_);
                std::vector<KeyValuePair<KeyType<KeyT>, ValueT>> res;
                auto root_handler = get_node_handler(get_root_addr());
                search(key,&root_handler,&res,SearchMode::comprise);

                return res;
            }

            void Insert(KeyValuePair<KeyType<KeyT>, ValueT>& kvp) {
                auto root_addr = get_root_addr();
                // empty tree
                if (root_addr == INVALID_ROOT_ADDR) {
                    uint64_t new_addr = allocate_block();
                    get_header()->root_addr = new_addr;

                    NodeHeader root_header = NodeHeader{BlockType::LeafBlock,0,new_addr};
                    NodeHeader *header = get_address<NodeHeader>(new_addr);
                    *header = root_header;

                    NodeHandler<KeyT, ValueT> root_handler = get_node_handler(new_addr);
                    root_handler.insert(kvp);
                    return;
                }
                
                Context<KeyT, ValueT> ctx;
                auto root_handler = get_node_handler(root_addr);
                ChooseLeaf(kvp.key,&root_handler,&ctx);
                
                split(&ctx,kvp);
            }

            bool Delete(KeyValuePair<KeyType<KeyT>, ValueT>& kvp) {
                Context<KeyT, ValueT> ctx;
                auto root_handler = get_node_handler(get_root_addr());
                NodeHandler<KeyT, ValueT> *target_leaf = FindLeaf(&ctx,&root_handler,&kvp.key);

                if (target_leaf==nullptr){
                    return false;
                }

                std::pair<uint64_t, uint64_t> handler_entry = ctx.path.back();
                uint64_t target_addr = handler_entry.first;
                uint64_t parent_entry_id = handler_entry.second;
                
                auto target_handler = get_node_handler(target_addr);
                target_handler.delete_elem_key(parent_entry_id);

                modify_parent_entry_mbr(&ctx);

                return true;
            }
            
            void PrintTree() {
                auto root_addr = get_root_addr();
                if (root_addr == INVALID_ROOT_ADDR) {
                    std::cout << "R-Tree is empty" << std::endl;
                    return;
                }
                
                auto root_handler = get_node_handler(root_addr);
                std::cout << "R-Tree Structure:" << std::endl;
                std::cout << "=================" << std::endl;
                print_node_recursive(&root_handler, 0);
            }

            std::vector<KeyValuePair<KeyType<KeyT>, ValueT>> GetAllEntries() {
                std::vector<KeyValuePair<KeyType<KeyT>, ValueT>> all_entries;
                auto root_addr = get_root_addr();
                
                if (root_addr == INVALID_ROOT_ADDR) {
                    return all_entries;
                }
                
                auto root_handler = get_node_handler(root_addr);
                collect_leaf_entries(&root_handler, all_entries);
                return all_entries;
            }
    };
}

#endif