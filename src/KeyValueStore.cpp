#include "rosbag/KeyValueStore.h"

#include <cassert>
#include <iostream>
#include <list>
#include <queue>
#include <fcntl.h>
#include <unistd.h>
#include <zconf.h>

namespace rosbag {

// template <typename KeyT>


// template <typename KeyT>
// _KeyValueStore<KeyT> _KeyValueStore<KeyT>::create(int dir, const char *name,
//                                       size_t value_size, size_t key_size, size_t block_size) {
//     int index = openat(dir, name, O_RDWR | O_CREAT | O_EXCL,
//                        S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
//     if (index == -1)
//         return _KeyValueStore<KeyT>{-1, value_size, key_size, block_size};
//     if (ftruncate(index, block_size) == -1) {
//         close(index);
//         return _KeyValueStore<KeyT>{-1, value_size, key_size, block_size};
//     }

//     _KeyValueStore<KeyT> ret{index, value_size, key_size, block_size};

//     if (ret.get_fd() != -1) {
//         /// Create the header.
//         *ret.get_header() = IndexHeader{
//             .magic = *reinterpret_cast<const uint64_t *>(MAGIC),
//             .block_size = block_size,
//             .key_size = key_size,
//             .value_size = value_size,
//             .root_addr = INVALID_ROOT,
//         };
//     }

//     return ret;
// }

// template <typename KeyT>
// _KeyValueStore<KeyT> _KeyValueStore<KeyT>::open(int dir, const char *name,
//                                     size_t value_size, size_t key_size, size_t block_size) {
//     value_size = (value_size + 7) / 8 * 8;

//     int index = openat(dir, name, O_RDWR);
//     if (index == -1)
//         return _KeyValueStore<KeyT>{-1, value_size, key_size, block_size};

//     _KeyValueStore<KeyT> ret{index, value_size, key_size, block_size};

//     if (ret.get_fd() != -1) {
//         /// Verify the header.
//         auto header = ret.get_header();

//         if (header->magic != *reinterpret_cast<const uint64_t *>(MAGIC) ||
//             header->block_size != block_size ||
//             header->value_size != value_size ||
//             header->key_size != key_size) {
//             return _KeyValueStore<KeyT>{-1, value_size, block_size};
//         }
//     }

//     return ret;
// }

// template <typename KeyT>
// uint64_t _KeyValueStore<KeyT>::allocate_block() {
//     uint64_t block = index.get_size();
//     // Extend the file with one block and return that block.
//     if (!index.truncate(block + get_block_size())) {
//         abort();
//     }
//     return block;
// }

// template <typename KeyT>
// NodeBlock<KeyT>* _KeyValueStore<KeyT>::get_leaf_block(KeyT key,NodeBlock<KeyT>* cur_node,Context<KeyT> *ctx) {
//     ctx->path_.push_back(*cur_node);
//     if (cur_node->IsLeafBlock()){
//         return cur_node;
//     }

//     size_t lb = cur_node->lower_bound(key);
//     assert(!cur_node->IsLeafBlock()); // should be an inner node
//     auto next_addr = *reinterpret_cast<uint64_t *>(cur_node->get_elem_value(lb+1));
//     auto next_node = get_node_block(next_addr);

//     return get_leaf_block(key,&next_node,ctx);
// }

// template <typename KeyT>
// void _KeyValueStore<KeyT>::insert(KeyT key,const void *value) {
//     // file lock
//     rw_latch_.try_write_lock();

//     // empty tree
//     if (get_header()->root_addr==rosbag::INVALID_ROOT) {
//         auto root_addr = allocate_block();

//         // set node header for the new root
//         // note that the initial root block is a LeafBlock
//         auto header = get_address<NodeHeader>(root_addr);
//         set_node_header(header,0,BlockType::LeafBlock,root_addr);
//         auto leaf_header = static_cast<LeafHeader*>(header);
//         leaf_header->SetNextSiblingLeaf(0);
//         leaf_header->SetPrevSiblingLeaf(0);
        
//         // insert the K/V pair
//         auto root_block = get_node_block(root_addr);
//         root_block.insert(key,value);

//         header->SetEntryCount(1);
//         get_header()->root_addr=root_addr;

//         rw_latch_.unlock();
//         return;
//     }

//     // has leaves
//     Context<KeyT> ctx;
//     auto root_node_block = get_node_block(get_header()->root_addr);
//     get_leaf_block(key,&root_node_block,&ctx);
    
//     split(&ctx,key,value);

//     rw_latch_.unlock();
// }

// if the splitted node is root
// maintain the IndexHeader
// template <typename KeyT>
// void _KeyValueStore<KeyT>::split_new_root(KeyT split_key,uint64_t left_addr,uint64_t right_addr){
//     // allocate a new block to store the root node
//     // and change the root_addr_ in IndexHeader to this node's in-file address
//     auto new_root_addr = allocate_block();
//     auto new_root_header = get_address<NodeHeader>(new_root_addr);
//     set_node_header(new_root_header,1,BlockType::InnerBlock,new_root_addr);
//     get_header()->root_addr = new_root_addr;
//     auto root_block = get_node_block(new_root_addr);

//     // insert the first key in the new root
//     auto first_key = root_block.get_elem_key(0);
//     *first_key = split_key;

//     // insert 2 pointers to the children node(i.e. cur_node and new_block)
//     auto kl = root_block.get_elem_value(0);
//     auto kr = root_block.get_elem_value(1);
//     memcpy(kl,&left_addr,value_size);
//     memcpy(kr,&right_addr,value_size);
//     return;
// }

// template <typename KeyT>
// void _KeyValueStore<KeyT>::split(Context<KeyT> *ctx,KeyT key,const void* value){
//     auto cur_node = ctx->path_.back();
//     ctx->path_.pop_back();

//     // case 1 : not full
//     if(!cur_node.is_full()){
//         cur_node.insert(key,value);

//         cur_node.set_count(cur_node.get_count()+1);
//         return;
//     }

//     // case 2 : trace back and split
//     auto new_node_addr = allocate_block();
//     // set node header
//     auto new_header = get_address<NodeHeader>(new_node_addr);
//     new_header->SetInFileAddr(new_node_addr);
//     // note that if the sibling node is a leaf node
//     // then the new node is a leaf node,otherwise it's a inner node
//     if (cur_node.IsLeafBlock()){
//         new_header->SetBlockType(BlockType::LeafBlock);
//     }else{
//         new_header->SetBlockType(BlockType::InnerBlock);
//     }

//     // time for split and data(entries) movement
//     auto new_block = get_node_block(new_node_addr);
//     // move half of the entries from current node to new node
//     if (cur_node.IsLeafBlock()){
//         memmove(
//             new_block.get_elem_ptr(0),
//             cur_node.get_elem_ptr(
//                 (cur_node.get_count()+1)/2
//             ),
//             cur_node.get_count()/2*cur_node.get_pair_size()
//         );

//         new_block.set_count(cur_node.get_count()/2);
//         cur_node.set_count((cur_node.get_count()+1)/2);

//         // insert the K/V pair
//         if (key>=*new_block.first_key()){
//             new_block.insert(key,value);
//             new_block.set_count(new_block.get_count()+1);
//         // }else if(key==*new_block.first_key()){
//         //     abort();
//         // we allow insertion of the exisiting keys now
//         }else{
//             cur_node.insert(key,value);
//             cur_node.set_count(cur_node.get_count()+1);
//         }
//     }else{
//         // merge original k/v pairs and the new k/v pair into a new allocated memory buffer
//         // since we cannot modify the node block because it will do dirty write to time-index
//         auto cnt = cur_node.get_count()+1;
//         std::list<KeyT> keys;
//         std::list<void*> values;
//         bool flag = true;
//         for (int i=cur_node.get_count()-1;i>=0;i--){
//             auto key_ptr = cur_node.get_elem_key((uint64_t)i);
//             auto v = cur_node.get_elem_value(i+1);
//             if(*key_ptr<key && flag){
//                 keys.push_front(key);
//                 values.push_front(const_cast<void*>(value));
//                 flag = false;
//             }
//             keys.push_front(*key_ptr);
//             values.push_front(v);
//         }
//         values.push_front(cur_node.get_elem_value(0));
//         // std::cout<<values.size()<<" "<<keys.size()<<std::endl;

//         auto first_value = cur_node.get_elem_value(0);
//         memcpy(first_value,values.front(),value_size);
//         values.pop_front();
//         for(uint64_t i=0;i<cnt/2;i++){
//             auto key_ptr = cur_node.get_elem_key(i);
//             auto value_ptr = cur_node.get_elem_value(i+1);
//             *key_ptr = keys.front();
//             keys.pop_front();
//             memcpy(value_ptr,values.front(),value_size);
//             values.pop_front();
//         }
//         auto split_key = keys.front();
//         keys.pop_front();

//         auto new_first_value = new_block.get_elem_value(0);
//         memcpy(new_first_value,values.front(),value_size);
//         values.pop_front();
//         uint64_t i = 0;
//         while(!keys.empty()){
//             auto key_ptr = new_block.get_elem_key(i);
//             auto value_ptr = new_block.get_elem_value(i+1);
//             // std::cout<<keys.front()<<" "<<*static_cast<uint64_t*>(values.front())<<std::endl;
//             *key_ptr = keys.front();
//             keys.pop_front();
//             memcpy(value_ptr,values.front(),value_size);
//             values.pop_front();
//             i += 1;
//         }
//         assert(values.empty());
//         cur_node.set_count(cnt/2);
//         new_block.set_count((cnt-1)/2);

//         // special occasion when cur_node is the root node
//         if (cur_node.get_in_file_addr()==get_header()->root_addr){
//             // std::cout<<"inner root split happens"<<std::endl;
//             split_new_root(split_key,cur_node.get_in_file_addr(),new_node_addr);
//             // auto root = get_node_block(get_header()->root_addr);
//             // root.print_node();
//             // std::cout<<std::endl;
//             // auto l = get_node_block(*static_cast<uint64_t*>(root.get_elem_value(0)));
//             // l.print_node();
//             // std::cout<<std::endl;
//             // auto r = get_node_block(*static_cast<uint64_t*>(root.get_elem_value(1)));
//             // r.print_node();
//             return; // there'll be no more context in the ctx path_ since the root has splitted
//         }
//         split(ctx,split_key,&new_node_addr);
//         return;
//         // auto cnt = cur_node.get_count()+1;
//         // void* buffer = std::malloc(cnt*sizeof(uint64_t)+(cnt+1)*value_size);
//         // uint64_t j = 1;
//         // bool flag = true;
//         // for (int i = cur_node.get_count()-1;i>=0;i--){
//         //     auto key_ptr = cur_node.get_elem_key((uint64_t)i);
//         //     if (*key_ptr<key && flag){
//         //         auto new_key_ptr = static_cast<uint64_t*>(buffer+value_size+(cnt-j)*(sizeof(uint64_t)+value_size));
//         //         *new_key_ptr = key;
//         //         auto new_value_ptr = new_key_ptr+sizeof(uint64_t);
//         //         memcpy(new_value_ptr,value,value_size);
//         //         j += 1;
//         //         flag = false;
//         //     }
//         //     memcpy(
//         //         buffer+value_size+(cnt-j)*(sizeof(uint64_t)+value_size),
//         //         key_ptr,
//         //         sizeof(uint64_t)+value_size
//         //     );
//         //     j += 1;
//         // }
//         // memcpy(buffer,cur_node.get_elem_value(0),value_size);
//         // auto forward_half_bytes = value_size+cnt/2*cur_node.get_pair_size();
//         // memmove(cur_node.get_elem_value(0),buffer,forward_half_bytes);
//         // memmove(new_block.get_elem_value(0),buffer+forward_half_bytes+sizeof(uint64_t),value_size+((cnt-1)/2)*cur_node.get_pair_size());
//         // cur_node.set_count(cnt/2);
//         // new_block.set_count((cnt-1)/2);

//         // split(ctx,*static_cast<uint64_t*>(buffer+forward_half_bytes),&new_node_addr);
        
//         // delete buffer;
//         // return;
//     }
//     // for(size_t i=0;i<cur_node.get_count()/2;i++){
//     //     uint64_t idx = cur_node.get_count()-1-i;
//     //     new_block.insert(*cur_node.get_elem_key(idx),cur_node.get_elem_value(idx));
//     // }

//     // if the siblings are leaf blocks
//     // maintain the bi-pointer between 2 nodes
//     if (cur_node.IsLeafBlock()){
//         static_cast<LeafHeader*>(cur_node.get_header())->SetNextSiblingLeaf(new_node_addr);
//         static_cast<LeafHeader*>(new_block.get_header())->SetPrevSiblingLeaf(cur_node.get_in_file_addr());
//     }

//     // new_block.print_node();
//     // cur_node.print_node();

//     if (cur_node.get_in_file_addr()==get_header()->root_addr){
//         split_new_root(*cur_node.last_key(),cur_node.get_in_file_addr(),new_node_addr);
//         // root_block.print_node();
//         return; 
//     }

//     // when a node split,the pointer must be the sibling's address
//     // (] (] ... ()
//     split(ctx,*cur_node.last_key(),&new_node_addr); 
// }

// template <typename KeyT>
// void _KeyValueStore<KeyT>::print_b_plus_tree(){
//     std::cout<<"----------------------"<<"B+ Tree"<<"----------------------"<<std::endl;
//     std::queue<NodeBlock<KeyT>> q1;
//     std::queue<NodeBlock<KeyT>> q2;
//     if(get_header()->root_addr==rosbag::INVALID_ROOT){
//         std::cout<<"empty tree"<<std::endl;
//         std::cout<<"----------------------"<<"B+ Tree"<<"----------------------"<<"\n"<<std::endl;
//         return;
//     }
//     auto root_block = get_node_block(get_header()->root_addr);
//     q1.push(root_block);

//     while(!q1.empty()){
//         auto node_block = q1.front();
//         q1.pop();
//         node_block.print_node();
//         if (!node_block.IsLeafBlock()){
//             for(size_t i=0;i<node_block.get_count();i++){
//                 q2.push(get_node_block(*static_cast<uint64_t *>(node_block.get_elem_value(i))));
//             }
//             q2.push(get_node_block(*static_cast<uint64_t *>(node_block.get_elem_value(node_block.get_count()))));
//         }
        
//         if (q1.empty()){
//             std::cout<<std::endl;
//             while(!q2.empty()){
//                 q1.push(q2.front());
//                 q2.pop();
//             }
//         }
//     }
//     std::cout<<"----------------------"<<"B+ Tree"<<"----------------------"<<"\n"<<std::endl;
// }



} // namespace rosbag