#include "rosbag/FileCache.h"

#include <sys/mman.h>
#include <sys/stat.h>
#include <cassert>
#include <iostream>

namespace rosbag {
    FileCache::FileCache(int _fd, uint64_t block_size)
    : cache{}, size{0}, block_size{block_size}, fd{_fd} {
    do {
        if (fd == -1) {
            break;
        }

        struct stat64 tmp {};
        if (fstat64(fd, &tmp) == -1) {
            std::cout<<"fstat fail when creating a file cache"<<std::endl;
            break;
        }

        size = tmp.st_size;
        // file size must be multiple of block size
        if ((size & get_offset_mask()) != 0) {
            std::cout<<"file size must be multiple of block size"<<std::endl;
            break;
        }

        // map the entire file to memory and create index
        if (size > 0) {
            auto ptr = static_cast<uint8_t *>(
                mmap(nullptr, size, PROTECT, MODE, fd, 0));
            if (ptr == MAP_FAILED) {
                std::cout<<"MMap failed when creating a file cache"<<std::endl;
                break;
            }

            for (uint64_t block = 0; block < size; block += get_block_size()) {
                cache.emplace(block, ptr + block);
            }
        }

        return;
    } while (false);

    close(fd);
    fd = -1;
    size = 0;
}

bool FileCache::truncate(uint64_t _len) {
    uint64_t len = ((_len - 1) & get_addr_mask()) + get_block_size();
    if (len < _len) {
        return false;
    } // overflow
    if (len < size) {
        return false;
    } // reduce size of file not support
    if (len == size) {
        return true;
    } // nothing to do

    if (ftruncate(fd, len) != 0) {
        return false;
    }

    // map the extended part
    auto ptr = static_cast<uint8_t *>(
        mmap(nullptr, len - size, PROTECT, MODE, fd, size));

    if (ptr == MAP_FAILED) {
        return false;
    }

    // create index for new part
    for (uint64_t block = 0; block < len - size; block += get_block_size()) {
        cache.emplace(size + block, ptr + block);
    }

    size = len;
    return true;
}

void *FileCache::get_block(uint64_t block) {
    assert((block & get_offset_mask()) == 0);
    assert(block < size);

    auto ptr = cache.find(block);
    return ptr == cache.end() ? nullptr : ptr->second;
}

FileCache::~FileCache() {
    for (auto item : cache) {
        munmap(item.second, get_block_size());
    }
    if (fd != -1)
        close(fd);
}

}