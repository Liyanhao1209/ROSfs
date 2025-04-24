#ifndef FILE_CACHE_H
#define FILE_CACHE_H

#include <unordered_map>
#include <sys/mman.h>
#include <unistd.h>

namespace rosbag {
    /// Memory map based file cache.  Cannot be copied.  Not memory safe.
class FileCache {
  public:
    static constexpr int PROTECT = PROT_READ | PROT_WRITE;
    static constexpr int MODE = MAP_SHARED;

  private:
    std::unordered_map<uint64_t, void *> cache;
    uint64_t size;
    uint64_t block_size;
    int fd;

  public:
    /// _fd: the file discripter, file cache owns this fd.
    /// block_size: the smallest block used for cache.
    FileCache(int _fd, uint64_t block_size);

    FileCache(FileCache &&other) noexcept
        : cache{std::move(other.cache)}, size{other.size},
          block_size{other.block_size}, fd{other.fd} {
        other.fd = -1;
    }

    FileCache &operator=(FileCache &&other) noexcept {
        if (this != &other) {
            this->cache = std::move(other.cache);
            this->size = other.size;
            this->block_size = other.block_size;
            close(this->fd);
            this->fd = other.fd;
            other.fd = -1;
        }
        return *this;
    }

    /// Get the block size used for cache.
    u_int64_t get_block_size() const { return block_size; }

    /// The mask for in block offset
    u_int64_t get_offset_mask() const { return get_block_size() - 1; }

    /// The mask for block address
    u_int64_t get_addr_mask() const { return ~get_offset_mask(); }

    /// Get the fd for the file cache, used to indicate whether the cache
    /// is sussesfully established.
    int get_fd() const { return fd; }

    /// Get the size of the file
    uint64_t get_size() const { return size; }

    /// Resize the file, currently only support increase the file size.
    /// Return value for whether this operation is successful.
    bool truncate(uint64_t _len);

    /// get the memory address for the file block, the file offset must be
    /// `block_size` aligned.  Return `nullptr` if the block does not
    /// exists.
    void *get_block(uint64_t block);

    ~FileCache();
};
}

#endif