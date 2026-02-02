#include <rosfs/rosfs.hpp>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdexcept>
#include <filesystem>
#include <cstring>
#include <algorithm>
#include <iostream>
#include <vector>
#include <mutex> // [新增]

namespace rosfs {

namespace fs = std::filesystem;

struct IndexEntry {
    uint64_t timestamp;
    uint64_t offset;
} __attribute__((packed));

class ROSfsDB::Impl {
public:
    std::string base_path_;
    std::string data_file_path_;
    std::string index_file_path_;

    int fd_data_ = -1;
    int fd_index_ = -1;
    bool read_only_ = false;

    uint64_t current_data_offset_ = 0;
    uint64_t total_records_ = 0;
    
    // 内存索引缓存
    std::vector<IndexEntry> write_index_cache_;

    // MMap 读取
    IndexEntry* idx_map_ptr_ = nullptr;
    size_t idx_map_size_ = 0;
    size_t idx_count_ = 0;

    // [新增] 读写锁 (为了代码简洁使用互斥锁，高性能场景可用 shared_mutex)
    mutable std::mutex mutex_;

    Impl() {}

    ~Impl() {
        close_files();
    }

    void open(const std::string& path, bool read_only) {
        std::lock_guard<std::mutex> lock(mutex_); // Open 也要锁
        base_path_ = path;
        read_only_ = read_only;
        data_file_path_ = base_path_ + "/data.bin";
        index_file_path_ = base_path_ + "/index.idx";

        if (!fs::exists(base_path_)) {
            if (read_only) {
                throw std::runtime_error("ROSfs: DB path not found: " + path);
            }
            fs::create_directories(base_path_);
        }

        int flags = read_only ? O_RDONLY : (O_RDWR | O_CREAT);
        mode_t mode = 0644;

        fd_data_ = ::open(data_file_path_.c_str(), flags, mode);
        if (fd_data_ < 0) throw std::runtime_error("ROSfs: Failed to open data file");

        fd_index_ = ::open(index_file_path_.c_str(), flags, mode);
        if (fd_index_ < 0) throw std::runtime_error("ROSfs: Failed to open index file");

        if (!read_only) {
            struct stat st;
            ::fstat(fd_data_, &st);
            current_data_offset_ = st.st_size;
            
            ::fstat(fd_index_, &st);
            total_records_ = st.st_size / sizeof(IndexEntry);
            write_index_cache_.reserve(100000); 
        } else {
            load_index();
        }
    }

    void close_files() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (idx_map_ptr_) {
            ::munmap(idx_map_ptr_, idx_map_size_);
            idx_map_ptr_ = nullptr;
        }
        if (fd_data_ >= 0) { ::close(fd_data_); fd_data_ = -1; }
        if (fd_index_ >= 0) { ::close(fd_index_); fd_index_ = -1; }
        write_index_cache_.clear();
    }

    void append(uint64_t timestamp, const std::vector<uint8_t>& data) {
        // 数据准备不需要锁
        uint64_t size = data.size();
        std::vector<uint8_t> buffer(sizeof(uint64_t) + size);
        std::memcpy(buffer.data(), &size, sizeof(uint64_t));
        std::memcpy(buffer.data() + sizeof(uint64_t), data.data(), size);

        std::lock_guard<std::mutex> lock(mutex_); // [加锁]
        if (read_only_) throw std::runtime_error("ROSfs: Cannot append in read-only mode");

        // 1. 写入数据
        ssize_t written = ::pwrite(fd_data_, buffer.data(), buffer.size(), current_data_offset_);
        if (written != (ssize_t)buffer.size()) throw std::runtime_error("ROSfs: Data write failed");

        // 2. 写入索引
        IndexEntry entry;
        entry.timestamp = timestamp;
        entry.offset = current_data_offset_; 

        ssize_t idx_written = ::write(fd_index_, &entry, sizeof(IndexEntry));
        if (idx_written != sizeof(IndexEntry)) throw std::runtime_error("ROSfs: Index write failed");

        // 3. 更新状态
        current_data_offset_ += written;
        total_records_++;
        write_index_cache_.push_back(entry);
    }

    void range_query(uint64_t start_ts, uint64_t end_ts, 
                     std::function<bool(const Record&)> callback) {
        // [注意] 我们不把锁加在整个 query 上，而是分段加锁或复制索引
        // 为了实现最高并发度，这里演示“快照读”策略：先复制出需要查询的索引片段，再释放锁去读盘
        
        std::vector<IndexEntry> target_indices;
        bool use_mmap = false;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (read_only_) {
                use_mmap = true;
            } else {
                // 写模式：从 write_index_cache_ 复制符合条件的条目
                // 这样可以避免长时间持有锁阻塞 Writer
                auto it = std::lower_bound(write_index_cache_.begin(), write_index_cache_.end(), start_ts,
                    [](const IndexEntry& entry, uint64_t val) { return entry.timestamp < val; });
                
                for (; it != write_index_cache_.end(); ++it) {
                    if (it->timestamp > end_ts) break;
                    target_indices.push_back(*it);
                }
            }
        }

        if (use_mmap) {
            // Read-Only 模式通常不需要锁（除非有并发 Close），MMap 是线程安全的
            query_mmap(start_ts, end_ts, callback);
        } else {
            // Write 模式：使用复制出来的索引读盘
            std::vector<uint8_t> read_buf;
            Record rec;
            for (const auto& entry : target_indices) {
                // pread 是线程安全的原子操作
                uint64_t payload_size;
                if (::pread(fd_data_, &payload_size, sizeof(uint64_t), entry.offset) != sizeof(uint64_t)) break;
                
                if (read_buf.size() < payload_size) read_buf.resize(payload_size);
                if (::pread(fd_data_, read_buf.data(), payload_size, entry.offset + sizeof(uint64_t)) != (ssize_t)payload_size) break;

                rec.timestamp = entry.timestamp;
                rec.data.assign(read_buf.begin(), read_buf.begin() + payload_size);
                if (!callback(rec)) break;
            }
        }
    }

    void query_mmap(uint64_t start_ts, uint64_t end_ts, std::function<bool(const Record&)>& callback) {
        // 只有 open/close 会改 idx_map_ptr_，runtime 不会改，所以这里可以不加锁（假设没有并发 close）
        if (idx_count_ == 0) return;
        IndexEntry* end_ptr = idx_map_ptr_ + idx_count_;
        IndexEntry* it = std::lower_bound(idx_map_ptr_, end_ptr, start_ts, 
            [](const IndexEntry& entry, uint64_t val) { return entry.timestamp < val; });

        std::vector<uint8_t> read_buf;
        Record rec;
        for (; it != end_ptr; ++it) {
            if (it->timestamp > end_ts) break;
            uint64_t payload_size;
            if (::pread(fd_data_, &payload_size, sizeof(uint64_t), it->offset) != sizeof(uint64_t)) break;
            if (read_buf.size() < payload_size) read_buf.resize(payload_size);
            if (::pread(fd_data_, read_buf.data(), payload_size, it->offset + sizeof(uint64_t)) != (ssize_t)payload_size) break;
            rec.timestamp = it->timestamp;
            rec.data.assign(read_buf.begin(), read_buf.begin() + payload_size);
            if (!callback(rec)) break;
        }
    }

    Stats get_stats() const {
        std::lock_guard<std::mutex> lock(mutex_); // 简单的锁一下
        Stats s;
        struct stat st;
        if (fd_data_ >= 0 && ::fstat(fd_data_, &st) == 0) s.data_file_size = st.st_size;
        else s.data_file_size = 0;
        if (fd_index_ >= 0 && ::fstat(fd_index_, &st) == 0) s.index_file_size = st.st_size;
        else s.index_file_size = 0;
        s.total_records = total_records_;
        return s;
    }

private:
    void load_index() {
        struct stat st;
        if (::fstat(fd_index_, &st) != 0) return;
        idx_map_size_ = st.st_size;
        if (idx_map_size_ == 0) return;
        void* ptr = ::mmap(nullptr, idx_map_size_, PROT_READ, MAP_SHARED, fd_index_, 0);
        if (ptr == MAP_FAILED) {
            idx_map_ptr_ = nullptr; idx_map_size_ = 0;
            throw std::runtime_error("ROSfs: Failed to mmap index");
        }
        idx_map_ptr_ = static_cast<IndexEntry*>(ptr);
        idx_count_ = idx_map_size_ / sizeof(IndexEntry);
        total_records_ = idx_count_;
    }
};

ROSfsDB::ROSfsDB() : impl_(std::make_unique<Impl>()) {}
ROSfsDB::~ROSfsDB() = default;
void ROSfsDB::open(const std::string& path, bool read_only) { impl_->open(path, read_only); }
void ROSfsDB::append(uint64_t timestamp, const std::vector<uint8_t>& data) { impl_->append(timestamp, data); }
void ROSfsDB::range_query(uint64_t start_ts, uint64_t end_ts, std::function<bool(const Record&)> callback) { impl_->range_query(start_ts, end_ts, callback); }
ROSfsDB::Stats ROSfsDB::get_stats() const { return impl_->get_stats(); }
void ROSfsDB::close() { impl_->close_files(); }

} // namespace rosfs