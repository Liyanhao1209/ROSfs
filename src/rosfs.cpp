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

namespace rosfs {

namespace fs = std::filesystem;

// -----------------------------------------------------------------------------
// 内部数据结构定义 (On-Disk Layout)
// -----------------------------------------------------------------------------

// 索引项：定长 16 字节
// 模拟 B+ 树叶子节点的行为 (Key, Value)
struct IndexEntry {
    uint64_t timestamp;
    uint64_t offset;    // 指向数据文件的物理偏移
} __attribute__((packed));

// -----------------------------------------------------------------------------
// ROSfs Implementation (Pimpl)
// -----------------------------------------------------------------------------

class ROSfsDB::Impl {
public:
    std::string base_path_;
    std::string data_file_path_;
    std::string index_file_path_;

    int fd_data_ = -1;
    int fd_index_ = -1;
    bool read_only_ = false;

    // 写入状态维护
    uint64_t current_data_offset_ = 0;
    uint64_t total_records_ = 0;

    // 读取状态维护 (MMap Index)
    IndexEntry* idx_map_ptr_ = nullptr;
    size_t idx_map_size_ = 0;
    size_t idx_count_ = 0;

    Impl() {}

    ~Impl() {
        close_files();
    }

    void open(const std::string& path, bool read_only) {
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

        // 打开数据文件
        fd_data_ = ::open(data_file_path_.c_str(), flags, mode);
        if (fd_data_ < 0) throw std::runtime_error("ROSfs: Failed to open data file");

        // 打开索引文件
        fd_index_ = ::open(index_file_path_.c_str(), flags, mode);
        if (fd_index_ < 0) throw std::runtime_error("ROSfs: Failed to open index file");

        if (!read_only) {
            // Append 模式：恢复 offset
            struct stat st;
            ::fstat(fd_data_, &st);
            current_data_offset_ = st.st_size;
            
            ::fstat(fd_index_, &st);
            total_records_ = st.st_size / sizeof(IndexEntry);
        } else {
            // Read-Only 模式：加载索引到内存
            load_index();
        }
    }

    void close_files() {
        if (idx_map_ptr_) {
            ::munmap(idx_map_ptr_, idx_map_size_);
            idx_map_ptr_ = nullptr;
        }
        if (fd_data_ >= 0) { ::close(fd_data_); fd_data_ = -1; }
        if (fd_index_ >= 0) { ::close(fd_index_); fd_index_ = -1; }
    }

    void append(uint64_t timestamp, const std::vector<uint8_t>& data) {
        if (read_only_) throw std::runtime_error("ROSfs: Cannot append in read-only mode");

        // Format: [Size (8B)] [Payload]
        uint64_t size = data.size();
        
        // 拼接 Buffer 以减少 syscall 次数
        std::vector<uint8_t> buffer(sizeof(uint64_t) + size);
        std::memcpy(buffer.data(), &size, sizeof(uint64_t));
        std::memcpy(buffer.data() + sizeof(uint64_t), data.data(), size);

        // 1. 写入数据 (Append Data Log)
        ssize_t written = ::pwrite(fd_data_, buffer.data(), buffer.size(), current_data_offset_);
        if (written != (ssize_t)buffer.size()) {
            throw std::runtime_error("ROSfs: Data write failed");
        }

        // 2. 写入索引 (Append Index Log)
        IndexEntry entry;
        entry.timestamp = timestamp;
        entry.offset = current_data_offset_; // 记录的是 Block 的起始位置

        ssize_t idx_written = ::write(fd_index_, &entry, sizeof(IndexEntry));
        if (idx_written != sizeof(IndexEntry)) {
            throw std::runtime_error("ROSfs: Index write failed");
        }

        // 更新状态
        current_data_offset_ += written;
        total_records_++;
    }

    void range_query(uint64_t start_ts, uint64_t end_ts, 
                     std::function<bool(const Record&)> callback) {
        if (!read_only_) {
            throw std::runtime_error("ROSfs: Query only supported in read-only mode");
        }

        if (idx_count_ == 0) return;

        // 1. 二分查找定位起点 (Index Search)
        // std::lower_bound 在有序数组上是 O(log N)
        IndexEntry* end_ptr = idx_map_ptr_ + idx_count_;
        IndexEntry* it = std::lower_bound(idx_map_ptr_, end_ptr, start_ts, 
            [](const IndexEntry& entry, uint64_t val) {
                return entry.timestamp < val;
            });

        // 2. 顺序扫描 (Leaf Node Scan)
        std::vector<uint8_t> read_buf;
        Record rec;

        for (; it != end_ptr; ++it) {
            if (it->timestamp > end_ts) break; // 超出范围，停止

            // 3. 随机读取数据 (Data Access)
            // 先读 Size
            uint64_t payload_size;
            if (::pread(fd_data_, &payload_size, sizeof(uint64_t), it->offset) != sizeof(uint64_t)) {
                break; 
            }

            // 再读 Payload
            if (read_buf.size() < payload_size) read_buf.resize(payload_size);
            if (::pread(fd_data_, read_buf.data(), payload_size, it->offset + sizeof(uint64_t)) != (ssize_t)payload_size) {
                break;
            }

            // 构造 Record
            rec.timestamp = it->timestamp;
            rec.data.assign(read_buf.begin(), read_buf.begin() + payload_size);

            if (!callback(rec)) break;
        }
    }

    Stats get_stats() const {
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

        // MMap 整个索引文件，提供极快的二分查找性能
        void* ptr = ::mmap(nullptr, idx_map_size_, PROT_READ, MAP_SHARED, fd_index_, 0);
        if (ptr == MAP_FAILED) {
            idx_map_ptr_ = nullptr;
            idx_map_size_ = 0;
            throw std::runtime_error("ROSfs: Failed to mmap index");
        }
        idx_map_ptr_ = static_cast<IndexEntry*>(ptr);
        idx_count_ = idx_map_size_ / sizeof(IndexEntry);

        // [修复] 这里必须同步 total_records_ 否则 ReadOnly 模式下 stats 不对
        total_records_ = idx_count_;
    }
};

// -----------------------------------------------------------------------------
// Pimpl Wrapper Implementation
// -----------------------------------------------------------------------------

ROSfsDB::ROSfsDB() : impl_(std::make_unique<Impl>()) {}
ROSfsDB::~ROSfsDB() = default;

void ROSfsDB::open(const std::string& path, bool read_only) {
    impl_->open(path, read_only);
}

void ROSfsDB::append(uint64_t timestamp, const std::vector<uint8_t>& data) {
    impl_->append(timestamp, data);
}

void ROSfsDB::range_query(uint64_t start_ts, uint64_t end_ts, 
                          std::function<bool(const Record&)> callback) {
    impl_->range_query(start_ts, end_ts, callback);
}

ROSfsDB::Stats ROSfsDB::get_stats() const {
    return impl_->get_stats();
}

void ROSfsDB::close() {
    impl_->close_files();
}

} // namespace rosfs