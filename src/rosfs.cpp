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
#include <shared_mutex>
#include <atomic>
#include <thread>

namespace rosfs {

namespace fs = std::filesystem;

// ============================================================================
// 磁盘 B+ 树布局 (mmap 持久化)
// ============================================================================

// B+ 树参数
constexpr size_t BTREE_ORDER = 64;  // 每个节点最多 64 个 key
constexpr size_t BTREE_MIN_KEYS = BTREE_ORDER / 2;

// 索引条目 (叶节点中存储)
struct IndexEntry {
    uint64_t timestamp;
    uint64_t offset;
} __attribute__((packed));

// B+ 树节点 (固定大小，便于 mmap)
// 节点大小 = 4096 bytes (一个页)
struct alignas(4096) BTreeNode {
    uint32_t is_leaf;           // 是否叶节点
    uint32_t num_keys;          // 当前 key 数量
    uint64_t keys[BTREE_ORDER]; // timestamps
    
    union {
        uint64_t values[BTREE_ORDER];      // 叶节点: data offsets
        uint32_t children[BTREE_ORDER + 1]; // 内部节点: 子节点索引 (节点号)
    };
    
    uint32_t next_leaf;         // 叶节点链表 (节点号, 0 表示无)
    uint32_t parent;            // 父节点索引
    uint8_t  padding[4096 - sizeof(uint32_t)*4 - sizeof(uint64_t)*BTREE_ORDER*2 - sizeof(uint32_t)*(BTREE_ORDER+1)];
} __attribute__((packed));

static_assert(sizeof(BTreeNode) == 4096, "BTreeNode must be 4096 bytes");

// B+ 树文件头
struct alignas(4096) BTreeHeader {
    uint64_t magic;             // 魔数 0x42505452454558  "BPTREIDX"
    uint32_t version;           // 版本号
    uint32_t root_node;         // 根节点索引 (0 表示空树)
    uint32_t first_leaf;        // 第一个叶节点索引
    uint32_t node_count;        // 已分配节点数
    uint64_t record_count;      // 记录总数
    uint8_t  padding[4096 - 32];
} __attribute__((packed));

static_assert(sizeof(BTreeHeader) == 4096, "BTreeHeader must be 4096 bytes");

constexpr uint64_t BTREE_MAGIC = 0x5844494552545042ULL; // "BPTREIDX" little-endian

// ============================================================================
// MMap B+ 树实现
// ============================================================================

class MMapBPlusTree {
private:
    int fd_ = -1;
    void* map_base_ = nullptr;
    size_t map_size_ = 0;
    size_t file_size_ = 0;
    bool read_only_ = false;
    
    BTreeHeader* header_ = nullptr;
    
    static constexpr size_t INITIAL_FILE_SIZE = 4096 * 1024; // 4MB 初始大小
    static constexpr size_t GROW_SIZE = 4096 * 256;          // 每次增长 1MB
    
public:
    MMapBPlusTree() = default;
    
    ~MMapBPlusTree() {
        close();
    }
    
    // 禁用拷贝
    MMapBPlusTree(const MMapBPlusTree&) = delete;
    MMapBPlusTree& operator=(const MMapBPlusTree&) = delete;
    
    void open(const std::string& path, bool read_only) {
        read_only_ = read_only;
        
        int flags = read_only ? O_RDONLY : (O_RDWR | O_CREAT);
        fd_ = ::open(path.c_str(), flags, 0644);
        if (fd_ < 0) {
            throw std::runtime_error("MMapBPlusTree: Failed to open file: " + path);
        }
        
        struct stat st;
        ::fstat(fd_, &st);
        file_size_ = st.st_size;
        
        if (file_size_ == 0) {
            if (read_only) {
                // 空文件，只读模式
                header_ = nullptr;
                return;
            }
            // 初始化新文件
            init_new_file();
        } else {
            // 映射已有文件
            remap(file_size_);
            
            // 验证魔数
            if (header_->magic != BTREE_MAGIC) {
                close();
                throw std::runtime_error("MMapBPlusTree: Invalid file format");
            }
        }
    }
    
    void close() {
        if (map_base_) {
            if (!read_only_) {
                ::msync(map_base_, map_size_, MS_SYNC);
            }
            ::munmap(map_base_, map_size_);
            map_base_ = nullptr;
            header_ = nullptr;
        }
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        map_size_ = 0;
        file_size_ = 0;
    }
    
    void insert(uint64_t timestamp, uint64_t offset) {
        if (read_only_) {
            throw std::runtime_error("MMapBPlusTree: Cannot insert in read-only mode");
        }
        
        if (!header_ || header_->root_node == 0) {
            // 空树，创建根节点
            uint32_t root_idx = allocate_node();
            BTreeNode* root = get_node(root_idx);
            root->is_leaf = 1;
            root->num_keys = 1;
            root->keys[0] = timestamp;
            root->values[0] = offset;
            root->next_leaf = 0;
            root->parent = 0;
            
            header_->root_node = root_idx;
            header_->first_leaf = root_idx;
            header_->record_count = 1;
            return;
        }
        
        // 找到目标叶节点
        uint32_t leaf_idx = find_leaf(timestamp);
        BTreeNode* leaf = get_node(leaf_idx);
        
        // 插入到叶节点
        insert_into_leaf(leaf_idx, timestamp, offset);
        header_->record_count++;
    }
    
    void range_query(uint64_t start_ts, uint64_t end_ts,
                     std::vector<IndexEntry>& results) const {
        if (!header_ || header_->root_node == 0) {
            return;
        }
        
        // 找到起始叶节点
        uint32_t leaf_idx = find_leaf_const(start_ts);
        if (leaf_idx == 0) return;
        
        const BTreeNode* leaf = get_node_const(leaf_idx);
        
        // 在叶节点中找到起始位置
        uint32_t idx = 0;
        while (idx < leaf->num_keys && leaf->keys[idx] < start_ts) {
            idx++;
        }
        
        // 遍历叶节点链表
        while (leaf_idx != 0) {
            for (; idx < leaf->num_keys; idx++) {
                if (leaf->keys[idx] > end_ts) {
                    return;
                }
                results.push_back({leaf->keys[idx], leaf->values[idx]});
            }
            leaf_idx = leaf->next_leaf;
            if (leaf_idx != 0) {
                leaf = get_node_const(leaf_idx);
                idx = 0;
            }
        }
    }
    
    uint64_t record_count() const {
        return header_ ? header_->record_count : 0;
    }
    
    size_t file_size() const {
        return file_size_;
    }
    
    void sync() {
        if (map_base_ && !read_only_) {
            ::msync(map_base_, map_size_, MS_SYNC);
        }
    }
    
    bool is_open() const {
        return fd_ >= 0;
    }
    
private:
    void init_new_file() {
        // 扩展文件
        if (::ftruncate(fd_, INITIAL_FILE_SIZE) != 0) {
            throw std::runtime_error("MMapBPlusTree: Failed to initialize file");
        }
        file_size_ = INITIAL_FILE_SIZE;
        
        // 映射
        remap(file_size_);
        
        // 初始化头部
        header_->magic = BTREE_MAGIC;
        header_->version = 1;
        header_->root_node = 0;
        header_->first_leaf = 0;
        header_->node_count = 0;
        header_->record_count = 0;
    }
    
    void remap(size_t size) {
        if (map_base_) {
            if (!read_only_) {
                ::msync(map_base_, map_size_, MS_SYNC);
            }
            ::munmap(map_base_, map_size_);
        }
        
        int prot = read_only_ ? PROT_READ : (PROT_READ | PROT_WRITE);
        map_base_ = ::mmap(nullptr, size, prot, MAP_SHARED, fd_, 0);
        if (map_base_ == MAP_FAILED) {
            map_base_ = nullptr;
            throw std::runtime_error("MMapBPlusTree: mmap failed");
        }
        map_size_ = size;
        header_ = static_cast<BTreeHeader*>(map_base_);
    }
    
    void ensure_capacity(size_t needed_nodes) {
        size_t needed_size = sizeof(BTreeHeader) + needed_nodes * sizeof(BTreeNode);
        if (needed_size > file_size_) {
            size_t new_size = file_size_ + GROW_SIZE;
            while (new_size < needed_size) {
                new_size += GROW_SIZE;
            }
            
            if (::ftruncate(fd_, new_size) != 0) {
                throw std::runtime_error("MMapBPlusTree: Failed to grow file");
            }
            file_size_ = new_size;
            remap(file_size_);
        }
    }
    
    uint32_t allocate_node() {
        ensure_capacity(header_->node_count + 2);
        uint32_t idx = ++header_->node_count;
        BTreeNode* node = get_node(idx);
        std::memset(node, 0, sizeof(BTreeNode));
        return idx;
    }
    
    BTreeNode* get_node(uint32_t idx) {
        if (idx == 0) return nullptr;
        char* base = static_cast<char*>(map_base_);
        return reinterpret_cast<BTreeNode*>(base + sizeof(BTreeHeader) + (idx - 1) * sizeof(BTreeNode));
    }
    
    const BTreeNode* get_node_const(uint32_t idx) const {
        if (idx == 0) return nullptr;
        const char* base = static_cast<const char*>(map_base_);
        return reinterpret_cast<const BTreeNode*>(base + sizeof(BTreeHeader) + (idx - 1) * sizeof(BTreeNode));
    }
    
    uint32_t find_leaf(uint64_t key) const {
        uint32_t node_idx = header_->root_node;
        while (node_idx != 0) {
            const BTreeNode* node = get_node_const(node_idx);
            if (node->is_leaf) {
                return node_idx;
            }
            // 内部节点：找到正确的子节点
            uint32_t i = 0;
            while (i < node->num_keys && key >= node->keys[i]) {
                i++;
            }
            node_idx = node->children[i];
        }
        return 0;
    }
    
    uint32_t find_leaf_const(uint64_t key) const {
        return find_leaf(key);
    }
    
    void insert_into_leaf(uint32_t leaf_idx, uint64_t key, uint64_t value) {
        BTreeNode* leaf = get_node(leaf_idx);
        
        if (leaf->num_keys < BTREE_ORDER - 1) {
            // 叶节点有空间，直接插入
            uint32_t i = leaf->num_keys;
            while (i > 0 && key < leaf->keys[i - 1]) {
                leaf->keys[i] = leaf->keys[i - 1];
                leaf->values[i] = leaf->values[i - 1];
                i--;
            }
            leaf->keys[i] = key;
            leaf->values[i] = value;
            leaf->num_keys++;
        } else {
            // 叶节点满，需要分裂
            split_leaf_and_insert(leaf_idx, key, value);
        }
    }
    
    void split_leaf_and_insert(uint32_t leaf_idx, uint64_t key, uint64_t value) {
        BTreeNode* leaf = get_node(leaf_idx);
        
        // 临时数组存储所有 key-value
        uint64_t temp_keys[BTREE_ORDER];
        uint64_t temp_values[BTREE_ORDER];
        
        // 复制并插入新 key
        uint32_t insert_pos = 0;
        while (insert_pos < leaf->num_keys && leaf->keys[insert_pos] < key) {
            insert_pos++;
        }
        
        for (uint32_t i = 0; i < insert_pos; i++) {
            temp_keys[i] = leaf->keys[i];
            temp_values[i] = leaf->values[i];
        }
        temp_keys[insert_pos] = key;
        temp_values[insert_pos] = value;
        for (uint32_t i = insert_pos; i < leaf->num_keys; i++) {
            temp_keys[i + 1] = leaf->keys[i];
            temp_values[i + 1] = leaf->values[i];
        }
        
        uint32_t total = leaf->num_keys + 1;
        uint32_t split_point = total / 2;
        
        // 创建新叶节点
        uint32_t new_leaf_idx = allocate_node();
        // 注意：allocate_node 可能 remap，需要重新获取指针
        leaf = get_node(leaf_idx);
        BTreeNode* new_leaf = get_node(new_leaf_idx);
        
        new_leaf->is_leaf = 1;
        new_leaf->parent = leaf->parent;
        new_leaf->next_leaf = leaf->next_leaf;
        leaf->next_leaf = new_leaf_idx;
        
        // 分配 key
        leaf->num_keys = split_point;
        for (uint32_t i = 0; i < split_point; i++) {
            leaf->keys[i] = temp_keys[i];
            leaf->values[i] = temp_values[i];
        }
        
        new_leaf->num_keys = total - split_point;
        for (uint32_t i = 0; i < new_leaf->num_keys; i++) {
            new_leaf->keys[i] = temp_keys[split_point + i];
            new_leaf->values[i] = temp_values[split_point + i];
        }
        
        // 将分裂产生的 key 插入父节点
        uint64_t up_key = new_leaf->keys[0];
        insert_into_parent(leaf_idx, up_key, new_leaf_idx);
    }
    
    void insert_into_parent(uint32_t left_idx, uint64_t key, uint32_t right_idx) {
        BTreeNode* left = get_node(left_idx);
        uint32_t parent_idx = left->parent;
        
        if (parent_idx == 0) {
            // 创建新根
            uint32_t new_root_idx = allocate_node();
            // remap 后重新获取指针
            left = get_node(left_idx);
            BTreeNode* right = get_node(right_idx);
            BTreeNode* new_root = get_node(new_root_idx);
            
            new_root->is_leaf = 0;
            new_root->num_keys = 1;
            new_root->keys[0] = key;
            new_root->children[0] = left_idx;
            new_root->children[1] = right_idx;
            new_root->parent = 0;
            
            left->parent = new_root_idx;
            right->parent = new_root_idx;
            
            header_->root_node = new_root_idx;
            return;
        }
        
        BTreeNode* parent = get_node(parent_idx);
        
        if (parent->num_keys < BTREE_ORDER - 1) {
            // 父节点有空间
            uint32_t i = parent->num_keys;
            while (i > 0 && key < parent->keys[i - 1]) {
                parent->keys[i] = parent->keys[i - 1];
                parent->children[i + 1] = parent->children[i];
                i--;
            }
            parent->keys[i] = key;
            parent->children[i + 1] = right_idx;
            parent->num_keys++;
            
            get_node(right_idx)->parent = parent_idx;
        } else {
            // 父节点也满了，需要分裂
            split_internal_and_insert(parent_idx, key, right_idx);
        }
    }
    
    void split_internal_and_insert(uint32_t node_idx, uint64_t key, uint32_t right_child_idx) {
        BTreeNode* node = get_node(node_idx);
        
        // 临时数组
        uint64_t temp_keys[BTREE_ORDER];
        uint32_t temp_children[BTREE_ORDER + 1];
        
        // 找到插入位置
        uint32_t insert_pos = 0;
        while (insert_pos < node->num_keys && key >= node->keys[insert_pos]) {
            insert_pos++;
        }
        
        // 复制 keys
        for (uint32_t i = 0; i < insert_pos; i++) {
            temp_keys[i] = node->keys[i];
        }
        temp_keys[insert_pos] = key;
        for (uint32_t i = insert_pos; i < node->num_keys; i++) {
            temp_keys[i + 1] = node->keys[i];
        }
        
        // 复制 children
        for (uint32_t i = 0; i <= insert_pos; i++) {
            temp_children[i] = node->children[i];
        }
        temp_children[insert_pos + 1] = right_child_idx;
        for (uint32_t i = insert_pos + 1; i <= node->num_keys; i++) {
            temp_children[i + 1] = node->children[i];
        }
        
        uint32_t total_keys = node->num_keys + 1;
        uint32_t split_point = total_keys / 2;
        
        // 创建新内部节点
        uint32_t new_node_idx = allocate_node();
        // remap 后重新获取指针
        node = get_node(node_idx);
        BTreeNode* new_node = get_node(new_node_idx);
        
        new_node->is_leaf = 0;
        new_node->parent = node->parent;
        
        // 左节点保留前半部分
        node->num_keys = split_point;
        for (uint32_t i = 0; i < split_point; i++) {
            node->keys[i] = temp_keys[i];
        }
        for (uint32_t i = 0; i <= split_point; i++) {
            node->children[i] = temp_children[i];
        }
        
        // 中间 key 上提
        uint64_t up_key = temp_keys[split_point];
        
        // 右节点保留后半部分
        new_node->num_keys = total_keys - split_point - 1;
        for (uint32_t i = 0; i < new_node->num_keys; i++) {
            new_node->keys[i] = temp_keys[split_point + 1 + i];
        }
        for (uint32_t i = 0; i <= new_node->num_keys; i++) {
            new_node->children[i] = temp_children[split_point + 1 + i];
            // 更新子节点的 parent
            get_node(new_node->children[i])->parent = new_node_idx;
        }
        
        // 更新原节点子节点的 parent
        for (uint32_t i = 0; i <= node->num_keys; i++) {
            get_node(node->children[i])->parent = node_idx;
        }
        
        // 递归插入父节点
        insert_into_parent(node_idx, up_key, new_node_idx);
    }
};

// ============================================================================
// ROSfsDB 实现
// ============================================================================

class ROSfsDB::Impl {
public:
    std::string base_path_;
    std::string data_file_path_;
    std::string index_file_path_;

    int fd_data_ = -1;
    bool read_only_ = false;

    uint64_t current_data_offset_ = 0;
    
    // 磁盘 B+ 树索引 (mmap 持久化)
    MMapBPlusTree index_tree_;

    // 读写锁: 支持多读单写
    mutable std::shared_mutex rw_mutex_;
    
    // 关闭标志
    std::atomic<bool> closed_{true};
    
    // 活跃操作计数器
    std::atomic<int> active_ops_{0};

    Impl() = default;

    ~Impl() {
        close_impl();
    }

    void open(const std::string& path, bool read_only) {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        
        // 如果已经打开，先关闭
        if (!closed_.load()) {
            close_internal();
        }
        
        base_path_ = path;
        read_only_ = read_only;
        data_file_path_ = base_path_ + "/data.bin";
        index_file_path_ = base_path_ + "/index.bpt";  // 改为 .bpt 后缀

        if (!fs::exists(base_path_)) {
            if (read_only) {
                throw std::runtime_error("ROSfs: DB path not found: " + path);
            }
            fs::create_directories(base_path_);
        }

        // 打开数据文件
        int flags = read_only ? O_RDONLY : (O_RDWR | O_CREAT);
        fd_data_ = ::open(data_file_path_.c_str(), flags, 0644);
        if (fd_data_ < 0) {
            throw std::runtime_error("ROSfs: Failed to open data file: " + data_file_path_);
        }

        // 获取数据文件大小
        struct stat st;
        ::fstat(fd_data_, &st);
        current_data_offset_ = st.st_size;
        
        // 打开 B+ 树索引
        try {
            index_tree_.open(index_file_path_, read_only);
        } catch (...) {
            ::close(fd_data_);
            fd_data_ = -1;
            throw;
        }
        
        closed_.store(false);
    }

    void close_files() {
        // 标记关闭，让正在进行的操作尽快退出
        closed_.store(true);
        
        // 等待活跃操作完成
        while (active_ops_.load() > 0) {
            std::this_thread::yield();
        }
        
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        close_internal();
    }
    
    void close_impl() {
        closed_.store(true);
        while (active_ops_.load() > 0) {
            std::this_thread::yield();
        }
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        close_internal();
    }
    
    void close_internal() {
        // 必须在持有写锁的情况下调用
        index_tree_.close();
        if (fd_data_ >= 0) { 
            ::close(fd_data_); 
            fd_data_ = -1; 
        }
        closed_.store(true);
    }

    void append(uint64_t timestamp, const std::vector<uint8_t>& data) {
        // 检查是否已关闭
        if (closed_.load()) {
            throw std::runtime_error("ROSfs: Database is closed");
        }
        
        // 增加活跃操作计数
        active_ops_++;
        struct OpGuard {
            std::atomic<int>& counter;
            ~OpGuard() { counter--; }
        } guard{active_ops_};
        
        // 数据准备 (无锁)
        uint64_t size = data.size();
        std::vector<uint8_t> buffer(sizeof(uint64_t) + size);
        std::memcpy(buffer.data(), &size, sizeof(uint64_t));
        std::memcpy(buffer.data() + sizeof(uint64_t), data.data(), size);

        // 写操作需要独占锁
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        
        if (closed_.load()) {
            throw std::runtime_error("ROSfs: Database is closed");
        }
        
        if (read_only_) {
            throw std::runtime_error("ROSfs: Cannot append in read-only mode");
        }
        
        if (fd_data_ < 0) {
            throw std::runtime_error("ROSfs: Database not open");
        }

        // 1. 写入数据文件
        ssize_t written = ::pwrite(fd_data_, buffer.data(), buffer.size(), current_data_offset_);
        if (written != (ssize_t)buffer.size()) {
            throw std::runtime_error("ROSfs: Data write failed");
        }

        // 2. 插入 B+ 树索引 (会自动持久化到 mmap 文件)
        index_tree_.insert(timestamp, current_data_offset_);

        // 3. 更新数据偏移
        current_data_offset_ += written;
    }

    void range_query(uint64_t start_ts, uint64_t end_ts, 
                     std::function<bool(const Record&)> callback) {
        // 检查是否已关闭
        if (closed_.load()) {
            throw std::runtime_error("ROSfs: Database is closed");
        }
        
        // 增加活跃操作计数
        active_ops_++;
        struct OpGuard {
            std::atomic<int>& counter;
            ~OpGuard() { counter--; }
        } guard{active_ops_};
        
        // 快照读：复制索引条目，然后释放锁读数据
        std::vector<IndexEntry> target_indices;
        int local_fd_data;
        
        {
            std::shared_lock<std::shared_mutex> lock(rw_mutex_);
            
            if (closed_.load() || fd_data_ < 0) {
                throw std::runtime_error("ROSfs: Database is closed or not open");
            }
            
            // 从 B+ 树获取范围内的索引
            index_tree_.range_query(start_ts, end_ts, target_indices);
            local_fd_data = fd_data_;
        }
        
        // 释放锁后读取数据 (pread 是线程安全的)
        std::vector<uint8_t> read_buf;
        Record rec;
        
        for (const auto& entry : target_indices) {
            if (closed_.load()) {
                break;
            }
            
            uint64_t payload_size;
            ssize_t ret = ::pread(local_fd_data, &payload_size, sizeof(uint64_t), entry.offset);
            if (ret != sizeof(uint64_t)) {
                break;
            }
            
            if (read_buf.size() < payload_size) {
                read_buf.resize(payload_size);
            }
            
            ret = ::pread(local_fd_data, read_buf.data(), payload_size, entry.offset + sizeof(uint64_t));
            if (ret != (ssize_t)payload_size) {
                break;
            }

            rec.timestamp = entry.timestamp;
            rec.data.assign(read_buf.begin(), read_buf.begin() + payload_size);
            
            if (!callback(rec)) {
                break;
            }
        }
    }

    Stats get_stats() const {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        
        Stats s;
        struct stat st;
        
        if (fd_data_ >= 0 && ::fstat(fd_data_, &st) == 0) {
            s.data_file_size = st.st_size;
        } else {
            s.data_file_size = 0;
        }
        
        s.index_file_size = index_tree_.file_size();
        s.total_records = index_tree_.record_count();
        
        return s;
    }
    
    void sync() {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        if (fd_data_ >= 0) {
            ::fsync(fd_data_);
        }
        index_tree_.sync();
    }
};

// ============================================================================
// Public API
// ============================================================================

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