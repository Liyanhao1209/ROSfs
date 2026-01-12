// Copyright (c) 2026 TSKV Authors. All rights reserved.
// SSTable format and builder

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "tskv/coding.h"
#include "tskv/slice.h"
#include "tskv/status.h"

namespace tskv {

// SSTable format:
// +-----------------+
// | Data Block 1    |
// +-----------------+
// | Data Block 2    |
// +-----------------+
// | ...             |
// +-----------------+
// | Data Block N    |
// +-----------------+
// | Index Block     |
// +-----------------+
// | Footer          |
// +-----------------+

// Data Block format:
// +-----------------+
// | Entry 1         |  key_len | key | value_len | value
// +-----------------+
// | Entry 2         |
// +-----------------+
// | ...             |
// +-----------------+
// | Restart Array   |  Array of restart point offsets
// +-----------------+
// | Restart Count   |  uint32_t
// +-----------------+
// | CRC32           |  uint32_t
// +-----------------+

// Footer format (48 bytes):
// +-----------------+
// | Index Offset    |  uint64_t
// +-----------------+
// | Index Size      |  uint64_t
// +-----------------+
// | Min Key Offset  |  uint32_t
// +-----------------+
// | Min Key Size    |  uint32_t
// +-----------------+
// | Max Key Offset  |  uint32_t
// +-----------------+
// | Max Key Size    |  uint32_t
// +-----------------+
// | Entry Count     |  uint64_t
// +-----------------+
// | Magic Number    |  uint64_t
// +-----------------+

constexpr uint64_t kSSTableMagicNumber = 0x54534B5654534B56ULL;  // "TSKVTSKV"
constexpr size_t kFooterSize = 48;
constexpr size_t kBlockSize = 4096;  // 4KB blocks
constexpr int kRestartInterval = 16;

// Block handle for locating blocks in file
struct BlockHandle {
    uint64_t offset = 0;
    uint64_t size = 0;

    void EncodeTo(std::string* dst) const {
        PutVarint64(dst, offset);
        PutVarint64(dst, size);
    }

    Status DecodeFrom(Slice* input) {
        if (!GetVarint64(input, &offset) || !GetVarint64(input, &size)) {
            return Status::Corruption("bad block handle");
        }
        return Status::OK();
    }

private:
    static bool GetVarint64(Slice* input, uint64_t* value) {
        const char* p = input->data();
        const char* limit = p + input->size();
        const char* q = GetVarint64Ptr(p, limit, value);
        if (q == nullptr) return false;
        *input = Slice(q, limit - q);
        return true;
    }
};

// SSTable metadata for index
struct SSTableMeta {
    uint64_t file_number;
    std::string smallest_key;
    std::string largest_key;
    uint64_t file_size;
    uint64_t entry_count;
    uint64_t creation_time;

    // For skiplist index
    bool Contains(const Slice& key) const {
        return key >= Slice(smallest_key) && key <= Slice(largest_key);
    }
};

// Block builder for creating data blocks
class BlockBuilder {
public:
    explicit BlockBuilder(int restart_interval = kRestartInterval);

    // Reset the builder
    void Reset();

    // Add a key-value pair
    void Add(const Slice& key, const Slice& value);

    // Finish building the block and return its contents
    Slice Finish();

    // Returns estimated size of the current block
    size_t CurrentSizeEstimate() const;

    // Is the block empty?
    bool empty() const { return buffer_.empty(); }

private:
    std::string buffer_;
    std::vector<uint32_t> restarts_;
    int restart_interval_;
    int counter_;
    std::string last_key_;
    bool finished_;
};

inline BlockBuilder::BlockBuilder(int restart_interval)
    : restart_interval_(restart_interval), counter_(0), finished_(false) {
    restarts_.push_back(0);
}

inline void BlockBuilder::Reset() {
    buffer_.clear();
    restarts_.clear();
    restarts_.push_back(0);
    counter_ = 0;
    last_key_.clear();
    finished_ = false;
}

inline void BlockBuilder::Add(const Slice& key, const Slice& value) {
    assert(!finished_);

    size_t shared = 0;
    if (counter_ < restart_interval_) {
        // See how much sharing to do with previous key
        const size_t min_length = std::min(last_key_.size(), key.size());
        while (shared < min_length && last_key_[shared] == key[shared]) {
            shared++;
        }
    } else {
        // Restart compression
        restarts_.push_back(static_cast<uint32_t>(buffer_.size()));
        counter_ = 0;
    }
    const size_t non_shared = key.size() - shared;

    // Format: shared_bytes | non_shared_bytes | value_length | key_delta | value
    PutVarint32(&buffer_, static_cast<uint32_t>(shared));
    PutVarint32(&buffer_, static_cast<uint32_t>(non_shared));
    PutVarint32(&buffer_, static_cast<uint32_t>(value.size()));
    buffer_.append(key.data() + shared, non_shared);
    buffer_.append(value.data(), value.size());

    last_key_.assign(key.data(), key.size());
    counter_++;
}

inline Slice BlockBuilder::Finish() {
    // Append restart array
    for (uint32_t restart : restarts_) {
        PutFixed32(&buffer_, restart);
    }
    PutFixed32(&buffer_, static_cast<uint32_t>(restarts_.size()));
    finished_ = true;
    return Slice(buffer_);
}

inline size_t BlockBuilder::CurrentSizeEstimate() const {
    return buffer_.size() + restarts_.size() * sizeof(uint32_t) + sizeof(uint32_t);
}

// Block for reading
class Block {
public:
    Block(const char* data, size_t size) : data_(data), size_(size) {
        if (size_ < sizeof(uint32_t)) {
            size_ = 0;  // Error: block is too small
        } else {
            size_t max_restarts_allowed = (size_ - sizeof(uint32_t)) / sizeof(uint32_t);
            uint32_t num_restarts = DecodeFixed32(data_ + size_ - sizeof(uint32_t));
            if (num_restarts > max_restarts_allowed) {
                size_ = 0;  // Error: too many restarts
            } else {
                restart_offset_ = size_ - (1 + num_restarts) * sizeof(uint32_t);
                num_restarts_ = num_restarts;
            }
        }
    }

    class Iterator {
    public:
        Iterator(const Block* block) : block_(block), current_(0), restart_index_(0), valid_(false) {}

        bool Valid() const { return valid_; }

        void SeekToFirst() {
            SeekToRestartPoint(0);
            ParseNextKey();
        }

        void Seek(const Slice& target) {
            // Binary search in restart array to find the right restart point
            uint32_t left = 0;
            uint32_t right = block_->num_restarts_;
            
            while (left < right) {
                uint32_t mid = (left + right) / 2;
                uint32_t region_offset = GetRestartPoint(mid);
                uint32_t shared, non_shared, value_length;
                const char* key_ptr = DecodeEntry(block_->data_ + region_offset,
                                                   block_->data_ + block_->restart_offset_,
                                                   &shared, &non_shared, &value_length);
                if (key_ptr == nullptr || shared != 0) {
                    // Restart points should have shared=0
                    // If not, fall back to linear scan
                    left = 0;
                    break;
                }
                Slice mid_key(key_ptr, non_shared);
                if (mid_key < target) {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }
            
            // Ensure we start from a valid restart point
            if (left > 0) left--;

            // Linear search from restart point
            SeekToRestartPoint(left);
            while (true) {
                if (!ParseNextKey()) {
                    return;
                }
                if (key() >= target) {
                    return;
                }
            }
        }

        void Next() {
            ParseNextKey();
        }

        Slice key() const { return key_; }
        Slice value() const { return value_; }

    private:
        void SeekToRestartPoint(uint32_t index) {
            key_.clear();
            restart_index_ = index;
            current_ = GetRestartPoint(index);
            valid_ = false;
        }

        uint32_t GetRestartPoint(uint32_t index) const {
            return DecodeFixed32(block_->data_ + block_->restart_offset_ + index * sizeof(uint32_t));
        }

        bool ParseNextKey() {
            if (current_ >= block_->restart_offset_) {
                valid_ = false;
                return false;
            }

            uint32_t shared, non_shared, value_length;
            const char* p = DecodeEntry(block_->data_ + current_,
                                        block_->data_ + block_->restart_offset_,
                                        &shared, &non_shared, &value_length);
            if (p == nullptr) {
                current_ = block_->restart_offset_;
                valid_ = false;
                return false;
            }

            key_.resize(shared);
            key_.append(p, non_shared);
            value_ = Slice(p + non_shared, value_length);
            current_ = (p + non_shared + value_length) - block_->data_;
            valid_ = true;
            return true;
        }

        static const char* DecodeEntry(const char* p, const char* limit,
                                       uint32_t* shared, uint32_t* non_shared,
                                       uint32_t* value_length) {
            if (limit - p < 3) return nullptr;
            
            *shared = reinterpret_cast<const unsigned char*>(p)[0];
            *non_shared = reinterpret_cast<const unsigned char*>(p)[1];
            *value_length = reinterpret_cast<const unsigned char*>(p)[2];
            
            if ((*shared | *non_shared | *value_length) < 128) {
                // Fast path
                p += 3;
            } else {
                p = GetVarint32Ptr(p, limit, shared);
                if (p == nullptr) return nullptr;
                p = GetVarint32Ptr(p, limit, non_shared);
                if (p == nullptr) return nullptr;
                p = GetVarint32Ptr(p, limit, value_length);
                if (p == nullptr) return nullptr;
            }

            if (static_cast<uint32_t>(limit - p) < (*non_shared + *value_length)) {
                return nullptr;
            }
            return p;
        }

        const Block* block_;
        uint32_t current_;
        uint32_t restart_index_;
        std::string key_;
        Slice value_;
        bool valid_;
    };

    size_t size() const { return size_; }

private:
    const char* data_;
    size_t size_;
    uint32_t restart_offset_ = 0;
    uint32_t num_restarts_ = 0;
};

// SSTable builder
class SSTableBuilder {
public:
    SSTableBuilder(const std::string& filename);
    ~SSTableBuilder();

    // Add key-value pair (keys must be added in sorted order)
    void Add(const Slice& key, const Slice& value);

    // Finish building and close the file
    Status Finish();

    // Abandon building (delete partial file)
    void Abandon();

    // Returns number of entries
    uint64_t NumEntries() const { return num_entries_; }

    // Returns file size estimate
    uint64_t FileSize() const { return offset_; }

    // Get smallest key
    const std::string& SmallestKey() const { return smallest_key_; }

    // Get largest key
    const std::string& LargestKey() const { return largest_key_; }

private:
    void Flush();
    void WriteBlock(const Slice& block_contents, BlockHandle* handle);
    void WriteRawBlock(const Slice& block_contents, BlockHandle* handle);

    std::string filename_;
    std::ofstream file_;
    uint64_t offset_ = 0;
    
    BlockBuilder data_block_;
    std::string index_block_;  // Simple format: key | offset | size for each block
    
    std::string smallest_key_;
    std::string largest_key_;
    std::string pending_index_entry_key_;
    BlockHandle pending_handle_;
    bool pending_index_entry_ = false;
    
    uint64_t num_entries_ = 0;
    bool closed_ = false;
    Status status_;
};

inline SSTableBuilder::SSTableBuilder(const std::string& filename)
    : filename_(filename),
      file_(filename, std::ios::binary | std::ios::trunc),
      data_block_(kRestartInterval) {
    if (!file_.is_open()) {
        status_ = Status::IOError("Cannot open file: " + filename);
    }
}

inline SSTableBuilder::~SSTableBuilder() {
    if (!closed_) {
        Abandon();
    }
}

inline void SSTableBuilder::Add(const Slice& key, const Slice& value) {
    if (!status_.ok()) return;

    // Track actual smallest and largest keys (by comparison, not insertion order)
    // This is important when keys may not be added in sorted order
    if (num_entries_ == 0) {
        smallest_key_.assign(key.data(), key.size());
        largest_key_.assign(key.data(), key.size());
    } else {
        if (Slice(key) < Slice(smallest_key_)) {
            smallest_key_.assign(key.data(), key.size());
        }
        if (Slice(key) > Slice(largest_key_)) {
            largest_key_.assign(key.data(), key.size());
        }
    }

    if (pending_index_entry_) {
        // Add index entry for the previous block
        PutLengthPrefixedSlice(&index_block_, pending_index_entry_key_.data(), 
                               pending_index_entry_key_.size());
        PutVarint64(&index_block_, pending_handle_.offset);
        PutVarint64(&index_block_, pending_handle_.size);
        pending_index_entry_ = false;
    }

    data_block_.Add(key, value);
    num_entries_++;

    if (data_block_.CurrentSizeEstimate() >= kBlockSize) {
        Flush();
    }
}

inline void SSTableBuilder::Flush() {
    if (data_block_.empty()) return;
    
    Slice block_contents = data_block_.Finish();
    WriteBlock(block_contents, &pending_handle_);
    pending_index_entry_ = true;
    pending_index_entry_key_ = largest_key_;
    data_block_.Reset();
}

inline void SSTableBuilder::WriteBlock(const Slice& block_contents, BlockHandle* handle) {
    WriteRawBlock(block_contents, handle);
}

inline void SSTableBuilder::WriteRawBlock(const Slice& block_contents, BlockHandle* handle) {
    handle->offset = offset_;
    handle->size = block_contents.size();
    file_.write(block_contents.data(), block_contents.size());
    offset_ += block_contents.size();
}

inline Status SSTableBuilder::Finish() {
    if (closed_) {
        return status_;
    }
    closed_ = true;

    if (!status_.ok()) {
        return status_;
    }

    // Flush remaining data
    Flush();

    // Add last index entry
    if (pending_index_entry_) {
        PutLengthPrefixedSlice(&index_block_, pending_index_entry_key_.data(),
                               pending_index_entry_key_.size());
        PutVarint64(&index_block_, pending_handle_.offset);
        PutVarint64(&index_block_, pending_handle_.size);
    }

    // Write index block
    BlockHandle index_handle;
    index_handle.offset = offset_;
    index_handle.size = index_block_.size();
    file_.write(index_block_.data(), index_block_.size());
    offset_ += index_block_.size();

    // Write footer
    std::string footer;
    footer.reserve(kFooterSize);
    
    // Index offset and size
    PutFixed64(&footer, index_handle.offset);
    PutFixed64(&footer, index_handle.size);
    
    // Min key offset and size (stored after footer in file, we'll fix this)
    uint64_t min_key_offset = offset_ + kFooterSize;
    PutFixed32(&footer, static_cast<uint32_t>(min_key_offset));
    PutFixed32(&footer, static_cast<uint32_t>(smallest_key_.size()));
    
    // Max key offset and size
    uint64_t max_key_offset = min_key_offset + smallest_key_.size();
    PutFixed32(&footer, static_cast<uint32_t>(max_key_offset));
    PutFixed32(&footer, static_cast<uint32_t>(largest_key_.size()));
    
    // Entry count
    PutFixed64(&footer, num_entries_);
    
    // Magic number
    PutFixed64(&footer, kSSTableMagicNumber);
    
    // Pad to kFooterSize
    footer.resize(kFooterSize, 0);
    
    file_.write(footer.data(), footer.size());
    offset_ += footer.size();

    // Write min/max keys
    file_.write(smallest_key_.data(), smallest_key_.size());
    file_.write(largest_key_.data(), largest_key_.size());
    offset_ += smallest_key_.size() + largest_key_.size();

    file_.close();
    
    if (!file_.good()) {
        status_ = Status::IOError("Error writing file");
    }
    
    return status_;
}

inline void SSTableBuilder::Abandon() {
    closed_ = true;
    file_.close();
    std::remove(filename_.c_str());
}

// SSTable reader using memory-mapped I/O for high performance
class SSTableReader {
public:
    ~SSTableReader();

    // Open an SSTable file
    static Status Open(const std::string& filename, std::unique_ptr<SSTableReader>* reader);

    // Get value for key
    Status Get(const Slice& key, std::string* value) const;

    // Check if key might be in this table
    bool MayContain(const Slice& key) const {
        return key >= Slice(smallest_key_) && key <= Slice(largest_key_);
    }

    // Get smallest key
    const std::string& SmallestKey() const { return smallest_key_; }

    // Get largest key
    const std::string& LargestKey() const { return largest_key_; }

    // Get entry count
    uint64_t NumEntries() const { return num_entries_; }

    // Get file size
    uint64_t FileSize() const { return file_size_; }

    // Get file number
    uint64_t FileNumber() const { return file_number_; }

    void SetFileNumber(uint64_t num) { file_number_ = num; }

    // Iterator for scanning
    class Iterator {
    public:
        Iterator(const SSTableReader* reader);
        
        bool Valid() const { return valid_; }
        void SeekToFirst();
        void Seek(const Slice& target);
        void Next();
        
        Slice key() const { return key_; }
        Slice value() const { return value_; }

    private:
        void LoadBlock(uint64_t offset, uint64_t size);
        bool NextInBlock();

        const SSTableReader* reader_;
        bool valid_ = false;
        
        // Index iteration
        const char* index_ptr_;
        const char* index_end_;
        
        // Current block
        std::unique_ptr<Block> current_block_;
        std::unique_ptr<Block::Iterator> block_iter_;
        
        Slice key_;
        Slice value_;
    };

    Iterator* NewIterator() const {
        return new Iterator(this);
    }

private:
    SSTableReader() = default;

    int fd_ = -1;
    char* mmap_base_ = nullptr;
    size_t file_size_ = 0;
    uint64_t file_number_ = 0;

    // Parsed from footer
    uint64_t index_offset_ = 0;
    uint64_t index_size_ = 0;
    std::string smallest_key_;
    std::string largest_key_;
    uint64_t num_entries_ = 0;
};

inline SSTableReader::~SSTableReader() {
    if (mmap_base_ != nullptr) {
        munmap(mmap_base_, file_size_);
    }
    if (fd_ >= 0) {
        close(fd_);
    }
}

inline Status SSTableReader::Open(const std::string& filename, 
                                   std::unique_ptr<SSTableReader>* reader) {
    auto r = std::unique_ptr<SSTableReader>(new SSTableReader());

    r->fd_ = open(filename.c_str(), O_RDONLY);
    if (r->fd_ < 0) {
        return Status::IOError("Cannot open file: " + filename);
    }

    struct stat st;
    if (fstat(r->fd_, &st) != 0) {
        return Status::IOError("Cannot stat file: " + filename);
    }
    r->file_size_ = st.st_size;

    if (r->file_size_ < kFooterSize) {
        return Status::Corruption("File too small");
    }

    // Memory map the file
    r->mmap_base_ = static_cast<char*>(
        mmap(nullptr, r->file_size_, PROT_READ, MAP_SHARED, r->fd_, 0));
    if (r->mmap_base_ == MAP_FAILED) {
        return Status::IOError("mmap failed");
    }

    // Hint to the kernel about access pattern
    madvise(r->mmap_base_, r->file_size_, MADV_RANDOM);

    // Parse footer (at the end minus keys)
    // First we need to find the footer location
    // The footer is at a fixed position before the min/max keys
    // We need to read from the end to find it
    
    // Actually, let's read from a known position
    // We stored: data blocks | index block | footer | min_key | max_key
    // Footer starts at file_size - keys_size - kFooterSize
    // But we don't know keys_size yet...
    
    // Let's try a different approach: scan backwards for magic number
    (void)(r->mmap_base_ + r->file_size_);  // Suppress unused warning
    bool found_footer = false;
    
    // The footer contains fixed-size fields ending with magic number
    // Try to find it by scanning
    for (size_t offset = kFooterSize; offset <= r->file_size_ && offset <= kFooterSize + 1024; ) {
        const char* footer_start = r->mmap_base_ + r->file_size_ - offset;
        // Check for keys after footer
        if (offset >= kFooterSize) {
            // Potential footer at this position
            const char* magic_pos = footer_start + kFooterSize - 8;
            uint64_t magic = DecodeFixed64(magic_pos);
            if (magic == kSSTableMagicNumber) {
                // Found footer!
                r->index_offset_ = DecodeFixed64(footer_start);
                r->index_size_ = DecodeFixed64(footer_start + 8);
                
                uint32_t min_key_offset = DecodeFixed32(footer_start + 16);
                uint32_t min_key_size = DecodeFixed32(footer_start + 20);
                uint32_t max_key_offset = DecodeFixed32(footer_start + 24);
                uint32_t max_key_size = DecodeFixed32(footer_start + 28);
                
                r->num_entries_ = DecodeFixed64(footer_start + 32);
                
                if (min_key_offset + min_key_size <= r->file_size_ &&
                    max_key_offset + max_key_size <= r->file_size_) {
                    r->smallest_key_.assign(r->mmap_base_ + min_key_offset, min_key_size);
                    r->largest_key_.assign(r->mmap_base_ + max_key_offset, max_key_size);
                    found_footer = true;
                    break;
                }
            }
        }
        offset++;
    }

    if (!found_footer) {
        return Status::Corruption("Invalid SSTable: cannot find footer");
    }

    *reader = std::move(r);
    return Status::OK();
}

inline Status SSTableReader::Get(const Slice& key, std::string* value) const {
    if (!MayContain(key)) {
        return Status::NotFound();
    }

    // Search in index block to find the right data block
    const char* index_ptr = mmap_base_ + index_offset_;
    const char* index_end = index_ptr + index_size_;
    
    uint64_t target_block_offset = 0;
    uint64_t target_block_size = 0;
    bool found_block = false;

    while (index_ptr < index_end) {
        // Decode index entry: key | offset | size
        uint32_t key_len;
        const char* key_start = GetVarint32Ptr(index_ptr, index_end, &key_len);
        if (key_start == nullptr) break;
        
        Slice index_key(key_start, key_len);
        const char* offset_ptr = key_start + key_len;
        
        uint64_t block_offset, block_size;
        offset_ptr = GetVarint64Ptr(offset_ptr, index_end, &block_offset);
        if (offset_ptr == nullptr) break;
        offset_ptr = GetVarint64Ptr(offset_ptr, index_end, &block_size);
        if (offset_ptr == nullptr) break;

        if (key <= index_key) {
            target_block_offset = block_offset;
            target_block_size = block_size;
            found_block = true;
            break;
        }
        
        index_ptr = offset_ptr;
    }

    if (!found_block) {
        return Status::NotFound();
    }

    // Search in data block
    Block block(mmap_base_ + target_block_offset, target_block_size);
    Block::Iterator iter(&block);
    iter.Seek(key);
    
    if (iter.Valid() && iter.key() == key) {
        value->assign(iter.value().data(), iter.value().size());
        return Status::OK();
    }

    return Status::NotFound();
}

// SSTableReader::Iterator implementation
inline SSTableReader::Iterator::Iterator(const SSTableReader* reader)
    : reader_(reader),
      index_ptr_(reader->mmap_base_ + reader->index_offset_),
      index_end_(index_ptr_ + reader->index_size_) {}

inline void SSTableReader::Iterator::SeekToFirst() {
    index_ptr_ = reader_->mmap_base_ + reader_->index_offset_;
    
    if (index_ptr_ >= index_end_) {
        valid_ = false;
        return;
    }

    // Load first block
    uint32_t key_len;
    const char* key_start = GetVarint32Ptr(index_ptr_, index_end_, &key_len);
    if (key_start == nullptr) {
        valid_ = false;
        return;
    }
    
    const char* offset_ptr = key_start + key_len;
    uint64_t block_offset, block_size;
    offset_ptr = GetVarint64Ptr(offset_ptr, index_end_, &block_offset);
    offset_ptr = GetVarint64Ptr(offset_ptr, index_end_, &block_size);
    
    index_ptr_ = offset_ptr;
    
    LoadBlock(block_offset, block_size);
    if (current_block_) {
        block_iter_->SeekToFirst();
        if (block_iter_->Valid()) {
            key_ = block_iter_->key();
            value_ = block_iter_->value();
            valid_ = true;
        }
    }
}

inline void SSTableReader::Iterator::Seek(const Slice& target) {
    index_ptr_ = reader_->mmap_base_ + reader_->index_offset_;
    
    // Find the right block in index
    while (index_ptr_ < index_end_) {
        uint32_t key_len;
        const char* key_start = GetVarint32Ptr(index_ptr_, index_end_, &key_len);
        if (key_start == nullptr) break;
        
        Slice index_key(key_start, key_len);
        const char* offset_ptr = key_start + key_len;
        
        uint64_t block_offset, block_size;
        offset_ptr = GetVarint64Ptr(offset_ptr, index_end_, &block_offset);
        offset_ptr = GetVarint64Ptr(offset_ptr, index_end_, &block_size);

        if (target <= index_key) {
            LoadBlock(block_offset, block_size);
            index_ptr_ = offset_ptr;
            if (current_block_) {
                block_iter_->Seek(target);
                if (block_iter_->Valid()) {
                    key_ = block_iter_->key();
                    value_ = block_iter_->value();
                    valid_ = true;
                    return;
                }
            }
        }
        index_ptr_ = offset_ptr;
    }
    valid_ = false;
}

inline void SSTableReader::Iterator::Next() {
    if (!valid_) return;
    
    block_iter_->Next();
    if (block_iter_->Valid()) {
        key_ = block_iter_->key();
        value_ = block_iter_->value();
        return;
    }
    
    // Move to next block
    if (index_ptr_ >= index_end_) {
        valid_ = false;
        return;
    }
    
    uint32_t key_len;
    const char* key_start = GetVarint32Ptr(index_ptr_, index_end_, &key_len);
    if (key_start == nullptr) {
        valid_ = false;
        return;
    }
    
    const char* offset_ptr = key_start + key_len;
    uint64_t block_offset, block_size;
    offset_ptr = GetVarint64Ptr(offset_ptr, index_end_, &block_offset);
    offset_ptr = GetVarint64Ptr(offset_ptr, index_end_, &block_size);
    
    index_ptr_ = offset_ptr;
    
    LoadBlock(block_offset, block_size);
    if (current_block_) {
        block_iter_->SeekToFirst();
        if (block_iter_->Valid()) {
            key_ = block_iter_->key();
            value_ = block_iter_->value();
        } else {
            valid_ = false;
        }
    } else {
        valid_ = false;
    }
}

inline void SSTableReader::Iterator::LoadBlock(uint64_t offset, uint64_t size) {
    current_block_ = std::make_unique<Block>(reader_->mmap_base_ + offset, size);
    block_iter_ = std::make_unique<Block::Iterator>(current_block_.get());
}

}  // namespace tskv
