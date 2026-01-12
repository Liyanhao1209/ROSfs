// Copyright (c) 2026 TSKV Authors. All rights reserved.
// MemTable - In-memory write buffer

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>

#include "tskv/arena.h"
#include "tskv/coding.h"
#include "tskv/skiplist.h"
#include "tskv/slice.h"
#include "tskv/status.h"

namespace tskv {

// Value types for internal keys
enum ValueType : uint8_t {
    kTypeDeletion = 0x0,
    kTypeValue = 0x1,
};

// MemTable key format:
// key_size (varint32) | key_data | sequence (7 bytes) | type (1 byte) | value_size (varint32) | value_data

class MemTable {
public:
    explicit MemTable(size_t write_buffer_size = 64 * 1024 * 1024);  // 64MB default

    // Disallow copy
    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;

    // Add an entry into memtable
    void Add(uint64_t seq, ValueType type, const Slice& key, const Slice& value);

    // If memtable contains a value for key, store it in *value and return true.
    // If memtable contains a deletion for key, store NotFound in *s and return true.
    // Else, return false.
    bool Get(const Slice& key, std::string* value, Status* s) const;

    // Return an estimate of the number of bytes of data in use by this table
    size_t ApproximateMemoryUsage() const {
        return arena_.MemoryUsage();
    }

    // Return the number of entries
    size_t Count() const { return table_.Count(); }

    // Check if the memtable should be flushed
    bool ShouldFlush() const {
        return arena_.MemoryUsage() >= write_buffer_size_;
    }

    // Get the first key in memtable (smallest)
    bool GetFirstKey(std::string* key) const;

    // Get the last key in memtable (largest)
    bool GetLastKey(std::string* key) const;

    // Iterator for memtable
    class Iterator {
    public:
        explicit Iterator(const MemTable* mem);

        bool Valid() const { return iter_.Valid(); }
        
        void SeekToFirst() { iter_.SeekToFirst(); }
        void SeekToLast() { iter_.SeekToLast(); }
        void Seek(const Slice& target);
        void Next() { iter_.Next(); }
        void Prev() { iter_.Prev(); }

        // Returns the user key
        Slice key() const;
        
        // Returns the value
        Slice value() const;

        // Returns the sequence number
        uint64_t sequence() const;

        // Returns the value type
        ValueType type() const;

    private:
        SkipList<BytewiseComparator>::Iterator iter_;
    };

    Iterator* NewIterator() const {
        return new Iterator(this);
    }

    // Reference counting
    void Ref() { refs_.fetch_add(1, std::memory_order_relaxed); }
    void Unref() {
        if (refs_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            delete this;
        }
    }

private:
    friend class Iterator;

    struct KeyComparator {
        int operator()(const char* a, const char* b) const;
    };

    // Encode key for internal use
    static void EncodeKey(std::string* buf, const Slice& key, uint64_t seq, ValueType type);

    // Decode internal key
    static void DecodeKey(const char* entry, Slice* key, uint64_t* seq, ValueType* type, Slice* value);

    mutable Arena arena_;
    SkipList<BytewiseComparator> table_;
    size_t write_buffer_size_;
    std::atomic<int> refs_{0};
};

// Implementation

inline MemTable::MemTable(size_t write_buffer_size)
    : arena_(4096),
      table_(BytewiseComparator(), &arena_),
      write_buffer_size_(write_buffer_size) {}

inline void MemTable::Add(uint64_t seq, ValueType type, const Slice& key, const Slice& value) {
    // Format:
    // key_size | key | seq_type | value_size | value
    size_t key_size = key.size();
    size_t val_size = value.size();
    size_t internal_key_size = key_size + 8;  // 8 bytes for seq + type
    
    size_t encoded_len = VarintLength(internal_key_size) + internal_key_size +
                         VarintLength(val_size) + val_size;
    
    char* buf = arena_.Allocate(encoded_len);
    char* p = buf;
    
    // Encode internal key size
    p = EncodeVarint32(p, static_cast<uint32_t>(internal_key_size));
    
    // Encode user key
    memcpy(p, key.data(), key_size);
    p += key_size;
    
    // Encode sequence number and type (packed: seq << 8 | type)
    uint64_t tag = (seq << 8) | type;
    EncodeFixed64(p, tag);
    p += 8;
    
    // Encode value size and value
    p = EncodeVarint32(p, static_cast<uint32_t>(val_size));
    memcpy(p, value.data(), val_size);
    
    table_.Insert(buf);
}

inline bool MemTable::Get(const Slice& key, std::string* value, Status* s) const {
    // Build lookup key - we need to find entries with matching user key
    // The internal key format is: user_key + (seq << 8 | type)
    // Skiplist compares these lexicographically, so we search for the user key
    // and then iterate to find matching entries
    
    SkipList<BytewiseComparator>::Iterator iter(&table_);
    iter.SeekToFirst();
    
    // Linear scan to find matching key (simple but correct approach)
    // For time-series data with monotonic keys, this is typically fast
    // because we're looking for recent keys near the end
    uint64_t best_seq = 0;
    ValueType best_type = kTypeDeletion;
    const char* best_value_ptr = nullptr;
    uint32_t best_value_length = 0;
    bool found = false;
    
    while (iter.Valid()) {
        const char* entry = iter.key();
        uint32_t key_length;
        const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
        if (key_ptr && key_length >= 8) {
            // Extract user key (without seq + type)
            Slice entry_key(key_ptr, key_length - 8);
            
            if (entry_key == key) {
                // Found matching key - check sequence number
                const char* tag_ptr = key_ptr + key_length - 8;
                uint64_t tag_val = DecodeFixed64(tag_ptr);
                uint64_t seq = tag_val >> 8;
                ValueType vtype = static_cast<ValueType>(tag_val & 0xff);
                
                // Keep the entry with highest sequence number
                if (!found || seq > best_seq) {
                    best_seq = seq;
                    best_type = vtype;
                    if (vtype == kTypeValue) {
                        const char* vptr = key_ptr + key_length;
                        GetVarint32Ptr(vptr, vptr + 5, &best_value_length);
                        best_value_ptr = vptr;
                    }
                    found = true;
                }
            } else if (found && entry_key > key) {
                // Past our key, stop searching
                break;
            }
        }
        iter.Next();
    }
    
    if (found) {
        if (best_type == kTypeValue && best_value_ptr) {
            uint32_t vlen;
            const char* vdata = GetVarint32Ptr(best_value_ptr, best_value_ptr + 5, &vlen);
            if (vdata) {
                value->assign(vdata, vlen);
            }
            return true;
        } else if (best_type == kTypeDeletion) {
            *s = Status::NotFound();
            return true;
        }
    }
    
    return false;
}

inline bool MemTable::GetFirstKey(std::string* key) const {
    const char* first = table_.GetFirstKey();
    if (first == nullptr) return false;
    
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(first, first + 5, &key_length);
    key->assign(key_ptr, key_length - 8);  // Exclude seq + type
    return true;
}

inline bool MemTable::GetLastKey(std::string* key) const {
    const char* last = table_.GetLastKey();
    if (last == nullptr) return false;
    
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(last, last + 5, &key_length);
    key->assign(key_ptr, key_length - 8);  // Exclude seq + type
    return true;
}

// Iterator implementation

inline MemTable::Iterator::Iterator(const MemTable* mem) : iter_(&mem->table_) {}

inline void MemTable::Iterator::Seek(const Slice& target) {
    // Build a lookup key with sequence 0 and type value
    // This will position us at the first entry >= target user key
    std::string lookup_key;
    size_t internal_key_size = target.size() + 8;
    PutVarint32(&lookup_key, static_cast<uint32_t>(internal_key_size));
    lookup_key.append(target.data(), target.size());
    
    // Use sequence 0 to position at or before matching keys
    uint64_t tag = (0ULL << 8) | kTypeValue;
    char buf[8];
    EncodeFixed64(buf, tag);
    lookup_key.append(buf, 8);
    
    iter_.Seek(lookup_key.data());
}

inline Slice MemTable::Iterator::key() const {
    const char* entry = iter_.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    return Slice(key_ptr, key_length - 8);  // Exclude seq + type
}

inline Slice MemTable::Iterator::value() const {
    const char* entry = iter_.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    const char* value_ptr = key_ptr + key_length;
    uint32_t value_length;
    value_ptr = GetVarint32Ptr(value_ptr, value_ptr + 5, &value_length);
    return Slice(value_ptr, value_length);
}

inline uint64_t MemTable::Iterator::sequence() const {
    const char* entry = iter_.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    const char* tag_ptr = key_ptr + key_length - 8;
    uint64_t tag = DecodeFixed64(tag_ptr);
    return tag >> 8;
}

inline ValueType MemTable::Iterator::type() const {
    const char* entry = iter_.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    const char* tag_ptr = key_ptr + key_length - 8;
    uint64_t tag = DecodeFixed64(tag_ptr);
    return static_cast<ValueType>(tag & 0xff);
}

}  // namespace tskv
