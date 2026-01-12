// Copyright (c) 2026 TSKV Authors. All rights reserved.
// Time-Series Key-Value Store - A high-performance storage engine
// optimized for time-series robotic data without compaction.

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace tskv {

// Slice is a simple structure containing a pointer to external storage and
// size. Similar to RocksDB's Slice, optimized for zero-copy operations.
class Slice {
public:
    // Create an empty slice
    Slice() : data_(""), size_(0) {}

    // Create a slice that refers to d[0,n-1]
    Slice(const char* d, size_t n) : data_(d), size_(n) {}

    // Create a slice from std::string
    /* implicit */ Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}

    // Create a slice from string_view
    /* implicit */ Slice(std::string_view sv) : data_(sv.data()), size_(sv.size()) {}

    // Create a slice from null-terminated C string
    /* implicit */ Slice(const char* s) : data_(s), size_(s ? strlen(s) : 0) {}

    // Return a pointer to the beginning of the referenced data
    const char* data() const { return data_; }

    // Return the length of the referenced data
    size_t size() const { return size_; }

    // Return true if the length is zero
    bool empty() const { return size_ == 0; }

    // Return the ith byte in the referenced data
    char operator[](size_t n) const {
        assert(n < size_);
        return data_[n];
    }

    // Clear the slice
    void clear() {
        data_ = "";
        size_ = 0;
    }

    // Drop the first n bytes
    void remove_prefix(size_t n) {
        assert(n <= size_);
        data_ += n;
        size_ -= n;
    }

    // Drop the last n bytes
    void remove_suffix(size_t n) {
        assert(n <= size_);
        size_ -= n;
    }

    // Return a string copy
    std::string ToString() const { return std::string(data_, size_); }

    // Return a string_view
    std::string_view ToStringView() const { return std::string_view(data_, size_); }

    // Three-way comparison
    int compare(const Slice& b) const {
        const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
        int r = memcmp(data_, b.data_, min_len);
        if (r == 0) {
            if (size_ < b.size_) r = -1;
            else if (size_ > b.size_) r = +1;
        }
        return r;
    }

    // Return true if x is a prefix of *this
    bool starts_with(const Slice& x) const {
        return (size_ >= x.size_) && (memcmp(data_, x.data_, x.size_) == 0);
    }

    bool ends_with(const Slice& x) const {
        return (size_ >= x.size_) &&
               (memcmp(data_ + size_ - x.size_, x.data_, x.size_) == 0);
    }

    bool operator==(const Slice& other) const {
        return size_ == other.size_ && memcmp(data_, other.data_, size_) == 0;
    }

    bool operator!=(const Slice& other) const { return !(*this == other); }

    bool operator<(const Slice& other) const { return compare(other) < 0; }

    bool operator<=(const Slice& other) const { return compare(other) <= 0; }

    bool operator>(const Slice& other) const { return compare(other) > 0; }

    bool operator>=(const Slice& other) const { return compare(other) >= 0; }

private:
    const char* data_;
    size_t size_;
};

// Hash function for Slice
struct SliceHash {
    size_t operator()(const Slice& s) const {
        // FNV-1a hash
        size_t hash = 14695981039346656037ULL;
        for (size_t i = 0; i < s.size(); ++i) {
            hash ^= static_cast<unsigned char>(s[i]);
            hash *= 1099511628211ULL;
        }
        return hash;
    }
};

}  // namespace tskv
