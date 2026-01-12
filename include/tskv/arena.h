// Copyright (c) 2026 TSKV Authors. All rights reserved.
// Memory Arena for efficient allocation

#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

namespace tskv {

// Arena is a simple memory pool for efficient allocation.
// Thread-safe for allocation, not for destruction.
class Arena {
public:
    explicit Arena(size_t block_size = kDefaultBlockSize);
    ~Arena() = default;

    // Disallow copy
    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;

    // Allow move
    Arena(Arena&&) noexcept = default;
    Arena& operator=(Arena&&) noexcept = default;

    // Return a pointer to a newly allocated memory block of "bytes" bytes
    char* Allocate(size_t bytes);

    // Allocate memory with specified alignment
    char* AllocateAligned(size_t bytes, size_t alignment = alignof(std::max_align_t));

    // Return the total memory usage
    size_t MemoryUsage() const {
        return memory_usage_.load(std::memory_order_relaxed);
    }

    // Return approximate memory usage
    size_t ApproximateMemoryUsage() const { return MemoryUsage(); }

    // Reset the arena (clear all allocations)
    void Reset();

    static constexpr size_t kDefaultBlockSize = 4096;

private:
    char* AllocateFallback(size_t bytes);
    char* AllocateNewBlock(size_t block_bytes);

    // Current allocation block
    char* alloc_ptr_ = nullptr;
    size_t alloc_bytes_remaining_ = 0;

    // Array of allocated memory blocks
    std::vector<std::unique_ptr<char[]>> blocks_;

    // Block size
    size_t block_size_;

    // Total memory allocated
    std::atomic<size_t> memory_usage_{0};
};

inline Arena::Arena(size_t block_size) : block_size_(block_size) {
    assert(block_size > 0);
}

inline char* Arena::Allocate(size_t bytes) {
    assert(bytes > 0);
    if (bytes <= alloc_bytes_remaining_) {
        char* result = alloc_ptr_;
        alloc_ptr_ += bytes;
        alloc_bytes_remaining_ -= bytes;
        return result;
    }
    return AllocateFallback(bytes);
}

inline char* Arena::AllocateAligned(size_t bytes, size_t alignment) {
    assert((alignment & (alignment - 1)) == 0);  // Alignment must be power of 2

    size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (alignment - 1);
    size_t slop = (current_mod == 0 ? 0 : alignment - current_mod);
    size_t needed = bytes + slop;

    char* result;
    if (needed <= alloc_bytes_remaining_) {
        result = alloc_ptr_ + slop;
        alloc_ptr_ += needed;
        alloc_bytes_remaining_ -= needed;
    } else {
        // AllocateFallback always returns aligned memory
        result = AllocateFallback(bytes);
    }
    assert((reinterpret_cast<uintptr_t>(result) & (alignment - 1)) == 0);
    return result;
}

inline char* Arena::AllocateFallback(size_t bytes) {
    if (bytes > block_size_ / 4) {
        // Object is more than a quarter of our block size. Allocate it separately
        // to avoid wasting too much space in leftover bytes.
        return AllocateNewBlock(bytes);
    }

    // We waste the remaining space in the current block.
    alloc_ptr_ = AllocateNewBlock(block_size_);
    alloc_bytes_remaining_ = block_size_;

    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
}

inline char* Arena::AllocateNewBlock(size_t block_bytes) {
    // Ensure alignment
    constexpr size_t alignment = alignof(std::max_align_t);
    size_t aligned_bytes = (block_bytes + alignment - 1) & ~(alignment - 1);
    
    auto block = std::make_unique<char[]>(aligned_bytes);
    char* result = block.get();
    blocks_.push_back(std::move(block));
    memory_usage_.fetch_add(aligned_bytes + sizeof(char*), std::memory_order_relaxed);
    return result;
}

inline void Arena::Reset() {
    blocks_.clear();
    alloc_ptr_ = nullptr;
    alloc_bytes_remaining_ = 0;
    memory_usage_.store(0, std::memory_order_relaxed);
}

// Concurrent Arena with better thread-safety
class ConcurrentArena {
public:
    explicit ConcurrentArena(size_t block_size = Arena::kDefaultBlockSize);
    ~ConcurrentArena() = default;

    // Thread-safe allocation
    char* Allocate(size_t bytes);
    char* AllocateAligned(size_t bytes, size_t alignment = alignof(std::max_align_t));

    size_t MemoryUsage() const {
        return memory_usage_.load(std::memory_order_relaxed);
    }

private:
    std::atomic<char*> alloc_ptr_{nullptr};
    std::atomic<size_t> alloc_bytes_remaining_{0};
    std::vector<std::unique_ptr<char[]>> blocks_;
    size_t block_size_;
    std::atomic<size_t> memory_usage_{0};
    mutable std::mutex mutex_;

    char* AllocateFallback(size_t bytes);
};

}  // namespace tskv
