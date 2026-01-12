// Copyright (c) 2026 TSKV Authors. All rights reserved.
// High-performance lock-free SkipList implementation
// Optimized for time-series data with sequential insertion

#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <random>
#include <thread>

#include "tskv/arena.h"
#include "tskv/slice.h"

namespace tskv {

// Comparator interface for key comparison
struct BytewiseComparator {
    int operator()(const char* a, const char* b) const {
        Slice sa = GetLengthPrefixedSlice(a);
        Slice sb = GetLengthPrefixedSlice(b);
        return sa.compare(sb);
    }

    static Slice GetLengthPrefixedSlice(const char* data) {
        uint32_t len;
        const char* p = GetVarint32Ptr(data, data + 5, &len);
        return Slice(p, len);
    }

    static const char* GetVarint32Ptr(const char* p, const char* limit, uint32_t* v) {
        if (p < limit) {
            uint32_t result = *(reinterpret_cast<const unsigned char*>(p));
            if ((result & 128) == 0) {
                *v = result;
                return p + 1;
            }
        }
        return GetVarint32PtrFallback(p, limit, v);
    }

    static const char* GetVarint32PtrFallback(const char* p, const char* limit, uint32_t* v) {
        uint32_t result = 0;
        for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
            uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
            p++;
            if (byte & 128) {
                result |= ((byte & 127) << shift);
            } else {
                result |= (byte << shift);
                *v = result;
                return p;
            }
        }
        return nullptr;
    }
};

// Thread-local random number generator for height selection
class Random {
public:
    static uint32_t Next() {
        thread_local std::mt19937 rng(std::random_device{}());
        return rng();
    }

    static constexpr uint32_t kMaxNext = std::mt19937::max();
};

// Lock-free SkipList optimized for time-series data
template <class Comparator = BytewiseComparator>
class SkipList {
private:
    struct Node;

public:
    static constexpr int kMaxHeight = 12;
    static constexpr int kBranching = 4;

    explicit SkipList(Comparator cmp, Arena* arena);

    // Disallow copy
    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;

    // Insert key into the list
    // REQUIRES: nothing that compares equal to key is currently in the list.
    void Insert(const char* key);

    // Insert with hint for sequential insertion (optimized path)
    void InsertWithHint(const char* key, Node** hint);

    // Returns true if an entry that compares equal to key is in the list
    bool Contains(const char* key) const;

    // Allocate memory for a key in the arena
    char* AllocateKey(size_t key_size);

    // Iteration over the contents of a skip list
    class Iterator {
    public:
        explicit Iterator(const SkipList* list);

        // Returns true if the iterator is positioned at a valid node
        bool Valid() const { return node_ != nullptr; }

        // Returns the key at the current position
        const char* key() const {
            assert(Valid());
            return node_->key;
        }

        // Advances to the next position
        void Next() {
            assert(Valid());
            node_ = node_->Next(0);
        }

        // Advances to the previous position
        void Prev();

        // Advance to the first entry with a key >= target
        void Seek(const char* target);

        // Retreat to the last entry with a key <= target
        void SeekForPrev(const char* target);

        // Position at the first entry in list
        void SeekToFirst() {
            node_ = list_->head_->Next(0);
        }

        // Position at the last entry in list
        void SeekToLast();

    private:
        const SkipList* list_;
        Node* node_;
    };

    // Return the number of entries in the skiplist
    size_t Count() const { return count_.load(std::memory_order_relaxed); }

    // Get the first key
    const char* GetFirstKey() const {
        Node* x = head_->Next(0);
        return x ? x->key : nullptr;
    }

    // Get the last key
    const char* GetLastKey() const {
        Node* x = FindLast();
        return (x != head_) ? x->key : nullptr;
    }

private:
    struct Node {
        explicit Node(const char* k) : key(k) {}

        const char* const key;

        Node* Next(int n) {
            assert(n >= 0);
            return next_[n].load(std::memory_order_acquire);
        }

        void SetNext(int n, Node* x) {
            assert(n >= 0);
            next_[n].store(x, std::memory_order_release);
        }

        Node* NoBarrier_Next(int n) {
            assert(n >= 0);
            return next_[n].load(std::memory_order_relaxed);
        }

        void NoBarrier_SetNext(int n, Node* x) {
            assert(n >= 0);
            next_[n].store(x, std::memory_order_relaxed);
        }

    private:
        // Array of length equal to the node height. next_[0] is lowest level.
        std::atomic<Node*> next_[1];
    };

    Node* NewNode(const char* key, int height);
    int RandomHeight();

    bool Equal(const char* a, const char* b) const { return compare_(a, b) == 0; }
    bool LessThan(const char* a, const char* b) const { return compare_(a, b) < 0; }

    // Return true if key is greater than the data stored in "n"
    bool KeyIsAfterNode(const char* key, Node* n) const;

    // Returns the earliest node with a key >= key
    Node* FindGreaterOrEqual(const char* key, Node** prev) const;

    // Return the latest node with a key < key
    Node* FindLessThan(const char* key) const;

    // Return the last node in the list
    Node* FindLast() const;

    Comparator const compare_;
    Arena* const arena_;

    Node* const head_;

    // Current height of the skiplist
    std::atomic<int> max_height_;

    // Number of entries
    std::atomic<size_t> count_{0};

    // For optimizing sequential inserts
    Node* prev_[kMaxHeight];
    int prev_height_;
};

// Implementation

template <class Comparator>
typename SkipList<Comparator>::Node*
SkipList<Comparator>::NewNode(const char* key, int height) {
    size_t alloc_size = sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1);
    char* mem = arena_->AllocateAligned(alloc_size);
    return new (mem) Node(key);
}

template <class Comparator>
inline SkipList<Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(nullptr, kMaxHeight)),
      max_height_(1),
      prev_height_(1) {
    for (int i = 0; i < kMaxHeight; i++) {
        head_->SetNext(i, nullptr);
        prev_[i] = head_;
    }
}

template <class Comparator>
int SkipList<Comparator>::RandomHeight() {
    int height = 1;
    // Use 64-bit arithmetic to avoid overflow
    // kScaledInverseBranching = kMaxNext / kBranching (approximately 1/kBranching probability)
    constexpr uint64_t kScaledInverseBranching = static_cast<uint64_t>(Random::kMaxNext) / kBranching;
    while (height < kMaxHeight && Random::Next() < kScaledInverseBranching) {
        height++;
    }
    return height;
}

template <class Comparator>
bool SkipList<Comparator>::KeyIsAfterNode(const char* key, Node* n) const {
    return (n != nullptr) && (compare_(n->key, key) < 0);
}

template <class Comparator>
typename SkipList<Comparator>::Node*
SkipList<Comparator>::FindGreaterOrEqual(const char* key, Node** prev) const {
    Node* x = head_;
    int level = max_height_.load(std::memory_order_relaxed) - 1;
    while (true) {
        Node* next = x->Next(level);
        if (KeyIsAfterNode(key, next)) {
            // Keep searching in this list
            x = next;
        } else {
            if (prev != nullptr) prev[level] = x;
            if (level == 0) {
                return next;
            } else {
                // Switch to next list
                level--;
            }
        }
    }
}

template <class Comparator>
typename SkipList<Comparator>::Node*
SkipList<Comparator>::FindLessThan(const char* key) const {
    Node* x = head_;
    int level = max_height_.load(std::memory_order_relaxed) - 1;
    while (true) {
        Node* next = x->Next(level);
        if (next == nullptr || !LessThan(next->key, key)) {
            if (level == 0) {
                return x;
            } else {
                level--;
            }
        } else {
            x = next;
        }
    }
}

template <class Comparator>
typename SkipList<Comparator>::Node* SkipList<Comparator>::FindLast() const {
    Node* x = head_;
    int level = max_height_.load(std::memory_order_relaxed) - 1;
    while (true) {
        Node* next = x->Next(level);
        if (next == nullptr) {
            if (level == 0) {
                return x;
            } else {
                level--;
            }
        } else {
            x = next;
        }
    }
}

template <class Comparator>
void SkipList<Comparator>::Insert(const char* key) {
    // Find insert position
    FindGreaterOrEqual(key, prev_);

    int height = RandomHeight();
    if (height > max_height_.load(std::memory_order_relaxed)) {
        for (int i = max_height_.load(std::memory_order_relaxed); i < height; i++) {
            prev_[i] = head_;
        }
        max_height_.store(height, std::memory_order_relaxed);
    }

    Node* x = NewNode(key, height);
    for (int i = 0; i < height; i++) {
        x->NoBarrier_SetNext(i, prev_[i]->NoBarrier_Next(i));
        prev_[i]->SetNext(i, x);
    }
    prev_[0] = x;
    prev_height_ = height;
    count_.fetch_add(1, std::memory_order_relaxed);
}

template <class Comparator>
void SkipList<Comparator>::InsertWithHint(const char* key, Node** hint) {
    // This is an optimized path when we know the key will be inserted
    // near the hint position (useful for sequential time-series data)
    if (*hint == nullptr) {
        *hint = head_;
    }

    Node* x = *hint;
    if (x != head_ && !KeyIsAfterNode(key, x)) {
        // Key might be before hint, need full search
        FindGreaterOrEqual(key, prev_);
    } else {
        // Start from hint
        int level = max_height_.load(std::memory_order_relaxed) - 1;
        while (true) {
            Node* next = x->Next(level);
            if (KeyIsAfterNode(key, next)) {
                x = next;
            } else {
                prev_[level] = x;
                if (level == 0) {
                    break;
                } else {
                    level--;
                }
            }
        }
    }

    int height = RandomHeight();
    if (height > max_height_.load(std::memory_order_relaxed)) {
        for (int i = max_height_.load(std::memory_order_relaxed); i < height; i++) {
            prev_[i] = head_;
        }
        max_height_.store(height, std::memory_order_relaxed);
    }

    Node* node = NewNode(key, height);
    for (int i = 0; i < height; i++) {
        node->NoBarrier_SetNext(i, prev_[i]->NoBarrier_Next(i));
        prev_[i]->SetNext(i, node);
    }
    *hint = node;
    count_.fetch_add(1, std::memory_order_relaxed);
}

template <class Comparator>
bool SkipList<Comparator>::Contains(const char* key) const {
    Node* x = FindGreaterOrEqual(key, nullptr);
    if (x != nullptr && Equal(key, x->key)) {
        return true;
    }
    return false;
}

template <class Comparator>
char* SkipList<Comparator>::AllocateKey(size_t key_size) {
    return arena_->Allocate(key_size);
}

// Iterator implementation

template <class Comparator>
inline SkipList<Comparator>::Iterator::Iterator(const SkipList* list)
    : list_(list), node_(nullptr) {}

template <class Comparator>
void SkipList<Comparator>::Iterator::Seek(const char* target) {
    node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <class Comparator>
void SkipList<Comparator>::Iterator::SeekForPrev(const char* target) {
    Seek(target);
    if (!Valid()) {
        SeekToLast();
    }
    while (Valid() && list_->LessThan(target, key())) {
        Prev();
    }
}

template <class Comparator>
void SkipList<Comparator>::Iterator::Prev() {
    assert(Valid());
    node_ = list_->FindLessThan(node_->key);
    if (node_ == list_->head_) {
        node_ = nullptr;
    }
}

template <class Comparator>
void SkipList<Comparator>::Iterator::SeekToLast() {
    node_ = list_->FindLast();
    if (node_ == list_->head_) {
        node_ = nullptr;
    }
}

}  // namespace tskv
