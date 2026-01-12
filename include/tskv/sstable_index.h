// Copyright (c) 2026 TSKV Authors. All rights reserved.
// Global SSTable Index using Skip List
// This index maintains key ranges for all SSTables and enables fast lookup

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <vector>
#include <algorithm>

#include "tskv/arena.h"
#include "tskv/slice.h"
#include "tskv/sstable.h"

namespace tskv {

// Entry in the SSTable index
struct SSTableIndexEntry {
    std::string smallest_key;
    std::string largest_key;
    std::shared_ptr<SSTableReader> reader;
    uint64_t file_number;
    uint64_t creation_time;  // For ordering when keys overlap

    SSTableIndexEntry() = default;
    SSTableIndexEntry(const std::string& min_key, const std::string& max_key,
                      std::shared_ptr<SSTableReader> r, uint64_t num, uint64_t time)
        : smallest_key(min_key), largest_key(max_key), 
          reader(std::move(r)), file_number(num), creation_time(time) {}

    bool MayContain(const Slice& key) const {
        return key >= Slice(smallest_key) && key <= Slice(largest_key);
    }

    // For ordering: newer files (higher creation_time) come first
    bool operator<(const SSTableIndexEntry& other) const {
        // First compare by smallest_key for spatial ordering
        int cmp = Slice(smallest_key).compare(Slice(other.smallest_key));
        if (cmp != 0) return cmp < 0;
        // If same smallest_key, newer files come first (higher creation_time)
        return creation_time > other.creation_time;
    }
};

// Lock-free Skip List for SSTable index
// Optimized for read-heavy workloads with occasional inserts
template <int MaxHeight = 16, int Branching = 4>
class SSTableIndex {
public:
    SSTableIndex() : head_(NewNode(MaxHeight)), max_height_(1) {}
    
    ~SSTableIndex() {
        // Clean up nodes
        Node* current = head_;
        while (current != nullptr) {
            Node* next = current->next[0].load(std::memory_order_relaxed);
            delete current;
            current = next;
        }
    }

    // Add a new SSTable to the index
    void Add(std::shared_ptr<SSTableReader> reader, uint64_t file_number, uint64_t creation_time) {
        auto entry = std::make_shared<SSTableIndexEntry>(
            reader->SmallestKey(),
            reader->LargestKey(),
            reader,
            file_number,
            creation_time
        );
        Insert(entry);
    }

    // Remove an SSTable from the index
    void Remove(uint64_t file_number) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        
        Node* current = head_->next[0].load(std::memory_order_acquire);
        Node* prev = head_;
        
        while (current != nullptr) {
            if (current->entry && current->entry->file_number == file_number) {
                // Found it, remove from all levels
                for (int i = 0; i < MaxHeight; i++) {
                    Node* p = head_;
                    while (p->next[i].load(std::memory_order_relaxed) != nullptr) {
                        if (p->next[i].load(std::memory_order_relaxed) == current) {
                            p->next[i].store(current->next[i].load(std::memory_order_relaxed),
                                           std::memory_order_release);
                            break;
                        }
                        p = p->next[i].load(std::memory_order_relaxed);
                    }
                }
                count_.fetch_sub(1, std::memory_order_relaxed);
                // Note: We don't delete the node immediately for lock-free reads
                // In production, use epoch-based reclamation
                return;
            }
            current = current->next[0].load(std::memory_order_acquire);
        }
    }

    // Find all SSTables that may contain the given key
    // Returns them in order from newest to oldest
    std::vector<std::shared_ptr<SSTableReader>> FindCandidates(const Slice& key) const {
        std::vector<std::shared_ptr<SSTableReader>> candidates;
        std::shared_lock<std::shared_mutex> lock(mutex_);

        // Use skip list to find potential starting point, then scan
        // For monotonic keys, we use binary search-like traversal
        Node* x = head_;
        int level = max_height_.load(std::memory_order_relaxed) - 1;
        
        // Find the first SSTable whose largest_key >= key
        while (level >= 0) {
            Node* next = x->next[level].load(std::memory_order_acquire);
            // Skip nodes where largest_key < key
            while (next != nullptr && next->entry && 
                   Slice(next->entry->largest_key) < key) {
                x = next;
                next = x->next[level].load(std::memory_order_acquire);
            }
            level--;
        }
        
        // Now scan from this position to find all candidates
        Node* current = x->next[0].load(std::memory_order_acquire);
        while (current != nullptr && current->entry) {
            // For monotonic keys, once smallest_key > key, we can stop
            if (Slice(current->entry->smallest_key) > key) {
                break;
            }
            if (current->entry->MayContain(key)) {
                candidates.push_back(current->entry->reader);
            }
            current = current->next[0].load(std::memory_order_acquire);
        }

        // Sort by creation time (newest first) for correct MVCC ordering
        std::sort(candidates.begin(), candidates.end(),
            [](const std::shared_ptr<SSTableReader>& a, 
               const std::shared_ptr<SSTableReader>& b) {
                return a->FileNumber() > b->FileNumber();
            });

        return candidates;
    }

    // Find the SSTable containing the key
    // For monotonic data (timestamps), key ranges don't overlap
    // Use skip list binary search for O(log n) lookup
    std::shared_ptr<SSTableReader> FindOne(const Slice& key) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);

        // Binary search using skip list levels
        Node* x = head_;
        int level = max_height_.load(std::memory_order_relaxed) - 1;
        
        while (level >= 0) {
            Node* next = x->next[level].load(std::memory_order_acquire);
            // Skip nodes where largest_key < key (key cannot be in those SSTables)
            while (next != nullptr && next->entry && 
                   Slice(next->entry->largest_key) < key) {
                x = next;
                next = x->next[level].load(std::memory_order_acquire);
            }
            level--;
        }
        
        // Check the node we landed on
        Node* current = x->next[0].load(std::memory_order_acquire);
        if (current != nullptr && current->entry && 
            current->entry->MayContain(key)) {
            return current->entry->reader;
        }

        return nullptr;
    }

    // Get all SSTables in order
    std::vector<std::shared_ptr<SSTableReader>> GetAll() const {
        std::vector<std::shared_ptr<SSTableReader>> result;
        std::shared_lock<std::shared_mutex> lock(mutex_);

        Node* current = head_->next[0].load(std::memory_order_acquire);
        while (current != nullptr && current->entry) {
            result.push_back(current->entry->reader);
            current = current->next[0].load(std::memory_order_acquire);
        }
        return result;
    }

    // Get count of SSTables
    size_t Count() const { return count_.load(std::memory_order_relaxed); }

    // Get total size of all SSTables
    uint64_t TotalSize() const {
        uint64_t total = 0;
        std::shared_lock<std::shared_mutex> lock(mutex_);
        
        Node* current = head_->next[0].load(std::memory_order_acquire);
        while (current != nullptr && current->entry) {
            total += current->entry->reader->FileSize();
            current = current->next[0].load(std::memory_order_acquire);
        }
        return total;
    }

private:
    struct Node {
        std::shared_ptr<SSTableIndexEntry> entry;
        std::atomic<Node*> next[MaxHeight];

        explicit Node(int height) : entry(nullptr) {
            for (int i = 0; i < height; i++) {
                next[i].store(nullptr, std::memory_order_relaxed);
            }
        }

        Node(std::shared_ptr<SSTableIndexEntry> e, int height) : entry(std::move(e)) {
            for (int i = 0; i < height; i++) {
                next[i].store(nullptr, std::memory_order_relaxed);
            }
        }
    };

    static Node* NewNode(int height) {
        return new Node(height);
    }

    static Node* NewNode(std::shared_ptr<SSTableIndexEntry> entry, int height) {
        return new Node(std::move(entry), height);
    }

    int RandomHeight() {
        static thread_local std::mt19937 rng(std::random_device{}());
        int height = 1;
        while (height < MaxHeight && (rng() % Branching) == 0) {
            height++;
        }
        return height;
    }

    void Insert(std::shared_ptr<SSTableIndexEntry> entry) {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        Node* prev[MaxHeight];
        Node* current = head_;

        // Find insert position
        for (int i = max_height_.load(std::memory_order_relaxed) - 1; i >= 0; i--) {
            while (true) {
                Node* next = current->next[i].load(std::memory_order_relaxed);
                if (next == nullptr || !next->entry || *entry < *next->entry) {
                    break;
                }
                current = next;
            }
            prev[i] = current;
        }

        int height = RandomHeight();
        if (height > max_height_.load(std::memory_order_relaxed)) {
            for (int i = max_height_.load(std::memory_order_relaxed); i < height; i++) {
                prev[i] = head_;
            }
            max_height_.store(height, std::memory_order_relaxed);
        }

        Node* node = NewNode(std::move(entry), height);
        for (int i = 0; i < height; i++) {
            node->next[i].store(prev[i]->next[i].load(std::memory_order_relaxed),
                               std::memory_order_relaxed);
            prev[i]->next[i].store(node, std::memory_order_release);
        }
        count_.fetch_add(1, std::memory_order_relaxed);
    }

    mutable std::shared_mutex mutex_;
    Node* head_;
    std::atomic<int> max_height_;
    std::atomic<size_t> count_{0};
};

// Partitioned SSTable Index for better scalability
// Each partition handles a range of keys
class PartitionedSSTableIndex {
public:
    explicit PartitionedSSTableIndex(size_t num_partitions = 16)
        : partitions_(num_partitions) {}

    void Add(std::shared_ptr<SSTableReader> reader, uint64_t file_number, uint64_t creation_time) {
        // Add to all partitions that might contain keys in this SSTable's range
        // For simplicity, we add to the partition based on the smallest key
        size_t partition = GetPartition(Slice(reader->SmallestKey()));
        partitions_[partition].Add(std::move(reader), file_number, creation_time);
    }

    std::shared_ptr<SSTableReader> FindOne(const Slice& key) const {
        size_t partition = GetPartition(key);
        return partitions_[partition].FindOne(key);
    }

    std::vector<std::shared_ptr<SSTableReader>> FindCandidates(const Slice& key) const {
        size_t partition = GetPartition(key);
        return partitions_[partition].FindCandidates(key);
    }

    size_t Count() const {
        size_t total = 0;
        for (const auto& p : partitions_) {
            total += p.Count();
        }
        return total;
    }

private:
    size_t GetPartition(const Slice& key) const {
        if (key.empty()) return 0;
        // Simple hash-based partitioning
        size_t hash = 0;
        for (size_t i = 0; i < key.size(); i++) {
            hash = hash * 31 + static_cast<unsigned char>(key[i]);
        }
        return hash % partitions_.size();
    }

    std::vector<SSTableIndex<>> partitions_;
};

}  // namespace tskv
