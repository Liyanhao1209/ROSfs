// Copyright (c) 2026 TSKV Authors. All rights reserved.
// TSKV - Time-Series Key-Value Storage Engine
// Main database interface

#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <queue>
#include <sys/stat.h>
#include <dirent.h>

#include "tskv/memtable.h"
#include "tskv/options.h"
#include "tskv/slice.h"
#include "tskv/sstable.h"
#include "tskv/sstable_index.h"
#include "tskv/status.h"
#include "tskv/wal.h"

namespace tskv {

// WriteBatch for atomic multi-key writes
class WriteBatch {
public:
    WriteBatch() = default;

    void Put(const Slice& key, const Slice& value) {
        PutVarint32(&rep_, static_cast<uint32_t>(kTypeValue));
        PutLengthPrefixedSlice(&rep_, key.data(), key.size());
        PutLengthPrefixedSlice(&rep_, value.data(), value.size());
        count_++;
    }

    void Delete(const Slice& key) {
        PutVarint32(&rep_, static_cast<uint32_t>(kTypeDeletion));
        PutLengthPrefixedSlice(&rep_, key.data(), key.size());
        count_++;
    }

    void Clear() {
        rep_.clear();
        count_ = 0;
    }

    size_t Count() const { return count_; }
    const std::string& Data() const { return rep_; }

    // Iterator for applying batch to memtable
    class Iterator {
    public:
        explicit Iterator(const WriteBatch* batch) 
            : data_(batch->rep_.data()), end_(data_ + batch->rep_.size()) {}

        bool Valid() const { return data_ < end_; }

        void Next() {
            if (!Valid()) return;
            uint32_t type;
            data_ = GetVarint32Ptr(data_, end_, &type);
            uint32_t key_len;
            data_ = GetVarint32Ptr(data_, end_, &key_len);
            data_ += key_len;
            if (type == kTypeValue) {
                uint32_t val_len;
                data_ = GetVarint32Ptr(data_, end_, &val_len);
                data_ += val_len;
            }
        }

        ValueType Type() const {
            uint32_t type;
            GetVarint32Ptr(data_, end_, &type);
            return static_cast<ValueType>(type);
        }

        Slice Key() const {
            uint32_t type;
            const char* p = GetVarint32Ptr(data_, end_, &type);
            uint32_t key_len;
            p = GetVarint32Ptr(p, end_, &key_len);
            return Slice(p, key_len);
        }

        Slice Value() const {
            uint32_t type;
            const char* p = GetVarint32Ptr(data_, end_, &type);
            uint32_t key_len;
            p = GetVarint32Ptr(p, end_, &key_len);
            p += key_len;
            uint32_t val_len;
            p = GetVarint32Ptr(p, end_, &val_len);
            return Slice(p, val_len);
        }

    private:
        const char* data_;
        const char* end_;
    };

private:
    std::string rep_;
    size_t count_ = 0;
};

// Database statistics
struct DBStats {
    std::atomic<uint64_t> bytes_written{0};
    std::atomic<uint64_t> bytes_read{0};
    std::atomic<uint64_t> keys_written{0};
    std::atomic<uint64_t> keys_read{0};
    std::atomic<uint64_t> memtable_hits{0};
    std::atomic<uint64_t> sstable_hits{0};
    std::atomic<uint64_t> cache_misses{0};
    std::atomic<uint64_t> flushes{0};
};

// Main Database class
class DB {
public:
    // Open or create a database
    static Status Open(const Options& options, const std::string& dbname,
                       std::unique_ptr<DB>* dbptr);

    ~DB();

    // Disallow copy
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    // Key-Value operations

    // Store the mapping "key->value" in the database
    Status Put(const WriteOptions& options, const Slice& key, const Slice& value);

    // Remove the database entry (if any) for "key"
    Status Delete(const WriteOptions& options, const Slice& key);

    // Apply the specified updates to the database
    Status Write(const WriteOptions& options, WriteBatch* batch);

    // If the database contains an entry for "key" store the value in *value
    Status Get(const ReadOptions& options, const Slice& key, std::string* value);

    // Database management

    // Force a memtable flush to disk
    Status Flush(const FlushOptions& options);

    // Get a snapshot of the current database state
    uint64_t GetSnapshot();

    // Release a previously acquired snapshot
    void ReleaseSnapshot(uint64_t snapshot);

    // Get database statistics
    const DBStats& GetStats() const { return stats_; }

    // Get the database path
    const std::string& GetName() const { return dbname_; }

    // Get approximate size of key range
    uint64_t GetApproximateSizes(const Slice& start, const Slice& end);

    // Iterator for database
    class Iterator {
    public:
        virtual ~Iterator() = default;
        virtual bool Valid() const = 0;
        virtual void SeekToFirst() = 0;
        virtual void SeekToLast() = 0;
        virtual void Seek(const Slice& target) = 0;
        virtual void Next() = 0;
        virtual void Prev() = 0;
        virtual Slice key() const = 0;
        virtual Slice value() const = 0;
        virtual Status status() const = 0;
    };

    // Create an iterator over the database
    std::unique_ptr<Iterator> NewIterator(const ReadOptions& options);

private:
    friend class DBIteratorImpl;

    DB(const Options& options, const std::string& dbname);

    Status Recover();
    Status RecoverWAL(const std::string& wal_file);
    
    void MaybeScheduleFlush();
    void BackgroundFlush();
    Status FlushMemTable(MemTable* mem);
    Status WriteToWAL(const WriteBatch& batch);

    std::string MakeSSTableFileName(uint64_t file_number);
    std::string MakeWALFileName(uint64_t file_number);
    std::string MakeCurrentFileName();
    std::string MakeManifestFileName(uint64_t file_number);

    Status SaveManifest();
    Status LoadManifest();

    // Configuration
    Options options_;
    std::string dbname_;

    // Synchronization
    mutable std::mutex mutex_;
    std::condition_variable background_cv_;
    std::condition_variable flush_cv_;
    bool background_flush_scheduled_ = false;
    bool shutting_down_ = false;

    // Background thread
    std::thread background_thread_;

    // MemTables
    std::unique_ptr<MemTable> mem_;
    std::deque<std::unique_ptr<MemTable>> imm_;  // Immutable memtables waiting for flush

    // SSTable index (the skip list index for locating SSTables)
    SSTableIndex<> sstable_index_;

    // WAL
    std::unique_ptr<WALWriter> wal_;
    uint64_t wal_file_number_ = 0;

    // Sequence numbers
    std::atomic<uint64_t> sequence_{0};
    uint64_t next_file_number_ = 1;

    // Statistics
    DBStats stats_;
};

// Implementation

inline DB::DB(const Options& options, const std::string& dbname)
    : options_(options), dbname_(dbname) {}

inline DB::~DB() {
    {
        std::unique_lock<std::mutex> lock(mutex_);
        shutting_down_ = true;
        background_cv_.notify_all();
    }

    if (background_thread_.joinable()) {
        background_thread_.join();
    }

    // Flush remaining data
    if (mem_ && mem_->Count() > 0) {
        FlushMemTable(mem_.get());
    }
}

inline Status DB::Open(const Options& options, const std::string& dbname,
                        std::unique_ptr<DB>* dbptr) {
    auto db = std::unique_ptr<DB>(new DB(options, dbname));

    // Create directory if needed
    mkdir(dbname.c_str(), 0755);

    // Recover existing data
    Status s = db->Recover();
    if (!s.ok()) {
        return s;
    }

    // Create new memtable
    db->mem_ = std::make_unique<MemTable>(options.write_buffer_size);
    db->mem_->Ref();

    // Create WAL
    if (options.enable_wal) {
        db->wal_file_number_ = db->next_file_number_++;
        db->wal_ = std::make_unique<WALWriter>(
            db->MakeWALFileName(db->wal_file_number_),
            options.sync_wal
        );
    }

    // Start background thread
    db->background_thread_ = std::thread([&db = *db]() {
        db.BackgroundFlush();
    });

    *dbptr = std::move(db);
    return Status::OK();
}

inline Status DB::Recover() {
    // Load manifest
    Status s = LoadManifest();
    if (s.IsNotFound()) {
        // New database
        return Status::OK();
    }
    if (!s.ok()) {
        return s;
    }

    // Recover WAL files
    DIR* dir = opendir(dbname_.c_str());
    if (dir == nullptr) {
        return Status::OK();
    }

    std::vector<std::string> wal_files;
    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        std::string name = entry->d_name;
        if (name.size() > 4 && name.substr(name.size() - 4) == ".wal") {
            wal_files.push_back(dbname_ + "/" + name);
        }
    }
    closedir(dir);

    // Sort and recover WAL files
    std::sort(wal_files.begin(), wal_files.end());
    for (const auto& wal_file : wal_files) {
        s = RecoverWAL(wal_file);
        if (!s.ok()) {
            return s;
        }
    }

    return Status::OK();
}

inline Status DB::RecoverWAL(const std::string& wal_file) {
    WALReader reader(wal_file);
    std::string record;

    while (reader.HasMore()) {
        Status s = reader.ReadRecord(&record);
        if (s.IsNotFound()) {
            break;  // End of file
        }
        if (!s.ok()) {
            return s;
        }

        // Apply record to memtable
        WriteBatch batch;
        // Parse record (it's a serialized WriteBatch)
        const char* p = record.data();
        const char* end = p + record.size();

        while (p < end) {
            uint32_t type;
            p = GetVarint32Ptr(p, end, &type);
            if (p == nullptr) break;

            uint32_t key_len;
            p = GetVarint32Ptr(p, end, &key_len);
            if (p == nullptr) break;
            Slice key(p, key_len);
            p += key_len;

            if (type == kTypeValue) {
                uint32_t val_len;
                p = GetVarint32Ptr(p, end, &val_len);
                if (p == nullptr) break;
                Slice value(p, val_len);
                p += val_len;
                batch.Put(key, value);
            } else {
                batch.Delete(key);
            }
        }

        // Apply batch
        if (mem_ == nullptr) {
            mem_ = std::make_unique<MemTable>(options_.write_buffer_size);
            mem_->Ref();
        }

        WriteBatch::Iterator iter(&batch);
        while (iter.Valid()) {
            uint64_t seq = sequence_.fetch_add(1, std::memory_order_relaxed);
            if (iter.Type() == kTypeValue) {
                mem_->Add(seq, kTypeValue, iter.Key(), iter.Value());
            } else {
                mem_->Add(seq, kTypeDeletion, iter.Key(), Slice());
            }
            iter.Next();
        }
    }

    return Status::OK();
}

inline Status DB::Put(const WriteOptions& options, const Slice& key, const Slice& value) {
    WriteBatch batch;
    batch.Put(key, value);
    return Write(options, &batch);
}

inline Status DB::Delete(const WriteOptions& options, const Slice& key) {
    WriteBatch batch;
    batch.Delete(key);
    return Write(options, &batch);
}

inline Status DB::Write(const WriteOptions& options, WriteBatch* batch) {
    // Acquire lock for memtable operations
    std::unique_lock<std::mutex> lock(mutex_);

    // Wait if too many immutable memtables
    while (imm_.size() >= static_cast<size_t>(options_.max_write_buffer_number)) {
        flush_cv_.wait(lock);
    }

    // Write to WAL FIRST (before memtable) to ensure durability
    // This must happen before memtable update to guarantee data can be recovered
    bool needs_wal = !options.disable_wal && options_.enable_wal && wal_ != nullptr;
    if (needs_wal) {
        Status s = wal_->AddRecord(Slice(batch->Data()));
        if (!s.ok()) {
            return s;
        }
        if (options.sync) {
            s = wal_->Sync();
            if (!s.ok()) {
                return s;
            }
        }
    }

    // Now apply to memtable (safe - WAL has the data)
    WriteBatch::Iterator iter(batch);
    while (iter.Valid()) {
        uint64_t seq = sequence_.fetch_add(1, std::memory_order_relaxed);
        if (iter.Type() == kTypeValue) {
            mem_->Add(seq, kTypeValue, iter.Key(), iter.Value());
            stats_.bytes_written.fetch_add(iter.Key().size() + iter.Value().size(),
                                           std::memory_order_relaxed);
        } else {
            mem_->Add(seq, kTypeDeletion, iter.Key(), Slice());
            stats_.bytes_written.fetch_add(iter.Key().size(), std::memory_order_relaxed);
        }
        stats_.keys_written.fetch_add(1, std::memory_order_relaxed);
        iter.Next();
    }

    // Check if memtable needs to be flushed
    if (mem_->ShouldFlush()) {
        MaybeScheduleFlush();
    }

    return Status::OK();
}

inline Status DB::WriteToWAL(const WriteBatch& batch) {
    if (wal_ == nullptr) {
        return Status::OK();
    }
    return wal_->AddRecord(Slice(batch.Data()));
}

inline Status DB::Get(const ReadOptions& options, const Slice& key, std::string* value) {
    // Optimistic locking for MemTable access? 
    // For now, we use a shared mutex pattern manually or just optimize the critical section.
    // TSKV's problem: The `mutex_` is protecting BOTH MemTables AND SSTable Index management.
    // But reading MemTable is thread-safe if MemTable itself is concurrent (it's a skip list).
    // Let's shorten the lock hold time.

    // Snapshot state references under lock
    MemTable* mem = nullptr;
    std::vector<MemTable*> imm;
    
    {
        std::unique_lock<std::mutex> lock(mutex_);
        mem = mem_.get();
        mem->Ref(); // Prevent deletion
        for (const auto& m : imm_) {
            m->Ref();
            imm.push_back(m.get());
        }
    } // UNLOCK immediately

    // Search MemTable (lock-free / fine-grained internal locks)
    Status s;
    bool found = false;
    
    if (mem->Get(key, value, &s)) {
        found = true;
    } else {
        // Search Imm
        for (auto* m : imm) {
            if (m->Get(key, value, &s)) {
                found = true;
                break;
            }
        }
    }

    // Release references
    {
        std::unique_lock<std::mutex> lock(mutex_);
        mem->Unref();
        for (auto* m : imm) m->Unref();
    }
    
    if (found) {
        stats_.memtable_hits.fetch_add(1, std::memory_order_relaxed);
        stats_.keys_read.fetch_add(1, std::memory_order_relaxed);
        if (s.ok()) {
            stats_.bytes_read.fetch_add(value->size(), std::memory_order_relaxed);
        }
        return s;
    }

    if (options.read_memtable_only) {
        return Status::NotFound();
    }

    // SSTable Index is concurrent skip list, no need for DB lock here
    // Search SSTables...
    
    // Try fast path first with FindOne
    auto reader = sstable_index_.FindOne(key);
    if (reader) {
        s = reader->Get(key, value);
        if (s.ok()) {
            stats_.sstable_hits.fetch_add(1, std::memory_order_relaxed);
            stats_.bytes_read.fetch_add(value->size(), std::memory_order_relaxed);
            stats_.keys_read.fetch_add(1, std::memory_order_relaxed);
            return s;
        }
        // FindOne returned an SSTable but key wasn't found
        // Fall through to check all candidates (handles overlapping ranges)
    }

    // Search all candidates that may contain the key
    auto candidates = sstable_index_.FindCandidates(key);
    for (const auto& candidate : candidates) {
        // Skip the SSTable we already checked
        if (candidate.get() == reader.get()) continue;
        
        s = candidate->Get(key, value);
        if (s.ok()) {
            stats_.sstable_hits.fetch_add(1, std::memory_order_relaxed);
            stats_.bytes_read.fetch_add(value->size(), std::memory_order_relaxed);
            stats_.keys_read.fetch_add(1, std::memory_order_relaxed);
            return s;
        }
    }

    stats_.cache_misses.fetch_add(1, std::memory_order_relaxed);
    return Status::NotFound();
}

inline void DB::MaybeScheduleFlush() {
    // Always move full memtable to immutable list, even if flush is already scheduled
    if (mem_->ShouldFlush()) {
        mem_->Ref();
        imm_.push_back(std::move(mem_));
        
        // Create new memtable
        mem_ = std::make_unique<MemTable>(options_.write_buffer_size);
        mem_->Ref();

        // Create new WAL
        if (options_.enable_wal) {
            wal_->Close();
            wal_file_number_ = next_file_number_++;
            wal_ = std::make_unique<WALWriter>(
                MakeWALFileName(wal_file_number_),
                options_.sync_wal
            );
        }
        
        // Always schedule/wake flush thread when we have work
        if (!shutting_down_) {
            background_flush_scheduled_ = true;
            background_cv_.notify_one();
        }
    }
}

inline void DB::BackgroundFlush() {
    while (true) {
        std::unique_lock<std::mutex> lock(mutex_);
        
        // Wait for work or shutdown
        while (imm_.empty() && !shutting_down_) {
            background_flush_scheduled_ = false;
            background_cv_.wait(lock);
        }

        if (shutting_down_) {
            break;
        }

        background_flush_scheduled_ = true;

        // Process ALL immutable memtables
        while (!imm_.empty()) {
            auto& mem = imm_.front();
            lock.unlock();

            // Flush to SSTable
            Status s = FlushMemTable(mem.get());
            
            lock.lock();
            if (s.ok()) {
                imm_.front()->Unref();
                imm_.pop_front();
                stats_.flushes.fetch_add(1, std::memory_order_relaxed);
                // Notify waiters after each flush
                flush_cv_.notify_all();
            } else {
                // Error - stop processing
                break;
            }
        }
    }
}

inline Status DB::FlushMemTable(MemTable* mem) {
    uint64_t file_number = next_file_number_++;
    std::string filename = MakeSSTableFileName(file_number);

    SSTableBuilder builder(filename);

    // Iterate through memtable and write to SSTable
    std::unique_ptr<MemTable::Iterator> iter(mem->NewIterator());
    iter->SeekToFirst();

    while (iter->Valid()) {
        // For time-series mode, we only keep the latest version of each key
        Slice key = iter->key();
        Slice value = iter->value();
        
        if (iter->type() == kTypeValue) {
            // Create internal key format for SSTable
            std::string internal_key;
            internal_key.append(key.data(), key.size());
            builder.Add(Slice(internal_key), value);
        }
        // Skip deletions in time-series mode (keys are never truly deleted)
        
        iter->Next();
    }

    Status s = builder.Finish();
    if (!s.ok()) {
        return s;
    }

    // Open the new SSTable and add to index
    std::unique_ptr<SSTableReader> reader;
    s = SSTableReader::Open(filename, &reader);
    if (!s.ok()) {
        return s;
    }

    reader->SetFileNumber(file_number);
    
    // Add to the skip list index
    sstable_index_.Add(
        std::shared_ptr<SSTableReader>(reader.release()),
        file_number,
        sequence_.load(std::memory_order_relaxed)
    );

    // Save manifest
    s = SaveManifest();

    return s;
}

inline Status DB::Flush(const FlushOptions& options) {
    std::unique_lock<std::mutex> lock(mutex_);

    if (mem_->Count() == 0) {
        // Still need to wait for pending flushes
        if (options.wait) {
            while (!imm_.empty()) {
                flush_cv_.wait(lock);
            }
        }
        return Status::OK();
    }

    // Force move memtable to immutable list (regardless of size)
    mem_->Ref();
    imm_.push_back(std::move(mem_));
    mem_ = std::make_unique<MemTable>(options_.write_buffer_size);
    mem_->Ref();
    
    // Always schedule/wake flush thread
    background_flush_scheduled_ = true;
    background_cv_.notify_one();

    if (options.wait) {
        while (!imm_.empty()) {
            flush_cv_.wait(lock);
        }
    }

    return Status::OK();
}

inline uint64_t DB::GetSnapshot() {
    return sequence_.load(std::memory_order_relaxed);
}

inline void DB::ReleaseSnapshot(uint64_t /*snapshot*/) {
    // No-op for now (snapshots are implicit in time-series mode)
}

inline std::string DB::MakeSSTableFileName(uint64_t file_number) {
    char buf[256];
    snprintf(buf, sizeof(buf), "%s/%06lu.sst", dbname_.c_str(), file_number);
    return buf;
}

inline std::string DB::MakeWALFileName(uint64_t file_number) {
    char buf[256];
    snprintf(buf, sizeof(buf), "%s/%06lu.wal", dbname_.c_str(), file_number);
    return buf;
}

inline std::string DB::MakeCurrentFileName() {
    return dbname_ + "/CURRENT";
}

inline std::string DB::MakeManifestFileName(uint64_t file_number) {
    char buf[256];
    snprintf(buf, sizeof(buf), "%s/MANIFEST-%06lu", dbname_.c_str(), file_number);
    return buf;
}

inline Status DB::SaveManifest() {
    // Simple manifest format: list of SSTable files
    std::string manifest;
    
    // Write sequence number
    PutFixed64(&manifest, sequence_.load(std::memory_order_relaxed));
    
    // Write next file number
    PutFixed64(&manifest, next_file_number_);
    
    // Write SSTable count
    auto sstables = sstable_index_.GetAll();
    PutFixed32(&manifest, static_cast<uint32_t>(sstables.size()));
    
    // Write each SSTable's info
    for (const auto& sst : sstables) {
        PutFixed64(&manifest, sst->FileNumber());
        PutLengthPrefixedSlice(&manifest, sst->SmallestKey().data(), sst->SmallestKey().size());
        PutLengthPrefixedSlice(&manifest, sst->LargestKey().data(), sst->LargestKey().size());
    }

    // Write to manifest file
    std::string manifest_file = MakeManifestFileName(next_file_number_);
    std::ofstream file(manifest_file, std::ios::binary | std::ios::trunc);
    if (!file.is_open()) {
        return Status::IOError("Cannot open manifest file");
    }
    file.write(manifest.data(), manifest.size());
    file.close();

    // Update CURRENT file
    std::ofstream current(MakeCurrentFileName(), std::ios::trunc);
    current << manifest_file;
    current.close();

    return Status::OK();
}

inline Status DB::LoadManifest() {
    // Read CURRENT file
    std::ifstream current(MakeCurrentFileName());
    if (!current.is_open()) {
        return Status::NotFound();
    }
    
    std::string manifest_file;
    std::getline(current, manifest_file);
    current.close();

    // Read manifest file
    std::ifstream file(manifest_file, std::ios::binary);
    if (!file.is_open()) {
        return Status::NotFound();
    }

    std::string manifest((std::istreambuf_iterator<char>(file)),
                          std::istreambuf_iterator<char>());
    file.close();

    const char* p = manifest.data();
    const char* end = p + manifest.size();

    // Read sequence number
    if (end - p < 8) return Status::Corruption("Manifest too small");
    sequence_.store(DecodeFixed64(p), std::memory_order_relaxed);
    p += 8;

    // Read next file number
    if (end - p < 8) return Status::Corruption("Manifest too small");
    next_file_number_ = DecodeFixed64(p);
    p += 8;

    // Read SSTable count
    if (end - p < 4) return Status::Corruption("Manifest too small");
    uint32_t count = DecodeFixed32(p);
    p += 4;

    // Read each SSTable
    for (uint32_t i = 0; i < count; i++) {
        if (end - p < 8) return Status::Corruption("Manifest truncated");
        uint64_t file_number = DecodeFixed64(p);
        p += 8;

        // Skip smallest and largest keys (we'll read them from the SSTable)
        uint32_t len;
        p = GetVarint32Ptr(p, end, &len);
        if (p == nullptr) return Status::Corruption("Manifest corrupted");
        p += len;
        p = GetVarint32Ptr(p, end, &len);
        if (p == nullptr) return Status::Corruption("Manifest corrupted");
        p += len;

        // Open the SSTable
        std::string filename = MakeSSTableFileName(file_number);
        std::unique_ptr<SSTableReader> reader;
        Status s = SSTableReader::Open(filename, &reader);
        if (!s.ok()) {
            continue;  // Skip missing files
        }

        reader->SetFileNumber(file_number);
        sstable_index_.Add(
            std::shared_ptr<SSTableReader>(reader.release()),
            file_number,
            file_number  // Use file number as creation time
        );
    }

    return Status::OK();
}

// Database Iterator implementation
class DBIteratorImpl : public DB::Iterator {
    struct SourceIterator {
        virtual ~SourceIterator() = default;
        virtual bool Valid() const = 0;
        virtual void Seek(const Slice& k) = 0;
        virtual void SeekToFirst() = 0;
        virtual void Next() = 0;
        virtual Slice key() const = 0;
        virtual Slice value() const = 0;
    };

    template<typename T>
    struct Wrapper : SourceIterator {
        std::unique_ptr<T> iter_;
        Wrapper(std::unique_ptr<T> iter) : iter_(std::move(iter)) {}
        bool Valid() const override { return iter_->Valid(); }
        void Seek(const Slice& k) override { iter_->Seek(k); }
        void SeekToFirst() override { iter_->SeekToFirst(); }
        void Next() override { iter_->Next(); }
        Slice key() const override { return iter_->key(); }
        Slice value() const override { return iter_->value(); }
    };

    struct IterCmp {
        bool operator()(SourceIterator* a, SourceIterator* b) {
            return a->key().compare(b->key()) > 0;
        }
    };

public:
    DBIteratorImpl(const DB* db, const ReadOptions& options)
        : db_(db), options_(options) {
        std::lock_guard<std::mutex> lock(db_->mutex_);
        
        // Add MemTable iterator
        if (db_->mem_) {
             wrappers_.push_back(std::make_unique<Wrapper<MemTable::Iterator>>(
                 std::unique_ptr<MemTable::Iterator>(db_->mem_->NewIterator())));
        }
        
        // Add Immutable MemTables
        for (const auto& imm : db_->imm_) {
             wrappers_.push_back(std::make_unique<Wrapper<MemTable::Iterator>>(
                 std::unique_ptr<MemTable::Iterator>(imm->NewIterator())));
        }

        // Add SSTable iterators
        auto sstables = db_->sstable_index_.GetAll(); 
        for (auto& sst : sstables) {
             wrappers_.push_back(std::make_unique<Wrapper<SSTableReader::Iterator>>(
                 std::unique_ptr<SSTableReader::Iterator>(sst->NewIterator())));
        }
    }

    ~DBIteratorImpl() override = default;

    bool Valid() const override { return !pq_.empty(); }
    
    void SeekToFirst() override {
        while (!pq_.empty()) pq_.pop();
        for (auto& w : wrappers_) {
            w->SeekToFirst();
            if (w->Valid()) pq_.push(w.get());
        }
    }
    
    void SeekToLast() override {
        // Not implemented
        while (!pq_.empty()) pq_.pop();
    }
    
    void Seek(const Slice& target) override {
        while (!pq_.empty()) pq_.pop();
        for (auto& w : wrappers_) {
            w->Seek(target);
            if (w->Valid()) pq_.push(w.get());
        }
    }
    
    void Next() override {
        if (pq_.empty()) return;
        SourceIterator* top = pq_.top();
        pq_.pop();
        
        top->Next();
        if (top->Valid()) {
            pq_.push(top);
        }
    }
    
    void Prev() override {
        // Not implemented
    }
    
    Slice key() const override { return pq_.top()->key(); }
    Slice value() const override { return pq_.top()->value(); }
    Status status() const override { return Status::OK(); }

private:
    const DB* db_;
    ReadOptions options_;
    std::vector<std::unique_ptr<SourceIterator>> wrappers_;
    std::priority_queue<SourceIterator*, std::vector<SourceIterator*>, IterCmp> pq_;
};

inline std::unique_ptr<DB::Iterator> DB::NewIterator(const ReadOptions& options) {
    return std::make_unique<DBIteratorImpl>(this, options);
}

}  // namespace tskv
