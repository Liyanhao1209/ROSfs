// Copyright (c) 2026 TSKV Authors. All rights reserved.
// Configuration options for TSKV

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace tskv {

struct Options {
    // ------- Basic Options -------

    // Create the DB if it's missing
    bool create_if_missing = true;

    // Throw an error if the DB already exists
    bool error_if_exists = false;

    // ------- Memory Options -------

    // Size of a single memtable (default: 64MB)
    size_t write_buffer_size = 64 * 1024 * 1024;

    // Maximum number of memtables to keep before blocking writes
    int max_write_buffer_number = 3;

    // ------- SSTable Options -------

    // Target size for SSTable data blocks (default: 4KB)
    size_t block_size = 4 * 1024;

    // Restart interval for data block compression
    int block_restart_interval = 16;

    // ------- Performance Options -------

    // Enable direct I/O for reads (bypass OS page cache)
    bool use_direct_reads = false;

    // Enable direct I/O for writes
    bool use_direct_writes = false;

    // Number of background threads for flushing
    int flush_threads = 1;

    // Enable WAL (Write-Ahead Log)
    bool enable_wal = true;

    // Sync WAL on every write (slower but safer)
    bool sync_wal = false;

    // ------- Time-Series Optimization Options -------

    // Optimize for sequential/time-series data
    // When true, disables compaction and uses append-only storage
    bool time_series_mode = true;

    // Expected key size (for memory pre-allocation)
    size_t expected_key_size = 32;

    // Expected value size (for memory pre-allocation)
    size_t expected_value_size = 256;
};

struct ReadOptions {
    // If true, all data read from underlying storage will be verified
    bool verify_checksums = false;

    // If true, read only from memtable (useful for recent data)
    bool read_memtable_only = false;

    // Snapshot to read from (0 = latest)
    uint64_t snapshot = 0;
};

struct WriteOptions {
    // If true, sync the write to disk before returning
    bool sync = false;

    // If true, skip writing to WAL (faster but less durable)
    bool disable_wal = false;
};

struct FlushOptions {
    // Wait for the flush to complete before returning
    bool wait = true;
};

}  // namespace tskv
