// Copyright (c) 2026 TSKV Authors. All rights reserved.
// Simple example demonstrating TSKV usage

#include <iostream>
#include <chrono>
#include "tskv/tskv.h"

int main() {
    std::cout << "TSKV - Time-Series Key-Value Store v" << tskv::Version() << std::endl;
    std::cout << "============================================" << std::endl;

    // Create database
    tskv::Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 4 * 1024 * 1024;  // 4MB for demo
    options.time_series_mode = true;

    std::unique_ptr<tskv::DB> db;
    tskv::Status s = tskv::DB::Open(options, "/tmp/tskv_example", &db);
    if (!s.ok()) {
        std::cerr << "Failed to open database: " << s.ToString() << std::endl;
        return 1;
    }

    std::cout << "Database opened successfully" << std::endl;

    // Write some time-series data (simulating robot sensor data)
    tskv::WriteOptions write_opts;
    write_opts.sync = false;

    auto start = std::chrono::high_resolution_clock::now();

    const int num_keys = 100000;
    for (int i = 0; i < num_keys; i++) {
        // Simulate timestamp-based keys
        std::string key = "sensor_data_" + std::to_string(i);
        std::string value = "position_x=" + std::to_string(i * 0.1) +
                           ",position_y=" + std::to_string(i * 0.2) +
                           ",velocity=" + std::to_string(i * 0.01);

        s = db->Put(write_opts, key, value);
        if (!s.ok()) {
            std::cerr << "Put failed: " << s.ToString() << std::endl;
            return 1;
        }
    }

    auto write_end = std::chrono::high_resolution_clock::now();
    auto write_duration = std::chrono::duration_cast<std::chrono::milliseconds>(write_end - start);
    
    std::cout << "Wrote " << num_keys << " keys in " << write_duration.count() << " ms" << std::endl;
    std::cout << "Write throughput: " << (num_keys * 1000.0 / write_duration.count()) << " ops/sec" << std::endl;

    // Read data back
    tskv::ReadOptions read_opts;
    std::string value;

    start = std::chrono::high_resolution_clock::now();

    int found = 0;
    for (int i = 0; i < num_keys; i++) {
        std::string key = "sensor_data_" + std::to_string(i);
        s = db->Get(read_opts, key, &value);
        if (s.ok()) {
            found++;
        }
    }

    auto read_end = std::chrono::high_resolution_clock::now();
    auto read_duration = std::chrono::duration_cast<std::chrono::milliseconds>(read_end - start);

    std::cout << "Read " << found << " keys in " << read_duration.count() << " ms" << std::endl;
    std::cout << "Read throughput: " << (found * 1000.0 / read_duration.count()) << " ops/sec" << std::endl;

    // Print statistics
    const auto& stats = db->GetStats();
    std::cout << "\nDatabase Statistics:" << std::endl;
    std::cout << "  Bytes written: " << stats.bytes_written.load() << std::endl;
    std::cout << "  Bytes read: " << stats.bytes_read.load() << std::endl;
    std::cout << "  Keys written: " << stats.keys_written.load() << std::endl;
    std::cout << "  Keys read: " << stats.keys_read.load() << std::endl;
    std::cout << "  Memtable hits: " << stats.memtable_hits.load() << std::endl;
    std::cout << "  SSTable hits: " << stats.sstable_hits.load() << std::endl;
    std::cout << "  Flushes: " << stats.flushes.load() << std::endl;

    // Test batch writes
    std::cout << "\nTesting batch writes..." << std::endl;
    
    tskv::WriteBatch batch;
    start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < 10000; i++) {
        std::string key = "batch_key_" + std::to_string(i);
        std::string val = "batch_value_" + std::to_string(i);
        batch.Put(key, val);
    }

    s = db->Write(write_opts, &batch);
    if (!s.ok()) {
        std::cerr << "Batch write failed: " << s.ToString() << std::endl;
        return 1;
    }

    auto batch_end = std::chrono::high_resolution_clock::now();
    auto batch_duration = std::chrono::duration_cast<std::chrono::microseconds>(batch_end - start);
    std::cout << "Batch write of 10000 keys in " << batch_duration.count() << " us" << std::endl;

    // Flush to disk
    tskv::FlushOptions flush_opts;
    flush_opts.wait = true;
    s = db->Flush(flush_opts);
    if (!s.ok()) {
        std::cerr << "Flush failed: " << s.ToString() << std::endl;
    }

    std::cout << "\nTest completed successfully!" << std::endl;
    return 0;
}
