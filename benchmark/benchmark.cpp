// Copyright (c) 2026 TSKV Authors. All rights reserved.
// Performance benchmarks for TSKV
// Compare with RocksDB for ablation study

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include <thread>
#include <atomic>

#include "tskv/tskv.h"

using namespace tskv;
using namespace std::chrono;

// Benchmark configuration
struct BenchmarkConfig {
    size_t num_keys = 1000000;           // 1M keys
    size_t key_size = 16;                // 16 bytes
    size_t value_size = 100;             // 100 bytes
    size_t write_buffer_size = 64 * 1024 * 1024;  // 64MB
    int num_threads = 1;
    bool sequential = true;              // Sequential or random keys
    std::string db_path = "/tmp/tskv_bench";
};

// Generate random string
std::string RandomString(size_t len, std::mt19937& rng) {
    static const char charset[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::string result;
    result.reserve(len);
    for (size_t i = 0; i < len; i++) {
        result += charset[rng() % (sizeof(charset) - 1)];
    }
    return result;
}

// Generate sequential key
std::string SequentialKey(size_t index, size_t key_size) {
    char buf[64];
    // Use key_size as the format width to ensure sortable keys
    snprintf(buf, sizeof(buf), "%0*zu", (int)key_size, index);
    return std::string(buf, key_size);
}

// Timer class
class Timer {
public:
    void Start() { start_ = high_resolution_clock::now(); }
    void Stop() { end_ = high_resolution_clock::now(); }
    
    double ElapsedMs() const {
        return duration_cast<microseconds>(end_ - start_).count() / 1000.0;
    }
    
    double ElapsedUs() const {
        return duration_cast<microseconds>(end_ - start_).count();
    }

private:
    high_resolution_clock::time_point start_;
    high_resolution_clock::time_point end_;
};

// Benchmark result
struct BenchmarkResult {
    std::string name;
    size_t ops;
    double duration_ms;
    double ops_per_sec;
    double avg_latency_us;
    double p50_us;
    double p99_us;
    double p999_us;
};

void PrintResult(const BenchmarkResult& result) {
    std::cout << std::left << std::setw(30) << result.name
              << std::right << std::setw(12) << std::fixed << std::setprecision(0) 
              << result.ops_per_sec << " ops/sec"
              << std::setw(12) << std::setprecision(2) << result.avg_latency_us << " us avg"
              << std::setw(12) << result.p99_us << " us p99"
              << std::endl;
}

// Run sequential write benchmark
BenchmarkResult BenchSequentialWrite(DB* db, const BenchmarkConfig& config) {
    std::vector<double> latencies;
    latencies.reserve(config.num_keys);
    
    WriteOptions write_opts;
    write_opts.sync = false;
    
    std::string value(config.value_size, 'x');
    
    Timer total_timer;
    total_timer.Start();
    
    for (size_t i = 0; i < config.num_keys; i++) {
        std::string key = SequentialKey(i, config.key_size);
        
        auto start = high_resolution_clock::now();
        Status s = db->Put(write_opts, key, value);
        auto end = high_resolution_clock::now();
        
        if (!s.ok()) {
            std::cerr << "Write failed: " << s.ToString() << std::endl;
            break;
        }
        
        latencies.push_back(duration_cast<nanoseconds>(end - start).count() / 1000.0);
    }
    
    total_timer.Stop();
    
    // Calculate statistics
    std::sort(latencies.begin(), latencies.end());
    
    BenchmarkResult result;
    result.name = "Sequential Write";
    result.ops = config.num_keys;
    result.duration_ms = total_timer.ElapsedMs();
    result.ops_per_sec = config.num_keys * 1000.0 / result.duration_ms;
    
    double sum = 0;
    for (double lat : latencies) sum += lat;
    result.avg_latency_us = sum / latencies.size();
    
    result.p50_us = latencies[latencies.size() * 50 / 100];
    result.p99_us = latencies[latencies.size() * 99 / 100];
    result.p999_us = latencies[latencies.size() * 999 / 1000];
    
    return result;
}

// Run random write benchmark
BenchmarkResult BenchRandomWrite(DB* db, const BenchmarkConfig& config) {
    std::vector<double> latencies;
    latencies.reserve(config.num_keys);
    
    std::mt19937 rng(42);
    WriteOptions write_opts;
    
    Timer total_timer;
    total_timer.Start();
    
    for (size_t i = 0; i < config.num_keys; i++) {
        std::string key = RandomString(config.key_size, rng);
        std::string value = RandomString(config.value_size, rng);
        
        auto start = high_resolution_clock::now();
        Status s = db->Put(write_opts, key, value);
        auto end = high_resolution_clock::now();
        
        if (!s.ok()) {
            std::cerr << "Write failed: " << s.ToString() << std::endl;
            break;
        }
        
        latencies.push_back(duration_cast<nanoseconds>(end - start).count() / 1000.0);
    }
    
    total_timer.Stop();
    
    std::sort(latencies.begin(), latencies.end());
    
    BenchmarkResult result;
    result.name = "Random Write";
    result.ops = config.num_keys;
    result.duration_ms = total_timer.ElapsedMs();
    result.ops_per_sec = config.num_keys * 1000.0 / result.duration_ms;
    
    double sum = 0;
    for (double lat : latencies) sum += lat;
    result.avg_latency_us = sum / latencies.size();
    
    result.p50_us = latencies[latencies.size() * 50 / 100];
    result.p99_us = latencies[latencies.size() * 99 / 100];
    result.p999_us = latencies[latencies.size() * 999 / 1000];
    
    return result;
}

// Run batch write benchmark
BenchmarkResult BenchBatchWrite(DB* db, const BenchmarkConfig& config) {
    std::vector<double> latencies;
    
    WriteOptions write_opts;
    const size_t batch_size = 1000;
    std::string value(config.value_size, 'x');
    
    Timer total_timer;
    total_timer.Start();
    
    for (size_t i = 0; i < config.num_keys; i += batch_size) {
        WriteBatch batch;
        for (size_t j = 0; j < batch_size && (i + j) < config.num_keys; j++) {
            std::string key = SequentialKey(i + j, config.key_size);
            batch.Put(key, value);
        }
        
        auto start = high_resolution_clock::now();
        Status s = db->Write(write_opts, &batch);
        auto end = high_resolution_clock::now();
        
        if (!s.ok()) {
            std::cerr << "Batch write failed: " << s.ToString() << std::endl;
            break;
        }
        
        latencies.push_back(duration_cast<nanoseconds>(end - start).count() / 1000.0);
    }
    
    total_timer.Stop();
    
    std::sort(latencies.begin(), latencies.end());
    
    BenchmarkResult result;
    result.name = "Batch Write (1000)";
    result.ops = config.num_keys;
    result.duration_ms = total_timer.ElapsedMs();
    result.ops_per_sec = config.num_keys * 1000.0 / result.duration_ms;
    
    double sum = 0;
    for (double lat : latencies) sum += lat;
    result.avg_latency_us = sum / latencies.size();
    
    result.p50_us = latencies[latencies.size() * 50 / 100];
    result.p99_us = latencies[latencies.size() * 99 / 100];
    result.p999_us = latencies[latencies.size() * 999 / 1000];
    
    return result;
}

// Run sequential read benchmark
BenchmarkResult BenchSequentialRead(DB* db, const BenchmarkConfig& config) {
    std::vector<double> latencies;
    latencies.reserve(config.num_keys);
    
    ReadOptions read_opts;
    
    Timer total_timer;
    total_timer.Start();
    
    size_t found = 0;
    for (size_t i = 0; i < config.num_keys; i++) {
        std::string key = SequentialKey(i, config.key_size);
        std::string value;
        
        auto start = high_resolution_clock::now();
        Status s = db->Get(read_opts, key, &value);
        auto end = high_resolution_clock::now();
        
        if (s.ok()) found++;
        latencies.push_back(duration_cast<nanoseconds>(end - start).count() / 1000.0);
    }
    
    total_timer.Stop();
    
    std::sort(latencies.begin(), latencies.end());
    
    BenchmarkResult result;
    result.name = "Sequential Read (found " + std::to_string(found * 100 / config.num_keys) + "%)";
    result.ops = config.num_keys;
    result.duration_ms = total_timer.ElapsedMs();
    result.ops_per_sec = config.num_keys * 1000.0 / result.duration_ms;
    
    double sum = 0;
    for (double lat : latencies) sum += lat;
    result.avg_latency_us = sum / latencies.size();
    
    result.p50_us = latencies[latencies.size() * 50 / 100];
    result.p99_us = latencies[latencies.size() * 99 / 100];
    result.p999_us = latencies[latencies.size() * 999 / 1000];
    
    return result;
}

// Run random read benchmark
BenchmarkResult BenchRandomRead(DB* db, const BenchmarkConfig& config) {
    std::vector<double> latencies;
    latencies.reserve(config.num_keys);
    
    std::mt19937 rng(42);
    ReadOptions read_opts;
    
    // Generate random indices
    std::vector<size_t> indices(config.num_keys);
    for (size_t i = 0; i < config.num_keys; i++) {
        indices[i] = rng() % config.num_keys;
    }
    
    Timer total_timer;
    total_timer.Start();
    
    size_t found = 0;
    for (size_t i = 0; i < config.num_keys; i++) {
        std::string key = SequentialKey(indices[i], config.key_size);
        std::string value;
        
        auto start = high_resolution_clock::now();
        Status s = db->Get(read_opts, key, &value);
        auto end = high_resolution_clock::now();
        
        if (s.ok()) found++;
        latencies.push_back(duration_cast<nanoseconds>(end - start).count() / 1000.0);
    }
    
    total_timer.Stop();
    
    std::sort(latencies.begin(), latencies.end());
    
    BenchmarkResult result;
    result.name = "Random Read (found " + std::to_string(found * 100 / config.num_keys) + "%)";
    result.ops = config.num_keys;
    result.duration_ms = total_timer.ElapsedMs();
    result.ops_per_sec = config.num_keys * 1000.0 / result.duration_ms;
    
    double sum = 0;
    for (double lat : latencies) sum += lat;
    result.avg_latency_us = sum / latencies.size();
    
    result.p50_us = latencies[latencies.size() * 50 / 100];
    result.p99_us = latencies[latencies.size() * 99 / 100];
    result.p999_us = latencies[latencies.size() * 999 / 1000];
    
    return result;
}

// Multi-threaded write benchmark
BenchmarkResult BenchConcurrentWrite(DB* db, const BenchmarkConfig& config) {
    std::atomic<size_t> total_ops{0};
    std::atomic<double> total_latency{0};
    std::vector<std::thread> threads;
    
    Timer total_timer;
    total_timer.Start();
    
    size_t keys_per_thread = config.num_keys / config.num_threads;
    
    for (int t = 0; t < config.num_threads; t++) {
        threads.emplace_back([&, t]() {
            WriteOptions write_opts;
            std::string value(config.value_size, 'x');
            double thread_latency = 0;
            
            size_t start_key = t * keys_per_thread;
            size_t end_key = start_key + keys_per_thread;
            
            for (size_t i = start_key; i < end_key; i++) {
                std::string key = SequentialKey(i, config.key_size);
                
                auto start = high_resolution_clock::now();
                db->Put(write_opts, key, value);
                auto end = high_resolution_clock::now();
                
                thread_latency += duration_cast<nanoseconds>(end - start).count() / 1000.0;
                total_ops.fetch_add(1, std::memory_order_relaxed);
            }
            
            // Add to total (approximate)
            double old_val = total_latency.load();
            while (!total_latency.compare_exchange_weak(old_val, old_val + thread_latency));
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    total_timer.Stop();
    
    BenchmarkResult result;
    result.name = "Concurrent Write (" + std::to_string(config.num_threads) + " threads)";
    result.ops = total_ops.load();
    result.duration_ms = total_timer.ElapsedMs();
    result.ops_per_sec = result.ops * 1000.0 / result.duration_ms;
    result.avg_latency_us = total_latency.load() / result.ops;
    result.p50_us = 0;  // Not measured for concurrent
    result.p99_us = 0;
    result.p999_us = 0;
    
    return result;
}

// SkipList microbenchmark
void BenchSkipList(const BenchmarkConfig& config) {
    std::cout << "\n=== SkipList Microbenchmark ===" << std::endl;
    
    Arena arena;
    SkipList<> list(BytewiseComparator(), &arena);
    
    // Insert benchmark
    Timer timer;
    timer.Start();
    
    for (size_t i = 0; i < config.num_keys; i++) {
        std::string key = SequentialKey(i, config.key_size);
        std::string encoded;
        PutVarint32(&encoded, static_cast<uint32_t>(key.size()));
        encoded.append(key);
        
        char* buf = list.AllocateKey(encoded.size());
        memcpy(buf, encoded.data(), encoded.size());
        list.Insert(buf);
    }
    
    timer.Stop();
    
    std::cout << "Insert " << config.num_keys << " keys: " 
              << std::fixed << std::setprecision(0)
              << (config.num_keys * 1000.0 / timer.ElapsedMs()) << " ops/sec" << std::endl;
    
    // Lookup benchmark
    timer.Start();
    
    for (size_t i = 0; i < config.num_keys; i++) {
        std::string key = SequentialKey(i, config.key_size);
        std::string encoded;
        PutVarint32(&encoded, static_cast<uint32_t>(key.size()));
        encoded.append(key);
        
        list.Contains(encoded.c_str());
    }
    
    timer.Stop();
    
    std::cout << "Lookup " << config.num_keys << " keys: "
              << std::fixed << std::setprecision(0)
              << (config.num_keys * 1000.0 / timer.ElapsedMs()) << " ops/sec" << std::endl;
    
    std::cout << "Memory usage: " << (arena.MemoryUsage() / 1024.0 / 1024.0) << " MB" << std::endl;
}

int main(int argc, char* argv[]) {
    BenchmarkConfig config;
    
    // Parse arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--keys" && i + 1 < argc) {
            config.num_keys = std::stoul(argv[++i]);
        } else if (arg == "--key-size" && i + 1 < argc) {
            config.key_size = std::stoul(argv[++i]);
        } else if (arg == "--value-size" && i + 1 < argc) {
            config.value_size = std::stoul(argv[++i]);
        } else if (arg == "--threads" && i + 1 < argc) {
            config.num_threads = std::stoi(argv[++i]);
        } else if (arg == "--db-path" && i + 1 < argc) {
            config.db_path = argv[++i];
        }
    }
    
    std::cout << "TSKV Performance Benchmark" << std::endl;
    std::cout << "==========================" << std::endl;
    std::cout << "Configuration:" << std::endl;
    std::cout << "  Keys: " << config.num_keys << std::endl;
    std::cout << "  Key size: " << config.key_size << " bytes" << std::endl;
    std::cout << "  Value size: " << config.value_size << " bytes" << std::endl;
    std::cout << "  Write buffer: " << (config.write_buffer_size / 1024 / 1024) << " MB" << std::endl;
    std::cout << "  Threads: " << config.num_threads << std::endl;
    std::cout << "  DB path: " << config.db_path << std::endl;
    std::cout << std::endl;
    
    // Run SkipList benchmark
    BenchSkipList(config);
    
    // Clean up and create database
    std::system(("rm -rf " + config.db_path).c_str());
    
    Options options;
    options.create_if_missing = true;
    options.write_buffer_size = config.write_buffer_size;
    options.time_series_mode = true;
    
    std::unique_ptr<DB> db;
    Status s = DB::Open(options, config.db_path, &db);
    if (!s.ok()) {
        std::cerr << "Failed to open database: " << s.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "\n=== Database Benchmark ===" << std::endl;
    
    // Run benchmarks
    std::vector<BenchmarkResult> results;
    
    results.push_back(BenchSequentialWrite(db.get(), config));
    PrintResult(results.back());
    
    results.push_back(BenchSequentialRead(db.get(), config));
    PrintResult(results.back());
    
    // Flush to disk
    FlushOptions flush_opts;
    flush_opts.wait = true;
    db->Flush(flush_opts);
    
    results.push_back(BenchRandomRead(db.get(), config));
    PrintResult(results.back());
    
    // Reopen for batch test
    db.reset();
    std::system(("rm -rf " + config.db_path).c_str());
    s = DB::Open(options, config.db_path, &db);
    
    results.push_back(BenchBatchWrite(db.get(), config));
    PrintResult(results.back());
    
    // Concurrent test
    if (config.num_threads > 1) {
        db.reset();
        std::system(("rm -rf " + config.db_path).c_str());
        s = DB::Open(options, config.db_path, &db);
        
        results.push_back(BenchConcurrentWrite(db.get(), config));
        PrintResult(results.back());
    }
    
    // Print final statistics
    std::cout << "\n=== Final Statistics ===" << std::endl;
    const auto& stats = db->GetStats();
    std::cout << "Bytes written: " << (stats.bytes_written.load() / 1024.0 / 1024.0) << " MB" << std::endl;
    std::cout << "Bytes read: " << (stats.bytes_read.load() / 1024.0 / 1024.0) << " MB" << std::endl;
    std::cout << "Keys written: " << stats.keys_written.load() << std::endl;
    std::cout << "Keys read: " << stats.keys_read.load() << std::endl;
    std::cout << "Memtable hits: " << stats.memtable_hits.load() << std::endl;
    std::cout << "SSTable hits: " << stats.sstable_hits.load() << std::endl;
    std::cout << "Flushes: " << stats.flushes.load() << std::endl;
    
    // Cleanup
    db.reset();
    std::system(("rm -rf " + config.db_path).c_str());
    
    return 0;
}
