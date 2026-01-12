#include <iostream>
#include <memory>
#include <string>
#include <filesystem>
#include <thread>
#include <chrono>
#include "include/tskv/tskv.h"

using namespace tskv;
namespace fs = std::filesystem;

int main() {
    std::string db_path = "/tmp/test_db_debug3";
    fs::remove_all(db_path);
    
    Options opt;
    opt.create_if_missing = true;
    opt.write_buffer_size = 8 * 1024 * 1024; // 8MB - normal size
    opt.enable_wal = false;
    
    std::unique_ptr<DB> db;
    Status s = DB::Open(opt, db_path, &db);
    if (!s.ok()) {
        std::cout << "Open failed: " << s.ToString() << std::endl;
        return 1;
    }
    
    // Write enough keys to trigger multiple flushes
    std::cout << "Writing keys..." << std::endl;
    for (int i = 0; i < 100; i++) {
        std::string key = "key" + std::to_string(i);
        // Large value to fill memtable faster
        std::string value(100 * 1024, 'v'); // 100KB value
        value.append(std::to_string(i));
        s = db->Put(WriteOptions(), key, value);
        if (!s.ok()) {
            std::cout << "Put failed at " << i << std::endl;
        }
    }
    
    // Force flush
    std::cout << "Flushing..." << std::endl;
    s = db->Flush(FlushOptions());
    
    // Give background thread time
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Count SSTable files
    int sst_count = 0;
    for (const auto& entry : fs::directory_iterator(db_path)) {
        if (entry.path().extension() == ".sst") {
            sst_count++;
        }
    }
    std::cout << "Created " << sst_count << " SSTable files" << std::endl;
    
    // Test reads
    std::cout << "\nReading keys..." << std::endl;
    int found = 0;
    for (int i = 0; i < 100; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value;
        s = db->Get(ReadOptions(), key, &value);
        if (s.ok()) {
            found++;
        } else {
            std::cout << "NOT FOUND: " << key << std::endl;
        }
    }
    std::cout << "Found " << found << "/100 keys" << std::endl;
    
    return 0;
}
