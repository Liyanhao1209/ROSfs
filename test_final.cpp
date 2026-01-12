#include <iostream>
#include <memory>
#include <string>
#include <filesystem>
#include <thread>
#include <chrono>
#include "include/tskv/tskv.h"
#include "include/tskv/sstable.h"

using namespace tskv;
namespace fs = std::filesystem;

int main() {
    std::string db_path = "/tmp/test_final";
    fs::remove_all(db_path);
    
    Options opt;
    opt.create_if_missing = true;
    opt.write_buffer_size = 4 * 1024 * 1024;  // 4MB
    opt.enable_wal = false;
    
    std::unique_ptr<DB> db;
    Status s = DB::Open(opt, db_path, &db);
    
    // Write 50 keys, 100KB each = 5MB total, should trigger ~1 flush
    for (int i = 0; i < 50; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value(100 * 1024, 'v');
        value.append(std::to_string(i));
        db->Put(WriteOptions(), key, value);
    }
    
    // Flush with wait
    FlushOptions fo;
    fo.wait = true;
    db->Flush(fo);
    
    // List SSTable files and check their ranges
    std::cout << "=== SSTable files ===" << std::endl;
    for (const auto& entry : fs::directory_iterator(db_path)) {
        if (entry.path().extension() == ".sst") {
            std::unique_ptr<SSTableReader> reader;
            s = SSTableReader::Open(entry.path().string(), &reader);
            if (s.ok()) {
                std::cout << entry.path().filename() << ": " 
                          << "[" << reader->SmallestKey() << " - " << reader->LargestKey() << "]"
                          << " entries=" << reader->NumEntries() << std::endl;
                
                // Test a few gets on this SST directly
                for (int i = 0; i < 10; i++) {
                    std::string key = "key" + std::to_string(i);
                    std::string value;
                    if (reader->MayContain(Slice(key))) {
                        if (reader->Get(Slice(key), &value).ok()) {
                            std::cout << "  " << key << " found in this SST" << std::endl;
                        }
                    }
                }
            }
        }
    }
    
    // Now read all keys through DB
    std::cout << "\n=== DB Get tests ===" << std::endl;
    int found = 0;
    for (int i = 0; i < 50; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value;
        if (db->Get(ReadOptions(), key, &value).ok()) {
            found++;
        } else {
            std::cout << "NOT FOUND: " << key << std::endl;
        }
    }
    std::cout << "Found: " << found << "/50" << std::endl;
    
    return 0;
}
