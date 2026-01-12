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
    std::string db_path = "/tmp/test_debug2";
    fs::remove_all(db_path);
    
    Options opt;
    opt.create_if_missing = true;
    opt.write_buffer_size = 4 * 1024 * 1024;  // 4MB
    opt.enable_wal = false;
    
    std::unique_ptr<DB> db;
    Status s = DB::Open(opt, db_path, &db);
    
    // Write and immediately check each key
    for (int i = 0; i < 50; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value(100 * 1024, 'v');  // 100KB values
        value.append(std::to_string(i));
        
        db->Put(WriteOptions(), key, value);
        
        // Verify immediately after write
        std::string read_value;
        if (!db->Get(ReadOptions(), key, &read_value).ok()) {
            std::cout << "WRITE-READ FAIL: " << key << " not found immediately after Put!" << std::endl;
        }
    }
    
    std::cout << "All 50 keys written and verified" << std::endl;
    
    // Flush with wait
    std::cout << "Flushing..." << std::endl;
    FlushOptions fo;
    fo.wait = true;
    db->Flush(fo);
    
    // Verify after flush
    std::cout << "Verifying after flush..." << std::endl;
    int found = 0;
    for (int i = 0; i < 50; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value;
        if (db->Get(ReadOptions(), key, &value).ok()) {
            found++;
        } else {
            std::cout << "NOT FOUND AFTER FLUSH: " << key << std::endl;
        }
    }
    std::cout << "Found after flush: " << found << "/50" << std::endl;
    
    return 0;
}
