#include <iostream>
#include <memory>
#include <string>
#include <filesystem>
#include "include/tskv/tskv.h"

using namespace tskv;
namespace fs = std::filesystem;

int main() {
    std::string db_path = "/tmp/test_debug";
    fs::remove_all(db_path);
    
    Options opt;
    opt.create_if_missing = true;
    opt.write_buffer_size = 4 * 1024 * 1024;  // 4MB to trigger more flushes
    opt.enable_wal = false;
    
    std::unique_ptr<DB> db;
    Status s = DB::Open(opt, db_path, &db);
    
    // Write fewer items to debug
    std::vector<std::string> keys;
    for (int i = 0; i < 50; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value(100 * 1024, 'v');  // 100KB values to trigger flush
        value.append(std::to_string(i));
        keys.push_back(key);
        db->Put(WriteOptions(), key, value);
    }
    
    // Flush
    db->Flush(FlushOptions());
    
    std::cout << "\n=== Before close ===\n";
    int found_before = 0;
    for (const auto& key : keys) {
        std::string value;
        if (db->Get(ReadOptions(), key, &value).ok()) {
            found_before++;
        } else {
            std::cout << "NOT FOUND BEFORE CLOSE: " << key << std::endl;
        }
    }
    std::cout << "Found before close: " << found_before << "/" << keys.size() << std::endl;
    
    // Close and reopen
    db.reset();
    
    s = DB::Open(opt, db_path, &db);
    if (!s.ok()) {
        std::cout << "Reopen failed: " << s.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "\n=== SSTable files ===\n";
    for (const auto& entry : fs::directory_iterator(db_path)) {
        std::cout << entry.path().filename() << std::endl;
    }
    
    std::cout << "\n=== After reopen ===\n";
    int found_after = 0;
    for (const auto& key : keys) {
        std::string value;
        if (db->Get(ReadOptions(), key, &value).ok()) {
            found_after++;
        } else {
            std::cout << "NOT FOUND AFTER REOPEN: " << key << std::endl;
        }
    }
    std::cout << "Found after reopen: " << found_after << "/" << keys.size() << std::endl;
    
    return 0;
}
