#include <iostream>
#include <memory>
#include <string>
#include <filesystem>
#include "include/tskv/tskv.h"

using namespace tskv;
namespace fs = std::filesystem;

std::string toHex(const std::string& s) {
    std::string hex;
    for (unsigned char c : s) {
        char buf[4];
        snprintf(buf, 4, "%02x ", c);
        hex += buf;
    }
    return hex;
}

int main() {
    std::string db_path = "/tmp/test_db_debug2";
    fs::remove_all(db_path);
    
    Options opt;
    opt.create_if_missing = true;
    opt.write_buffer_size = 1024;  // Small buffer to force flush
    opt.enable_wal = false;
    
    std::unique_ptr<DB> db;
    Status s = DB::Open(opt, db_path, &db);
    if (!s.ok()) {
        std::cout << "Open failed: " << s.ToString() << std::endl;
        return 1;
    }
    
    // Write keys
    std::cout << "Writing keys..." << std::endl;
    for (int i = 0; i < 10; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value = "value" + std::to_string(i);
        s = db->Put(WriteOptions(), key, value);
    }
    
    // Force flush
    s = db->Flush(FlushOptions());
    std::cout << "Flush done" << std::endl;
    
    // Check SSTable files
    std::cout << "\n=== Checking SSTable files ===" << std::endl;
    for (const auto& entry : fs::directory_iterator(db_path)) {
        std::cout << "Found: " << entry.path() << " ext=" << entry.path().extension() << std::endl;
        std::string ext = entry.path().extension();
        if (ext == ".sst") {
            std::cout << "File: " << entry.path().filename() << std::endl;
            std::unique_ptr<SSTableReader> reader;
            Status s = SSTableReader::Open(entry.path().string(), &reader);
            if (s.ok()) {
                std::cout << "  smallest: \"" << reader->SmallestKey() << "\" hex=[" << toHex(reader->SmallestKey()) << "]" << std::endl;
                std::cout << "  largest:  \"" << reader->LargestKey() << "\" hex=[" << toHex(reader->LargestKey()) << "]" << std::endl;
                
                // Try to find keys in this SSTable directly
                for (int i = 0; i < 10; i++) {
                    std::string key = "key" + std::to_string(i);
                    std::string value;
                    Status s = reader->Get(key, &value);
                    if (s.ok()) {
                        std::cout << "  Found " << key << " directly" << std::endl;
                    }
                }
            } else {
                std::cout << "  Open failed: " << s.ToString() << std::endl;
            }
        }
    }
    
    return 0;
}
