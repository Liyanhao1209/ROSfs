#include <iostream>
#include <memory>
#include <string>
#include <filesystem>
#include "include/tskv/tskv.h"

using namespace tskv;
namespace fs = std::filesystem;

int main() {
    std::string db_path = "/tmp/test_db_debug5";
    fs::remove_all(db_path);
    
    Options opt;
    opt.create_if_missing = true;
    opt.write_buffer_size = 8 * 1024 * 1024;
    opt.enable_wal = false;
    
    std::unique_ptr<DB> db;
    Status s = DB::Open(opt, db_path, &db);
    
    // Write enough to trigger multiple flushes
    for (int i = 0; i < 100; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value(100 * 1024, 'v');
        value.append(std::to_string(i));
        db->Put(WriteOptions(), key, value);
    }
    
    // Flush
    db->Flush(FlushOptions());
    
    // Close and reopen to make sure all flushes are done
    db.reset();
    
    s = DB::Open(opt, db_path, &db);
    if (!s.ok()) {
        std::cout << "Reopen failed: " << s.ToString() << std::endl;
        return 1;
    }
    
    // Count SSTable files
    int sst_count = 0;
    for (const auto& entry : fs::directory_iterator(db_path)) {
        if (entry.path().extension() == ".sst") {
            sst_count++;
            std::cout << "SSTable: " << entry.path().filename() << std::endl;
        }
    }
    std::cout << "Total " << sst_count << " SSTable files\n" << std::endl;
    
    // Test reads
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
    std::cout << "\nFound " << found << "/100 keys" << std::endl;
    
    return 0;
}
