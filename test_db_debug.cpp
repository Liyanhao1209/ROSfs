#include <iostream>
#include <memory>
#include <string>
#include "include/tskv/tskv.h"

using namespace tskv;

int main() {
    std::string db_path = "/tmp/test_db_debug";
    system(("rm -rf " + db_path).c_str());
    
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
        if (!s.ok()) {
            std::cout << "Put failed: " << s.ToString() << std::endl;
        }
        std::cout << "Put: " << key << std::endl;
    }
    
    // Force flush
    std::cout << "Flushing..." << std::endl;
    s = db->Flush(FlushOptions());
    if (!s.ok()) {
        std::cout << "Flush failed: " << s.ToString() << std::endl;
    }
    
    // Test reads
    std::cout << "Reading keys..." << std::endl;
    int found = 0;
    for (int i = 0; i < 10; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value;
        s = db->Get(ReadOptions(), key, &value);
        if (s.ok()) {
            std::cout << "Found: " << key << " = " << value << std::endl;
            found++;
        } else {
            std::cout << "NOT FOUND: " << key << std::endl;
        }
    }
    std::cout << "Found " << found << "/10 keys" << std::endl;
    
    return 0;
}
