#include <iostream>
#include <cstring>
#include <thread>
#include <chrono>
#include "include/tskv/tskv.h"

using namespace tskv;

std::string MakeBinaryKey(double ts, uint32_t topic_hash) {
    std::string key;
    key.resize(sizeof(double) + sizeof(uint32_t));
    memcpy(&key[0], &ts, sizeof(double));
    memcpy(&key[sizeof(double)], &topic_hash, sizeof(uint32_t));
    return key;
}

int main() {
    std::string db_path = "/tmp/test_write_read";
    system(("rm -rf " + db_path).c_str());
    
    Options opt;
    opt.create_if_missing = true;
    opt.write_buffer_size = 64 * 1024 * 1024;
    opt.enable_wal = false;
    
    std::unique_ptr<DB> db;
    DB::Open(opt, db_path, &db);
    
    double base_ts = 1317042193.123456;
    uint32_t topic_hash = 12345;
    const int N = 100;
    
    // Write
    std::cout << "Writing " << N << " keys..." << std::endl;
    for (int i = 0; i < N; i++) {
        std::string key = MakeBinaryKey(base_ts + i * 0.1, topic_hash);
        std::string value = "value" + std::to_string(i);
        db->Put(WriteOptions(), key, value);
    }
    
    std::cout << "Flush..." << std::endl;
    db->Flush(FlushOptions());
    
    // Wait for background flush
    std::cout << "Waiting for background flush..." << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Read
    int found = 0;
    for (int i = 0; i < N; i++) {
        std::string key = MakeBinaryKey(base_ts + i * 0.1, topic_hash);
        std::string value;
        if (db->Get(ReadOptions(), key, &value).ok()) {
            found++;
        }
    }
    std::cout << "Found: " << found << "/" << N << std::endl;
    
    return 0;
}
