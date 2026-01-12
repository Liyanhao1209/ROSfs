#include <iostream>
#include <cstring>
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
    std::string db_path = "/tmp/test_binary_key";
    
    Options opt;
    opt.create_if_missing = true;
    opt.enable_wal = false;
    
    std::unique_ptr<DB> db;
    Status s = DB::Open(opt, db_path, &db);
    if (!s.ok()) {
        std::cout << "Open failed: " << s.ToString() << std::endl;
        return 1;
    }
    
    double base_ts = 1317042193.123456;
    uint32_t topic_hash = 12345;
    
    int found = 0;
    for (int i = 0; i < 10; i++) {
        std::string key = MakeBinaryKey(base_ts + i * 0.1, topic_hash);
        std::string value;
        s = db->Get(ReadOptions(), key, &value);
        if (s.ok()) {
            std::cout << "Found key " << i << std::endl;
            found++;
        } else {
            std::cout << "NOT FOUND key " << i << std::endl;
        }
    }
    std::cout << "Found: " << found << "/10" << std::endl;
    
    return 0;
}
