#include <iostream>
#include <iomanip>
#include <sstream>
#include "include/tskv/tskv.h"

using namespace tskv;

std::string MakeTimestampKey(uint64_t ts) {
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(16) << ts;
    return oss.str();
}

int main() {
    std::string db_path = "/tmp/test_monotonic2";
    system(("rm -rf " + db_path).c_str());
    
    Options opt;
    opt.create_if_missing = true;
    opt.write_buffer_size = 4096;
    opt.enable_wal = false;
    
    std::unique_ptr<DB> db;
    DB::Open(opt, db_path, &db);
    
    // Write 20 keys
    for (int i = 0; i < 20; i++) {
        std::string key = MakeTimestampKey(1000000 + i);
        db->Put(WriteOptions(), key, "v" + std::to_string(i));
    }
    db->Flush(FlushOptions());
    
    // Get all SSTables from index
    std::cout << "SSTables in index:" << std::endl;
    auto all = db->GetSSTableIndex().GetAll();
    std::cout << "  Count: " << all.size() << std::endl;
    for (const auto& sst : all) {
        std::cout << "  [" << sst->SmallestKey() << ", " << sst->LargestKey() << "]" << std::endl;
    }
    
    // Test lookup
    std::cout << "\nLooking up keys:" << std::endl;
    for (int i = 0; i < 20; i++) {
        std::string key = MakeTimestampKey(1000000 + i);
        auto sst = db->GetSSTableIndex().FindOne(key);
        std::cout << "  " << key << " -> ";
        if (sst) {
            std::cout << "[" << sst->SmallestKey() << ", " << sst->LargestKey() << "]";
        } else {
            std::cout << "NOT FOUND";
        }
        std::cout << std::endl;
    }
    
    return 0;
}
