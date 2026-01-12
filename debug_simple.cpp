#include <iostream>
#include <iomanip>
#include <sstream>
#include <filesystem>
#include "include/tskv/tskv.h"

using namespace tskv;
namespace fs = std::filesystem;

std::string MakeKey(uint64_t ts) {
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(16) << ts;
    return oss.str();
}

int main() {
    std::string db_path = "/tmp/test_debug_simple";
    system(("rm -rf " + db_path).c_str());
    
    Options opt;
    opt.create_if_missing = true;
    opt.write_buffer_size = 4096;
    opt.enable_wal = false;
    
    std::unique_ptr<DB> db;
    DB::Open(opt, db_path, &db);
    
    // Write 20 keys
    std::cout << "Writing 20 keys..." << std::endl;
    for (int i = 0; i < 20; i++) {
        std::string key = MakeKey(1000000 + i);
        db->Put(WriteOptions(), key, "v" + std::to_string(i));
    }
    db->Flush(FlushOptions());
    
    // Count SST files
    int sst_count = 0;
    for (const auto& entry : fs::directory_iterator(db_path)) {
        if (entry.path().extension() == ".sst") {
            sst_count++;
            std::unique_ptr<SSTableReader> reader;
            SSTableReader::Open(entry.path().string(), &reader);
            std::cout << "SST: [" << reader->SmallestKey() << ", " << reader->LargestKey() << "]" << std::endl;
        }
    }
    std::cout << "Total SST files: " << sst_count << std::endl;
    
    // Test reads
    int found = 0, not_found = 0;
    for (int i = 0; i < 20; i++) {
        std::string key = MakeKey(1000000 + i);
        std::string value;
        if (db->Get(ReadOptions(), key, &value).ok()) {
            found++;
        } else {
            if (not_found < 5) {
                std::cout << "NOT FOUND: " << key << std::endl;
            }
            not_found++;
        }
    }
    std::cout << "Found: " << found << "/20" << std::endl;
    
    return 0;
}
