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
    std::string db_path = "/tmp/test_db_debug3";  // use existing data
    
    // Check SSTable files
    std::cout << "=== Checking SSTable files ===" << std::endl;
    for (const auto& entry : fs::directory_iterator(db_path)) {
        if (entry.path().extension() == ".sst") {
            std::cout << "\nFile: " << entry.path().filename() << std::endl;
            std::unique_ptr<SSTableReader> reader;
            Status s = SSTableReader::Open(entry.path().string(), &reader);
            if (s.ok()) {
                std::cout << "  smallest: \"" << reader->SmallestKey() << "\"" << std::endl;
                std::cout << "  largest:  \"" << reader->LargestKey() << "\"" << std::endl;
                std::cout << "  num_entries: " << reader->NumEntries() << std::endl;
                
                // Check for missing keys
                for (int i = 82; i <= 89; i++) {
                    std::string key = "key" + std::to_string(i);
                    std::string value;
                    Status s = reader->Get(key, &value);
                    if (s.ok()) {
                        std::cout << "  Found " << key << " in this SSTable" << std::endl;
                    }
                }
            } else {
                std::cout << "  Open failed: " << s.ToString() << std::endl;
            }
        }
    }
    
    return 0;
}
