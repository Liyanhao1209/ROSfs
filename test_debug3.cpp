#include <iostream>
#include <memory>
#include <string>
#include <filesystem>
#include "include/tskv/tskv.h"
#include "include/tskv/sstable.h"

using namespace tskv;
namespace fs = std::filesystem;

int main() {
    std::string db_path = "/tmp/test_debug3";
    fs::remove_all(db_path);
    
    // Create a simple SSTable with known keys
    {
        SSTableBuilder builder(db_path + "/test.sst");
        for (int i = 0; i < 10; i++) {
            std::string key = "key" + std::to_string(i);
            std::string value = "value" + std::to_string(i);
            builder.Add(Slice(key), Slice(value));
        }
        builder.Finish();
        
        std::cout << "SSTable created with keys key0-key9" << std::endl;
        std::cout << "Smallest key: " << builder.SmallestKey() << std::endl;
        std::cout << "Largest key: " << builder.LargestKey() << std::endl;
    }
    
    // Read back
    std::unique_ptr<SSTableReader> reader;
    Status s = SSTableReader::Open(db_path + "/test.sst", &reader);
    if (!s.ok()) {
        std::cout << "Failed to open SSTable: " << s.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "\nSSTable metadata:" << std::endl;
    std::cout << "  Smallest: " << reader->SmallestKey() << std::endl;
    std::cout << "  Largest: " << reader->LargestKey() << std::endl;
    std::cout << "  Entries: " << reader->NumEntries() << std::endl;
    
    // Try to find each key
    std::cout << "\nLookup test:" << std::endl;
    for (int i = 0; i < 10; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value;
        s = reader->Get(Slice(key), &value);
        std::cout << "  " << key << " => ";
        if (s.ok()) {
            std::cout << value << std::endl;
        } else {
            std::cout << "NOT FOUND" << std::endl;
        }
    }
    
    return 0;
}
