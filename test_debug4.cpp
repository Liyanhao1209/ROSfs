#include <iostream>
#include <memory>
#include <string>
#include <filesystem>
#include "include/tskv/tskv.h"
#include "include/tskv/sstable.h"

using namespace tskv;
namespace fs = std::filesystem;

int main() {
    std::string sst_file = "/tmp/test_sst.sst";
    std::remove(sst_file.c_str());
    
    // Create a simple SSTable with known keys
    std::cout << "Creating SSTable..." << std::endl;
    {
        SSTableBuilder builder(sst_file);
        for (int i = 0; i < 10; i++) {
            std::string key = "key" + std::to_string(i);
            std::string value = "value" + std::to_string(i);
            std::cout << "  Adding " << key << " => " << value << std::endl;
            builder.Add(Slice(key), Slice(value));
        }
        std::cout << "Finishing..." << std::endl;
        Status s = builder.Finish();
        std::cout << "Finish status: " << s.ToString() << std::endl;
        std::cout << "Smallest key: [" << builder.SmallestKey() << "]" << std::endl;
        std::cout << "Largest key: [" << builder.LargestKey() << "]" << std::endl;
        std::cout << "Num entries: " << builder.NumEntries() << std::endl;
        std::cout << "File size: " << builder.FileSize() << std::endl;
    }
    
    // Check file exists
    struct stat st;
    if (stat(sst_file.c_str(), &st) == 0) {
        std::cout << "\nFile exists, size: " << st.st_size << std::endl;
    } else {
        std::cout << "\nFILE NOT CREATED!" << std::endl;
        return 1;
    }
    
    // Read back
    std::cout << "\nOpening SSTable for read..." << std::endl;
    std::unique_ptr<SSTableReader> reader;
    Status s = SSTableReader::Open(sst_file, &reader);
    if (!s.ok()) {
        std::cout << "Failed to open SSTable: " << s.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "\nSSTable metadata:" << std::endl;
    std::cout << "  Smallest: [" << reader->SmallestKey() << "]" << std::endl;
    std::cout << "  Largest: [" << reader->LargestKey() << "]" << std::endl;
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
            std::cout << "NOT FOUND (" << s.ToString() << ")" << std::endl;
        }
    }
    
    return 0;
}
