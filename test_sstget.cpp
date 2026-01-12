#include <iostream>
#include <memory>
#include <string>
#include "include/tskv/sstable.h"

using namespace tskv;

int main() {
    // Create SSTable with key10-key19
    std::string file = "/tmp/test_sstget.sst";
    {
        SSTableBuilder builder(file);
        for (int i = 10; i < 20; i++) {
            std::string key = "key" + std::to_string(i);
            std::string value = "v_" + std::to_string(i);
            builder.Add(Slice(key), Slice(value));
        }
        builder.Finish();
    }
    
    std::unique_ptr<SSTableReader> reader;
    Status s = SSTableReader::Open(file, &reader);
    if (!s.ok()) {
        std::cout << "Open failed: " << s.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "Smallest: " << reader->SmallestKey() << std::endl;
    std::cout << "Largest: " << reader->LargestKey() << std::endl;
    
    // In lexicographic order, the keys are:
    // key10 < key11 < key12 < ... < key19
    // All less than key2!
    
    // MayContain("key10")
    std::cout << "\nMayContain tests:" << std::endl;
    std::cout << "  key10: " << reader->MayContain(Slice("key10")) << std::endl;
    std::cout << "  key15: " << reader->MayContain(Slice("key15")) << std::endl;
    std::cout << "  key19: " << reader->MayContain(Slice("key19")) << std::endl;
    std::cout << "  key2: " << reader->MayContain(Slice("key2")) << std::endl;  // key2 > key19 lexicographically!
    
    // Direct Get tests
    std::cout << "\nGet tests:" << std::endl;
    for (int i = 10; i < 20; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value;
        s = reader->Get(Slice(key), &value);
        std::cout << "  " << key << ": ";
        if (s.ok()) {
            std::cout << value << std::endl;
        } else {
            std::cout << s.ToString() << std::endl;
        }
    }
    
    return 0;
}
