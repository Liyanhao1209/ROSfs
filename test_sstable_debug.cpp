#include <iostream>
#include <memory>
#include <string>
#include "include/tskv/tskv.h"

using namespace tskv;

int main() {
    // Test direct SSTable write and read
    std::string filename = "/tmp/test_debug.sst";
    
    // Write some keys
    {
        SSTableBuilder builder(filename);
        for (int i = 0; i < 10; i++) {
            std::string key = "key" + std::to_string(i);
            std::string value = "value" + std::to_string(i);
            builder.Add(key, value);
            std::cout << "Added: " << key << std::endl;
        }
        Status s = builder.Finish();
        if (!s.ok()) {
            std::cout << "Build failed: " << s.ToString() << std::endl;
            return 1;
        }
        std::cout << "SSTable built. smallest=" << builder.SmallestKey() 
                  << " largest=" << builder.LargestKey() << std::endl;
    }
    
    // Read back
    std::unique_ptr<SSTableReader> reader;
    Status s = SSTableReader::Open(filename, &reader);
    if (!s.ok()) {
        std::cout << "Open failed: " << s.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "Opened SSTable. smallest=" << reader->SmallestKey() 
              << " largest=" << reader->LargestKey() << std::endl;
    
    // Test each key
    int found = 0;
    for (int i = 0; i < 10; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value;
        Status s = reader->Get(key, &value);
        if (s.ok()) {
            std::cout << "Found " << key << " = " << value << std::endl;
            found++;
        } else {
            std::cout << "NOT FOUND: " << key << std::endl;
        }
    }
    std::cout << "Found " << found << "/10 keys" << std::endl;
    
    return 0;
}
