#include <iostream>
#include <iomanip>
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

void PrintKeyHex(const std::string& key) {
    for (unsigned char c : key) {
        std::cout << std::hex << std::setw(2) << std::setfill('0') << (int)c << " ";
    }
    std::cout << std::dec;
}

int main() {
    // Open SSTable directly
    std::unique_ptr<SSTableReader> reader;
    Status s = SSTableReader::Open("/tmp/test_binary_key/000001.sst", &reader);
    if (!s.ok()) {
        std::cout << "Open failed: " << s.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "SSTable opened" << std::endl;
    std::cout << "  smallest key: "; PrintKeyHex(reader->SmallestKey()); std::cout << std::endl;
    std::cout << "  largest key:  "; PrintKeyHex(reader->LargestKey()); std::cout << std::endl;
    std::cout << "  num_entries: " << reader->NumEntries() << std::endl;
    
    // Try to get a key
    double base_ts = 1317042193.123456;
    uint32_t topic_hash = 12345;
    std::string key = MakeBinaryKey(base_ts, topic_hash);
    
    std::cout << "\nLooking for key: "; PrintKeyHex(key); std::cout << std::endl;
    
    // Check MayContain
    std::cout << "  MayContain: " << (reader->MayContain(key) ? "true" : "false") << std::endl;
    
    // Try Get
    std::string value;
    s = reader->Get(key, &value);
    std::cout << "  Get result: " << (s.ok() ? "found" : s.ToString()) << std::endl;
    
    return 0;
}
