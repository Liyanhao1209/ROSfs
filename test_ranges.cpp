#include <iostream>
#include <memory>
#include <string>
#include "include/tskv/sstable.h"

using namespace tskv;

int main() {
    // Check what the actual min/max are for each SST
    std::vector<std::pair<int, int>> ranges = {{0, 10}, {10, 20}, {20, 30}};
    
    for (int r = 0; r < 3; r++) {
        std::string file = "/tmp/range_sst" + std::to_string(r) + ".sst";
        
        // Create SSTable
        SSTableBuilder builder(file);
        for (int i = ranges[r].first; i < ranges[r].second; i++) {
            std::string key = "key" + std::to_string(i);
            builder.Add(Slice(key), Slice("v" + std::to_string(i)));
        }
        builder.Finish();
        
        std::cout << "Range " << ranges[r].first << "-" << (ranges[r].second-1) << ":" << std::endl;
        std::cout << "  Smallest: " << builder.SmallestKey() << std::endl;
        std::cout << "  Largest: " << builder.LargestKey() << std::endl;
        
        // Read it back
        std::unique_ptr<SSTableReader> reader;
        SSTableReader::Open(file, &reader);
        std::cout << "  Read smallest: " << reader->SmallestKey() << std::endl;
        std::cout << "  Read largest: " << reader->LargestKey() << std::endl;
        std::cout << std::endl;
    }
    
    return 0;
}
