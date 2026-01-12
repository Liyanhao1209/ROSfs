#include <iostream>
#include <memory>
#include <string>
#include "include/tskv/sstable_index.h"
#include "include/tskv/sstable.h"

using namespace tskv;

int main() {
    // Create several SSTables with different key ranges
    std::vector<std::string> files;
    
    // SSTable 1: key0-key9
    {
        std::string file = "/tmp/sst1.sst";
        files.push_back(file);
        SSTableBuilder builder(file);
        for (int i = 0; i < 10; i++) {
            std::string key = "key" + std::to_string(i);
            std::string value = "v1_" + std::to_string(i);
            builder.Add(Slice(key), Slice(value));
        }
        builder.Finish();
        std::cout << "SST1: key0-key9" << std::endl;
    }
    
    // SSTable 2: key10-key19
    {
        std::string file = "/tmp/sst2.sst";
        files.push_back(file);
        SSTableBuilder builder(file);
        for (int i = 10; i < 20; i++) {
            std::string key = "key" + std::to_string(i);
            std::string value = "v2_" + std::to_string(i);
            builder.Add(Slice(key), Slice(value));
        }
        builder.Finish();
        std::cout << "SST2: key10-key19" << std::endl;
    }
    
    // SSTable 3: key20-key29
    {
        std::string file = "/tmp/sst3.sst";
        files.push_back(file);
        SSTableBuilder builder(file);
        for (int i = 20; i < 30; i++) {
            std::string key = "key" + std::to_string(i);
            std::string value = "v3_" + std::to_string(i);
            builder.Add(Slice(key), Slice(value));
        }
        builder.Finish();
        std::cout << "SST3: key20-key29" << std::endl;
    }
    
    // Create index and add SSTables
    SSTableIndex<> index;
    
    for (size_t i = 0; i < files.size(); i++) {
        std::unique_ptr<SSTableReader> reader;
        Status s = SSTableReader::Open(files[i], &reader);
        if (!s.ok()) {
            std::cout << "Failed to open " << files[i] << ": " << s.ToString() << std::endl;
            continue;
        }
        reader->SetFileNumber(i + 1);
        std::cout << "Adding SST " << (i+1) << " to index: [" 
                  << reader->SmallestKey() << " - " << reader->LargestKey() << "]" << std::endl;
        index.Add(std::shared_ptr<SSTableReader>(reader.release()), i + 1, i + 1);
    }
    
    std::cout << "\nIndex has " << index.Count() << " SSTables" << std::endl;
    
    // Test FindOne for various keys
    std::cout << "\nFindOne tests:" << std::endl;
    for (int i = 0; i < 30; i++) {
        std::string key = "key" + std::to_string(i);
        auto reader = index.FindOne(Slice(key));
        std::cout << "  " << key << " => ";
        if (reader) {
            std::string value;
            Status s = reader->Get(Slice(key), &value);
            if (s.ok()) {
                std::cout << value << std::endl;
            } else {
                std::cout << "FOUND IN SST BUT GET FAILED!" << std::endl;
            }
        } else {
            std::cout << "NOT FOUND IN INDEX" << std::endl;
        }
    }
    
    return 0;
}
