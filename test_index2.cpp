#include <iostream>
#include <memory>
#include <string>
#include "include/tskv/sstable_index.h"
#include "include/tskv/sstable.h"

using namespace tskv;

int main() {
    // Create SSTables with proper lexicographic order keys
    // Lexicographic order: key0 < key1 < key10 < key19 < key2 < key20 < key29 < key3 < key9
    
    std::cout << "Lexicographic order of test keys:" << std::endl;
    std::vector<std::string> all_keys;
    for (int i = 0; i < 30; i++) {
        all_keys.push_back("key" + std::to_string(i));
    }
    std::sort(all_keys.begin(), all_keys.end());
    for (const auto& k : all_keys) {
        std::cout << "  " << k << std::endl;
    }
    
    std::cout << "\n\nCreating SSTables with proper ranges:" << std::endl;
    
    // SST1: key0-key9 lexicographic range
    std::string sst1_file = "/tmp/idx_sst1.sst";
    {
        SSTableBuilder builder(sst1_file);
        for (int i = 0; i < 10; i++) {
            std::string key = "key" + std::to_string(i);
            builder.Add(Slice(key), Slice("v1_" + std::to_string(i)));
        }
        builder.Finish();
        std::cout << "SST1 smallest: " << builder.SmallestKey() << ", largest: " << builder.LargestKey() << std::endl;
    }
    
    // Open and add to index
    SSTableIndex<> index;
    
    std::unique_ptr<SSTableReader> r1;
    SSTableReader::Open(sst1_file, &r1);
    r1->SetFileNumber(1);
    std::cout << "SST1 MayContain('key10'): " << r1->MayContain(Slice("key10")) << std::endl;
    std::cout << "SST1 MayContain('key15'): " << r1->MayContain(Slice("key15")) << std::endl;
    
    // The problem: SST1 has keys key0-key9, and lexicographically:
    // key0 < key1 < key10 < ... < key19 < key2 < key20 < ... < key29 < key3 < ... < key9
    // So "key10" is BETWEEN "key1" and "key2" lexicographically
    // MayContain returns true for key10 because key10 is between key0 and key9!
    
    std::cout << "\nSlice comparison:" << std::endl;
    std::cout << "  key0 <= key10 <= key9: " 
              << (Slice("key0") <= Slice("key10")) << " && " 
              << (Slice("key10") <= Slice("key9")) << std::endl;
    
    // The keys within SST1 in sorted order would be:
    // key0 < key1 < key2 < key3 < key4 < key5 < key6 < key7 < key8 < key9
    // No wait - SSTableBuilder adds them in the order we provide!
    
    std::cout << "\nWhen we add key0,key1,...,key9 in that order:" << std::endl;
    std::cout << "  smallest_key = key0" << std::endl;
    std::cout << "  largest_key = key9" << std::endl;
    std::cout << "  MayContain checks: key >= key0 && key <= key9" << std::endl;
    std::cout << "  For key10: key10 >= key0 = " << (Slice("key10") >= Slice("key0")) << std::endl;
    std::cout << "  For key10: key10 <= key9 = " << (Slice("key10") <= Slice("key9")) << std::endl;
    std::cout << "  Both true, so MayContain returns true!" << std::endl;
    
    return 0;
}
