#include <iostream>
#include <filesystem>
#include "include/tskv/tskv.h"

using namespace tskv;
namespace fs = std::filesystem;

int main() {
    std::string db_path = "/tmp/test_monotonic";
    
    for (const auto& entry : fs::directory_iterator(db_path)) {
        if (entry.path().extension() == ".sst") {
            std::unique_ptr<SSTableReader> reader;
            Status s = SSTableReader::Open(entry.path().string(), &reader);
            if (s.ok()) {
                std::cout << entry.path().filename() << ": "
                          << "[" << reader->SmallestKey() << ", " << reader->LargestKey() << "]"
                          << std::endl;
            }
        }
    }
    return 0;
}
