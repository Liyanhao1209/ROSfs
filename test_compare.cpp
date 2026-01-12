#include <iostream>
#include <string>
#include "include/tskv/slice.h"

using namespace tskv;

int main() {
    // Test string comparisons
    std::string key9 = "key9";
    std::string key10 = "key10";
    std::string key19 = "key19";
    std::string key20 = "key20";
    
    std::cout << "String comparisons:" << std::endl;
    std::cout << "  key9 < key10: " << (key9 < key10) << " (expected 1)" << std::endl;
    std::cout << "  key19 < key20: " << (key19 < key20) << " (expected 1)" << std::endl;
    
    // Lexicographic string comparison: "key10" < "key9" because '1' < '9'
    std::cout << "\nActual lexicographic order:" << std::endl;
    std::cout << "  'key10' < 'key9' (lexicographically): " << (std::string("key10") < std::string("key9")) << std::endl;
    
    // This is the problem! Lexicographic: key0 < key1 < key10 < key19 < key2 < key20 < key29 < key3 < ...
    
    std::cout << "\nSlice comparison:" << std::endl;
    std::cout << "  key9.compare(key10): " << Slice(key9).compare(Slice(key10)) << std::endl;
    std::cout << "  key10 <= key9: " << (Slice(key10) <= Slice(key9)) << std::endl;
    
    return 0;
}
