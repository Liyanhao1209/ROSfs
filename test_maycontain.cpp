#include <iostream>
#include "include/tskv/slice.h"

using namespace tskv;

int main() {
    // Check lexicographic order
    std::cout << "Lexicographic comparisons:" << std::endl;
    std::cout << "  key10 < key9: " << (Slice("key10") < Slice("key9")) << std::endl;
    std::cout << "  key10 > key0: " << (Slice("key10") > Slice("key0")) << std::endl;
    std::cout << "  key10 <= key9: " << (Slice("key10") <= Slice("key9")) << std::endl;
    
    // So key10 IS in range [key0, key9] lexicographically!
    // This is expected behavior.
    
    // The problem is that my test data has keys that don't sort numerically.
    // For time-series data (the target use case), keys would typically be:
    // - timestamps (numeric)
    // - or zero-padded strings like "key00", "key01", "key10"
    
    // Let's test with proper zero-padded keys
    std::cout << "\nWith zero-padded keys:" << std::endl;
    std::cout << "  key00 < key01: " << (Slice("key00") < Slice("key01")) << std::endl;
    std::cout << "  key09 < key10: " << (Slice("key09") < Slice("key10")) << std::endl;
    std::cout << "  key10 in [key00, key09]: " << ((Slice("key10") >= Slice("key00")) && (Slice("key10") <= Slice("key09"))) << std::endl;
    
    return 0;
}
