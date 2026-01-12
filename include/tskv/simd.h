// Copyright (c) 2026 TSKV Authors. All rights reserved.
// SIMD optimizations for TSKV (AVX/SSE)

#pragma once

#include <cstdint>
#include <cstring>
#include <x86intrin.h>

namespace tskv {

// SSE4.2 Hardware CRC32C
inline uint32_t crc32c_hw(uint32_t crc, const char* buf, size_t len) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(buf);
    
    // Process unaligned bytes
    while (len > 0 && (reinterpret_cast<uintptr_t>(p) & 7)) {
        crc = _mm_crc32_u8(crc, *p++);
        len--;
    }

    // Process 8 bytes at a time
    while (len >= 8) {
        crc = _mm_crc32_u64(crc, *reinterpret_cast<const uint64_t*>(p));
        p += 8;
        len -= 8;
    }

    // Process remaining bytes
    while (len > 0) {
        crc = _mm_crc32_u8(crc, *p++);
        len--;
    }
    
    return crc;
}

// AVX optimized memcpy
// Uses AVX registers (256-bit / 32 bytes) for copying
inline void* avx_memcpy(void* dest, const void* src, size_t count) {
    uint8_t* d = static_cast<uint8_t*>(dest);
    const uint8_t* s = static_cast<const uint8_t*>(src);
    size_t n = count;

    // Small copies: fallback to standard memcpy which is likely efficient enough
    if (n < 64) {
        return std::memcpy(dest, src, count);
    }

    // Copy prologue to align destination to 32 bytes
    while ((reinterpret_cast<uintptr_t>(d) & 31) && n > 0) {
        *d++ = *s++;
        n--;
    }
    
    // Copy 32-byte blocks using AVX
    while (n >= 32) {
        __m256i data = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(s));
        _mm256_store_si256(reinterpret_cast<__m256i*>(d), data); // Aligned store
        s += 32;
        d += 32;
        n -= 32;
    }

    // Copy remaining bytes
    while (n > 0) {
        *d++ = *s++;
        n--;
    }

    return dest;
}

} // namespace tskv
