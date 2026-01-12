// Copyright (c) 2026 TSKV Authors. All rights reserved.
// Encoding utilities for variable-length integers

#pragma once

#include <cstdint>
#include <cstring>
#include <string>

namespace tskv {

// Maximum bytes for varint encoding
constexpr int kMaxVarint32Length = 5;
constexpr int kMaxVarint64Length = 10;

// Encode 32-bit value to buffer, return pointer past the encoded value
inline char* EncodeVarint32(char* dst, uint32_t value) {
    unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
    static const int B = 128;
    if (value < (1 << 7)) {
        *(ptr++) = value;
    } else if (value < (1 << 14)) {
        *(ptr++) = value | B;
        *(ptr++) = value >> 7;
    } else if (value < (1 << 21)) {
        *(ptr++) = value | B;
        *(ptr++) = (value >> 7) | B;
        *(ptr++) = value >> 14;
    } else if (value < (1 << 28)) {
        *(ptr++) = value | B;
        *(ptr++) = (value >> 7) | B;
        *(ptr++) = (value >> 14) | B;
        *(ptr++) = value >> 21;
    } else {
        *(ptr++) = value | B;
        *(ptr++) = (value >> 7) | B;
        *(ptr++) = (value >> 14) | B;
        *(ptr++) = (value >> 21) | B;
        *(ptr++) = value >> 28;
    }
    return reinterpret_cast<char*>(ptr);
}

// Decode 32-bit value, return pointer past the decoded value
inline const char* GetVarint32Ptr(const char* p, const char* limit, uint32_t* value) {
    if (p < limit) {
        uint32_t result = *(reinterpret_cast<const unsigned char*>(p));
        if ((result & 128) == 0) {
            *value = result;
            return p + 1;
        }
    }
    
    uint32_t result = 0;
    for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
        uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
        p++;
        if (byte & 128) {
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return p;
        }
    }
    return nullptr;
}

// Encode 64-bit value to buffer
inline char* EncodeVarint64(char* dst, uint64_t value) {
    unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
    static const int B = 128;
    while (value >= B) {
        *(ptr++) = (value & (B - 1)) | B;
        value >>= 7;
    }
    *(ptr++) = static_cast<unsigned char>(value);
    return reinterpret_cast<char*>(ptr);
}

// Decode 64-bit value
inline const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
    uint64_t result = 0;
    for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
        uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
        p++;
        if (byte & 128) {
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return p;
        }
    }
    return nullptr;
}

// Fixed-length encoding
inline void EncodeFixed32(char* dst, uint32_t value) {
    memcpy(dst, &value, sizeof(value));
}

inline void EncodeFixed64(char* dst, uint64_t value) {
    memcpy(dst, &value, sizeof(value));
}

inline uint32_t DecodeFixed32(const char* ptr) {
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));
    return result;
}

inline uint64_t DecodeFixed64(const char* ptr) {
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));
    return result;
}

// Append encoding functions
inline void PutVarint32(std::string* dst, uint32_t value) {
    char buf[kMaxVarint32Length];
    char* ptr = EncodeVarint32(buf, value);
    dst->append(buf, ptr - buf);
}

inline void PutVarint64(std::string* dst, uint64_t value) {
    char buf[kMaxVarint64Length];
    char* ptr = EncodeVarint64(buf, value);
    dst->append(buf, ptr - buf);
}

inline void PutFixed32(std::string* dst, uint32_t value) {
    char buf[sizeof(value)];
    EncodeFixed32(buf, value);
    dst->append(buf, sizeof(buf));
}

inline void PutFixed64(std::string* dst, uint64_t value) {
    char buf[sizeof(value)];
    EncodeFixed64(buf, value);
    dst->append(buf, sizeof(buf));
}

// Length-prefixed slice
inline void PutLengthPrefixedSlice(std::string* dst, const char* data, size_t size) {
    PutVarint32(dst, static_cast<uint32_t>(size));
    dst->append(data, size);
}

// Varint length
inline int VarintLength(uint64_t value) {
    int len = 1;
    while (value >= 128) {
        value >>= 7;
        len++;
    }
    return len;
}

}  // namespace tskv
