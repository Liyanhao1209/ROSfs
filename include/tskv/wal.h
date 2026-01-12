// Copyright (c) 2026 TSKV Authors. All rights reserved.
// Write-Ahead Log (WAL) implementation

#pragma once

#include <cstdint>
#include <fstream>
#include <mutex>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <iostream>

#include "tskv/coding.h"
#include "tskv/slice.h"
#include "tskv/status.h"
#include "tskv/simd.h"

namespace tskv {

// WAL record format:
// | CRC (4 bytes) | Length (2 bytes) | Type (1 byte) | Payload |
// Type: kFullType, kFirstType, kMiddleType, kLastType

enum RecordType : uint8_t {
    kZeroType = 0,    // Padding
    kFullType = 1,    // Full record in one fragment
    kFirstType = 2,   // First fragment of a record
    kMiddleType = 3,  // Middle fragment of a record
    kLastType = 4,    // Last fragment of a record
};

constexpr int kWALBlockSize = 32768;  // 32KB blocks
constexpr int kWALHeaderSize = 7;     // CRC(4) + Length(2) + Type(1)

// Optimized CRC32
inline uint32_t CRC32(const char* data, size_t n) {
    // Initial value for CRC32C is usually 0xFFFFFFFF, but we can verify what RocksDB does
    // For now using the HW implementation with standard init
    return ~crc32c_hw(0xFFFFFFFF, data, n);
}

class WALWriter {
public:
    explicit WALWriter(const std::string& filename, bool sync = false);
    ~WALWriter();

    // Add a record to the WAL
    Status AddRecord(const Slice& data);

    // Sync the WAL to disk
    Status Sync();

    // Close the WAL
    Status Close();

    // Get current file size
    uint64_t FileSize() const { return file_size_; }

private:
    Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

    int fd_ = -1;
    std::string filename_;
    bool sync_on_write_;
    uint64_t file_size_ = 0;
    int block_offset_ = 0;  // Current offset within block

    // Optimized Buffer: 4MB raw buffer, aligned
    static constexpr size_t kBufferSize = 4 * 1024 * 1024;
    char* buffer_ = nullptr;
    size_t buffer_offset_ = 0;
};

#include <iostream>
#include <cstring>

inline WALWriter::WALWriter(const std::string& filename, bool sync)
    : filename_(filename), sync_on_write_(sync) {
    // Open with O_CREAT | O_TRUNC | O_WRONLY
    // Could add O_DIRECT here if we are confident in alignment
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    fd_ = open(filename.c_str(), flags, 0644);
    if (fd_ < 0) {
         std::cerr << "WALWriter: Failed to open " << filename << ": " << std::strerror(errno) << std::endl;
    }
    
    // Allocate aligned memory for potential O_DIRECT usage in future
    posix_memalign(reinterpret_cast<void**>(&buffer_), 4096, kBufferSize);
    
    // Pre-allocate file space (1GB hint) or let it grow. 
    // fallocate reduces metadata updates
    if (fd_ >= 0) {
        // Not all filesystems support fallocate, so we ignore error
        // fallocate(fd_, 0, 0, 64 * 1024 * 1024); 
    }
}

inline WALWriter::~WALWriter() {
    Close();
    if (buffer_) free(buffer_);
}

inline Status WALWriter::AddRecord(const Slice& data) {
    const char* ptr = data.data();
    size_t left = data.size();
    bool begin = true;

    do {
        const int leftover = kWALBlockSize - block_offset_;
        if (leftover < kWALHeaderSize) {
            // Switch to a new block
            if (leftover > 0) {
                // Fill trailer with zeros
                if (buffer_offset_ + leftover <= kBufferSize) {
                    std::memset(buffer_ + buffer_offset_, 0, leftover);
                    buffer_offset_ += leftover;
                }
            }
            block_offset_ = 0;
        }

        const size_t avail = kWALBlockSize - block_offset_ - kWALHeaderSize;
        const size_t fragment_length = (left < avail) ? left : avail;

        RecordType type;
        const bool end = (left == fragment_length);
        if (begin && end) {
            type = kFullType;
        } else if (begin) {
            type = kFirstType;
        } else if (end) {
            type = kLastType;
        } else {
            type = kMiddleType;
        }

        Status s = EmitPhysicalRecord(type, ptr, fragment_length);
        if (!s.ok()) {
            return s;
        }
        ptr += fragment_length;
        left -= fragment_length;
        begin = false;
    } while (left > 0);

    // Flush buffer if it's getting large
    if (buffer_offset_ >= kBufferSize) {
        // In case our logic allows buffer overflow (shouldn't if checked above)
        // Check if we need to flush partial
        Status s = Sync();
        if (!s.ok()) return s;
    }

    return Status::OK();
}

inline Status WALWriter::EmitPhysicalRecord(RecordType type, const char* ptr, size_t length) {
    // Format: CRC (4) + Length (2) + Type (1) + Payload
    
    // Check if we have space in buffer for this record segment
    // Total size = Header(7) + length
    size_t total_needed = kWALHeaderSize + length;
    
    if (buffer_offset_ + total_needed > kBufferSize) {
        Status s = Sync(); // Flush current buffer
        if (!s.ok()) return s;
    }

    char* header = buffer_ + buffer_offset_;
    
    header[4] = static_cast<char>(length & 0xff);
    header[5] = static_cast<char>(length >> 8);
    header[6] = static_cast<char>(type);

    // Compute CRC over type and payload
    uint32_t crc = crc32c_hw(0xFFFFFFFF, &header[6], 1);  // Type byte
    if (length > 0) {
        crc = crc32c_hw(crc, ptr, length);        // Payload
    }
    crc = ~crc; // Final XOR
    
    EncodeFixed32(header, crc);
    
    // Use AVX memcpy for payload
    avx_memcpy(header + kWALHeaderSize, ptr, length);
    
    buffer_offset_ += total_needed;
    block_offset_ += total_needed;

    return Status::OK();
}

inline Status WALWriter::Sync() {
    if (buffer_offset_ > 0) {
        if (fd_ < 0) return Status::IOError("WAL file not open");
        
        char* p = buffer_;
        size_t left = buffer_offset_;
        while (left > 0) {
            ssize_t written = write(fd_, p, left);
            if (written < 0) {
                 if (errno == EINTR) continue;
                 return Status::IOError(std::string("WAL write failed: ") + std::strerror(errno));
            }
            p += written;
            left -= written;
        }
        
        file_size_ += buffer_offset_;
        buffer_offset_ = 0;
    }
    
    if (sync_on_write_) {
        if (fsync(fd_) != 0) {
            return Status::IOError("WAL fsync failed");
        }
    }
    return Status::OK();
}

inline Status WALWriter::Close() {
    if (fd_ >= 0) {
        Sync();
        close(fd_);
        fd_ = -1;
    }
    return Status::OK();
}

class WALReader {
public:
    explicit WALReader(const std::string& filename);
    ~WALReader();

    // Read the next record
    Status ReadRecord(std::string* record);

    // Check if there are more records
    bool HasMore() const { return !eof_; }

private:
    int fd_ = -1;
    std::string filename_;
    std::string buffer_;
    size_t buffer_offset_ = 0;
    size_t buffer_size_ = 0;
    bool eof_ = false;
    std::string scratch_;  // For assembling multi-fragment records

    Status ReadPhysicalRecord(Slice* result, RecordType* type);
    Status RefillBuffer();
};

inline WALReader::WALReader(const std::string& filename) : filename_(filename) {
    fd_ = open(filename.c_str(), O_RDONLY);
    if (fd_ >= 0) {
        buffer_.resize(kWALBlockSize);
    }
}

inline WALReader::~WALReader() {
    if (fd_ >= 0) {
        close(fd_);
    }
}

inline Status WALReader::RefillBuffer() {
    ssize_t n = read(fd_, &buffer_[0], kWALBlockSize);
    if (n < 0) {
        return Status::IOError("WAL read failed");
    }
    if (n == 0) {
        eof_ = true;
        return Status::OK();
    }
    buffer_offset_ = 0;
    buffer_size_ = n;
    return Status::OK();
}

inline Status WALReader::ReadPhysicalRecord(Slice* result, RecordType* type) {
    while (true) {
        if (buffer_size_ - buffer_offset_ < kWALHeaderSize) {
            Status s = RefillBuffer();
            if (!s.ok()) return s;
            if (eof_) return Status::OK();
            continue;
        }

        const char* header = buffer_.data() + buffer_offset_;
        uint32_t length = static_cast<uint32_t>(static_cast<unsigned char>(header[4])) |
                          (static_cast<uint32_t>(static_cast<unsigned char>(header[5])) << 8);
        *type = static_cast<RecordType>(header[6]);

        if (kWALHeaderSize + length > buffer_size_ - buffer_offset_) {
            Status s = RefillBuffer();
            if (!s.ok()) return s;
            if (eof_) return Status::Corruption("Truncated record");
            continue;
        }

        if (*type == kZeroType && length == 0) {
            // Padding, skip
            buffer_offset_ = 0;
            buffer_size_ = 0;
            continue;
        }

        *result = Slice(header + kWALHeaderSize, length);
        buffer_offset_ += kWALHeaderSize + length;
        return Status::OK();
    }
}

inline Status WALReader::ReadRecord(std::string* record) {
    if (fd_ < 0) {
        return Status::IOError("WAL file not open");
    }

    scratch_.clear();
    record->clear();
    bool in_fragmented_record = false;

    while (true) {
        Slice fragment;
        RecordType type;
        Status s = ReadPhysicalRecord(&fragment, &type);
        if (!s.ok()) {
            return s;
        }
        if (eof_) {
            if (in_fragmented_record) {
                return Status::Corruption("Partial record at end of file");
            }
            return Status::NotFound();  // End of file
        }

        switch (type) {
            case kFullType:
                if (in_fragmented_record) {
                    return Status::Corruption("Partial record without end");
                }
                record->assign(fragment.data(), fragment.size());
                return Status::OK();

            case kFirstType:
                if (in_fragmented_record) {
                    return Status::Corruption("Partial record without end");
                }
                scratch_.assign(fragment.data(), fragment.size());
                in_fragmented_record = true;
                break;

            case kMiddleType:
                if (!in_fragmented_record) {
                    return Status::Corruption("Missing start of record");
                }
                scratch_.append(fragment.data(), fragment.size());
                break;

            case kLastType:
                if (!in_fragmented_record) {
                    return Status::Corruption("Missing start of record");
                }
                scratch_.append(fragment.data(), fragment.size());
                *record = std::move(scratch_);
                return Status::OK();

            default:
                return Status::Corruption("Unknown record type");
        }
    }
}

}  // namespace tskv
