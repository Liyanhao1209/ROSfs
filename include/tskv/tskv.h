// Copyright (c) 2026 TSKV Authors. All rights reserved.
// TSKV - Time-Series Key-Value Storage Engine
// 
// A high-performance storage engine optimized for time-series robotic data.
// Key features:
// - No compaction overhead (append-only storage)
// - Skip list index for fast SSTable lookup
// - Memory-mapped I/O for low-latency reads
// - Optimized for sequential/time-series writes

#pragma once

#include "tskv/arena.h"
#include "tskv/coding.h"
#include "tskv/db.h"
#include "tskv/memtable.h"
#include "tskv/options.h"
#include "tskv/skiplist.h"
#include "tskv/slice.h"
#include "tskv/sstable.h"
#include "tskv/sstable_index.h"
#include "tskv/status.h"
#include "tskv/wal.h"

namespace tskv {

// Version information
constexpr int kMajorVersion = 1;
constexpr int kMinorVersion = 0;
constexpr int kPatchVersion = 0;

inline std::string Version() {
    return std::to_string(kMajorVersion) + "." +
           std::to_string(kMinorVersion) + "." +
           std::to_string(kPatchVersion);
}

}  // namespace tskv
