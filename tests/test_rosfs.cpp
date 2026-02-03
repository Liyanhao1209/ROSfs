#include <gtest/gtest.h>
#include <rosfs/rosfs.hpp>
#include <filesystem>
#include <vector>
#include <numeric>
#include <random>
#include <algorithm>
#include <cstring> 

namespace fs = std::filesystem;

// Test Fixture: 负责每个测试前后的环境准备与清理
class ROSfsTest : public ::testing::Test {
protected:
    std::string db_path;

    void SetUp() override {
        // 使用时间戳和随机数避免目录冲突
        db_path = "./test_rosfs_db_" + std::to_string(std::time(nullptr)) + 
                  "_" + std::to_string(rand() % 10000);
        if (fs::exists(db_path)) {
            fs::remove_all(db_path);
        }
    }

    void TearDown() override {
        if (fs::exists(db_path)) {
            fs::remove_all(db_path);
        }
    }

    // 辅助：生成指定大小和内容的 payload
    std::vector<uint8_t> make_payload(size_t size, uint8_t fill_val) {
        std::vector<uint8_t> data(size);
        std::fill(data.begin(), data.end(), fill_val);
        return data;
    }
    
    // 辅助：生成带有可验证模式的 payload
    std::vector<uint8_t> make_verifiable_payload(uint64_t ts, size_t extra_size = 0) {
        std::vector<uint8_t> data(sizeof(uint64_t) + extra_size);
        std::memcpy(data.data(), &ts, sizeof(uint64_t));
        for (size_t i = sizeof(uint64_t); i < data.size(); ++i) {
            data[i] = (uint8_t)(ts + i);
        }
        return data;
    }
    
    // 验证 payload 内容
    bool verify_payload(const std::vector<uint8_t>& data, uint64_t expected_ts) {
        if (data.size() < sizeof(uint64_t)) return false;
        uint64_t stored_ts;
        std::memcpy(&stored_ts, data.data(), sizeof(uint64_t));
        if (stored_ts != expected_ts) return false;
        for (size_t i = sizeof(uint64_t); i < data.size(); ++i) {
            if (data[i] != (uint8_t)(expected_ts + i)) return false;
        }
        return true;
    }
};

// ============================================================================
// 基础功能测试
// ============================================================================

// 1. 基础写入测试
TEST_F(ROSfsTest, BasicAppendAndStats) {
    rosfs::ROSfsDB db;
    ASSERT_NO_THROW(db.open(db_path, false)); // Write mode

    db.append(100, make_payload(10, 0xA));
    db.append(200, make_payload(20, 0xB));

    auto stats = db.get_stats();
    EXPECT_EQ(stats.total_records, 2);
    EXPECT_GT(stats.data_file_size, 0);
    EXPECT_GT(stats.index_file_size, 0);

    db.close();
}

// 2. 持久化与数据校验测试
TEST_F(ROSfsTest, PersistenceCheck) {
    // Step 1: Write
    {
        rosfs::ROSfsDB db;
        db.open(db_path, false);
        for (int i = 0; i < 10; ++i) {
            db.append(1000 + i * 10, make_verifiable_payload(1000 + i * 10, 24));
        }
        db.close();
    }

    // Step 2: Read & Verify
    {
        rosfs::ROSfsDB db;
        ASSERT_NO_THROW(db.open(db_path, true)); // Read mode

        auto stats = db.get_stats();
        EXPECT_EQ(stats.total_records, 10);

        int count = 0;
        db.range_query(0, 2000, [&](const rosfs::Record& rec) {
            uint64_t expected_ts = 1000 + count * 10;
            EXPECT_EQ(rec.timestamp, expected_ts);
            EXPECT_TRUE(verify_payload(rec.data, expected_ts));
            count++;
            return true;
        });
        EXPECT_EQ(count, 10);
    }
}

// 3. 范围查询边界测试
TEST_F(ROSfsTest, RangeQueryBoundaries) {
    rosfs::ROSfsDB db;
    db.open(db_path, false);
    // Ts: 100, 200, 300, 400, 500
    for (int i = 1; i <= 5; ++i) {
        db.append(i * 100, make_payload(1, 0xFF));
    }
    db.close();

    db.open(db_path, true);

    // Case A: Exact Match Single
    int count = 0;
    db.range_query(200, 200, [&](const rosfs::Record& r) {
        EXPECT_EQ(r.timestamp, 200);
        count++;
        return true;
    });
    EXPECT_EQ(count, 1);

    // Case B: Range Subset [200, 400]
    std::vector<uint64_t> results;
    db.range_query(200, 400, [&](const rosfs::Record& r) {
        results.push_back(r.timestamp);
        return true;
    });
    EXPECT_EQ(results.size(), 3);
    EXPECT_EQ(results[0], 200);
    EXPECT_EQ(results[1], 300);
    EXPECT_EQ(results[2], 400);

    // Case C: Out of range (Left)
    count = 0;
    db.range_query(0, 50, [&](const auto&) { count++; return true; });
    EXPECT_EQ(count, 0);

    // Case D: Out of range (Right)
    count = 0;
    db.range_query(600, 800, [&](const auto&) { count++; return true; });
    EXPECT_EQ(count, 0);
}

// 4. 空数据库测试
TEST_F(ROSfsTest, EmptyDatabase) {
    rosfs::ROSfsDB db;
    db.open(db_path, false);
    db.close();

    db.open(db_path, true);
    auto stats = db.get_stats();
    EXPECT_EQ(stats.total_records, 0);
    
    int count = 0;
    db.range_query(0, 1000, [&](const auto&) { count++; return true; });
    EXPECT_EQ(count, 0);
}

// 5. 只读模式下写入应抛出异常
TEST_F(ROSfsTest, WriteInReadOnlyModeShouldThrow) {
    rosfs::ROSfsDB db;
    db.open(db_path, false);
    db.close();

    db.open(db_path, true); // Read Only
    EXPECT_THROW(db.append(100, {}), std::runtime_error);
}

// ============================================================================
// 大数据量测试
// ============================================================================

// 6. 大量数据写入与查询
TEST_F(ROSfsTest, LargeDataset) {
    constexpr int NUM_RECORDS = 10000;
    
    {
        rosfs::ROSfsDB db;
        db.open(db_path, false);
        for (int i = 0; i < NUM_RECORDS; ++i) {
            db.append(i * 100, make_payload(64, (uint8_t)i));
        }
        db.close();
    }

    {
        rosfs::ROSfsDB db;
        db.open(db_path, true);
        
        auto stats = db.get_stats();
        EXPECT_EQ(stats.total_records, NUM_RECORDS);

        // 范围查询子集
        int count = 0;
        db.range_query(100000, 200000, [&](const rosfs::Record& rec) {
            EXPECT_GE(rec.timestamp, 100000);
            EXPECT_LE(rec.timestamp, 200000);
            count++;
            return true;
        });
        EXPECT_EQ(count, 1001);  // 100000 到 200000 步长 100，共 1001 条
    }
}

// 7. 大 payload 测试
TEST_F(ROSfsTest, LargePayload) {
    constexpr size_t PAYLOAD_SIZE = 1024 * 1024;  // 1MB
    
    rosfs::ROSfsDB db;
    db.open(db_path, false);
    
    auto large_payload = make_payload(PAYLOAD_SIZE, 0xAB);
    db.append(12345, large_payload);
    db.close();

    db.open(db_path, true);
    
    int count = 0;
    db.range_query(12345, 12345, [&](const rosfs::Record& rec) {
        EXPECT_EQ(rec.timestamp, 12345);
        EXPECT_EQ(rec.data.size(), PAYLOAD_SIZE);
        EXPECT_EQ(rec.data[0], 0xAB);
        EXPECT_EQ(rec.data[PAYLOAD_SIZE - 1], 0xAB);
        count++;
        return true;
    });
    EXPECT_EQ(count, 1);
}

// ============================================================================
// B+ 树特性测试
// ============================================================================

// 8. 乱序插入测试 (验证 B+ 树正确维护有序性)
TEST_F(ROSfsTest, OutOfOrderInsert) {
    rosfs::ROSfsDB db;
    db.open(db_path, false);
    
    // 乱序插入
    std::vector<uint64_t> timestamps = {500, 100, 300, 200, 400, 150, 350, 250, 450, 50};
    for (auto ts : timestamps) {
        db.append(ts, make_verifiable_payload(ts));
    }
    db.close();

    db.open(db_path, true);
    
    // 范围查询应返回有序结果
    std::vector<uint64_t> results;
    db.range_query(0, 1000, [&](const rosfs::Record& rec) {
        results.push_back(rec.timestamp);
        EXPECT_TRUE(verify_payload(rec.data, rec.timestamp));
        return true;
    });
    
    EXPECT_EQ(results.size(), timestamps.size());
    
    // 验证结果有序
    for (size_t i = 1; i < results.size(); ++i) {
        EXPECT_LE(results[i - 1], results[i]);
    }
}

// 9. 重复时间戳测试
TEST_F(ROSfsTest, DuplicateTimestamps) {
    rosfs::ROSfsDB db;
    db.open(db_path, false);
    
    // 插入多条相同时间戳的记录
    for (int i = 0; i < 10; ++i) {
        db.append(1000, make_payload(8, (uint8_t)i));
    }
    db.close();

    db.open(db_path, true);
    
    int count = 0;
    db.range_query(1000, 1000, [&](const rosfs::Record& rec) {
        EXPECT_EQ(rec.timestamp, 1000);
        count++;
        return true;
    });
    EXPECT_EQ(count, 10);
}

// 10. B+ 树分裂测试 (大量插入触发多次分裂)
TEST_F(ROSfsTest, BTreeSplitStress) {
    constexpr int NUM_RECORDS = 5000;
    
    rosfs::ROSfsDB db;
    db.open(db_path, false);
    
    // 顺序插入大量数据，触发多次叶节点分裂
    for (int i = 0; i < NUM_RECORDS; ++i) {
        db.append(i, make_payload(16, (uint8_t)i));
    }
    db.close();

    db.open(db_path, true);
    auto stats = db.get_stats();
    EXPECT_EQ(stats.total_records, NUM_RECORDS);

    // 验证全部数据
    int count = 0;
    uint64_t last_ts = 0;
    db.range_query(0, NUM_RECORDS, [&](const rosfs::Record& rec) {
        if (count > 0) {
            EXPECT_GT(rec.timestamp, last_ts);
        }
        last_ts = rec.timestamp;
        count++;
        return true;
    });
    EXPECT_EQ(count, NUM_RECORDS);
}

// ============================================================================
// 边界条件测试
// ============================================================================

// 11. 空 payload 测试
TEST_F(ROSfsTest, EmptyPayload) {
    rosfs::ROSfsDB db;
    db.open(db_path, false);
    
    db.append(100, {});  // 空 payload
    db.close();

    db.open(db_path, true);
    
    int count = 0;
    db.range_query(100, 100, [&](const rosfs::Record& rec) {
        EXPECT_EQ(rec.timestamp, 100);
        EXPECT_TRUE(rec.data.empty());
        count++;
        return true;
    });
    EXPECT_EQ(count, 1);
}

// 12. 最大/最小时间戳测试
TEST_F(ROSfsTest, ExtremeTimestamps) {
    rosfs::ROSfsDB db;
    db.open(db_path, false);
    
    db.append(0, make_payload(8, 0x00));
    db.append(UINT64_MAX, make_payload(8, 0xFF));
    db.append(UINT64_MAX / 2, make_payload(8, 0x80));
    db.close();

    db.open(db_path, true);
    
    auto stats = db.get_stats();
    EXPECT_EQ(stats.total_records, 3);

    // 查询最小值
    int count = 0;
    db.range_query(0, 0, [&](const rosfs::Record& rec) {
        EXPECT_EQ(rec.timestamp, 0);
        count++;
        return true;
    });
    EXPECT_EQ(count, 1);

    // 查询最大值
    count = 0;
    db.range_query(UINT64_MAX, UINT64_MAX, [&](const rosfs::Record& rec) {
        EXPECT_EQ(rec.timestamp, UINT64_MAX);
        count++;
        return true;
    });
    EXPECT_EQ(count, 1);
}

// 13. Callback 中途停止测试
TEST_F(ROSfsTest, EarlyStopCallback) {
    rosfs::ROSfsDB db;
    db.open(db_path, false);
    
    for (int i = 0; i < 100; ++i) {
        db.append(i * 10, make_payload(8, (uint8_t)i));
    }
    db.close();

    db.open(db_path, true);
    
    int count = 0;
    db.range_query(0, 1000, [&](const rosfs::Record&) {
        count++;
        return count < 5;  // 只处理 5 条后停止
    });
    EXPECT_EQ(count, 5);
}

// ============================================================================
// 重新打开测试
// ============================================================================

// 14. 多次打开关闭测试
TEST_F(ROSfsTest, MultipleOpenClose) {
    rosfs::ROSfsDB db;
    
    // 第一轮写入
    db.open(db_path, false);
    for (int i = 0; i < 10; ++i) {
        db.append(i * 10, make_payload(8, 0xAA));
    }
    db.close();
    
    // 追加写入
    db.open(db_path, false);
    for (int i = 10; i < 20; ++i) {
        db.append(i * 10, make_payload(8, 0xBB));
    }
    db.close();
    
    // 验证
    db.open(db_path, true);
    auto stats = db.get_stats();
    EXPECT_EQ(stats.total_records, 20);
    
    int count = 0;
    db.range_query(0, 1000, [&](const rosfs::Record&) {
        count++;
        return true;
    });
    EXPECT_EQ(count, 20);
}

// 15. 不存在的路径只读打开应抛异常
TEST_F(ROSfsTest, OpenNonExistentReadOnly) {
    rosfs::ROSfsDB db;
    EXPECT_THROW(db.open("/nonexistent/path/db", true), std::runtime_error);
}

// ============================================================================
// 随机数据测试
// ============================================================================

// 16. 随机插入查询测试
TEST_F(ROSfsTest, RandomInsertAndQuery) {
    constexpr int NUM_RECORDS = 1000;
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> ts_dist(0, 1000000);
    std::uniform_int_distribution<size_t> size_dist(1, 256);
    
    std::vector<std::pair<uint64_t, std::vector<uint8_t>>> expected;
    
    {
        rosfs::ROSfsDB db;
        db.open(db_path, false);
        
        for (int i = 0; i < NUM_RECORDS; ++i) {
            uint64_t ts = ts_dist(gen);
            auto payload = make_verifiable_payload(ts, size_dist(gen));
            db.append(ts, payload);
            expected.emplace_back(ts, payload);
        }
        db.close();
    }
    
    // 排序 expected 以便验证
    std::sort(expected.begin(), expected.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    
    {
        rosfs::ROSfsDB db;
        db.open(db_path, true);
        
        // 随机范围查询
        for (int i = 0; i < 10; ++i) {
            uint64_t start = ts_dist(gen);
            uint64_t end = start + 10000;
            
            std::vector<uint64_t> results;
            db.range_query(start, end, [&](const rosfs::Record& rec) {
                EXPECT_GE(rec.timestamp, start);
                EXPECT_LE(rec.timestamp, end);
                EXPECT_TRUE(verify_payload(rec.data, rec.timestamp));
                results.push_back(rec.timestamp);
                return true;
            });
            
            // 验证结果有序
            for (size_t j = 1; j < results.size(); ++j) {
                EXPECT_LE(results[j - 1], results[j]);
            }
        }
    }
}