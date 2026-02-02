#include <gtest/gtest.h>
#include <rosfs/rosfs.hpp>
#include <filesystem>
#include <vector>
#include <numeric>

namespace fs = std::filesystem;

// Test Fixture: 负责每个测试前后的环境准备与清理
class ROSfsTest : public ::testing::Test {
protected:
    std::string db_path;

    void SetUp() override {
        // 使用时间戳避免目录冲突
        db_path = "./test_rosfs_db_" + std::to_string(std::time(nullptr));
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
};

// 1. 基础写入测试
TEST_F(ROSfsTest, BasicAppendAndStats) {
    rosfs::ROSfsDB db;
    ASSERT_NO_THROW(db.open(db_path, false)); // Write mode

    db.append(100, make_payload(10, 0xA));
    db.append(200, make_payload(20, 0xB));

    auto stats = db.get_stats();
    EXPECT_EQ(stats.total_records, 2);
    EXPECT_GT(stats.data_file_size, 0);
    // Index: 2 records * 16 bytes = 32 bytes
    EXPECT_EQ(stats.index_file_size, 32); 

    db.close();
}

// 2. 持久化与数据校验测试
TEST_F(ROSfsTest, PersistenceCheck) {
    // Step 1: Write
    {
        rosfs::ROSfsDB db;
        db.open(db_path, false);
        for (int i = 0; i < 10; ++i) {
            db.append(1000 + i * 10, make_payload(8, (uint8_t)i));
        }
        db.close();
    }

    // Step 2: Read & Verify
    {
        rosfs::ROSfsDB db;
        ASSERT_NO_THROW(db.open(db_path, true)); // Read mode

        auto stats = db.get_stats();
        // 如果这里失败 (Expected 10, Actual 0)，说明 load_index 没更新 total_records_
        EXPECT_EQ(stats.total_records, 10);

        int count = 0;
        // Query full range
        db.range_query(0, 2000, [&](const rosfs::Record& rec) {
            EXPECT_EQ(rec.timestamp, 1000 + count * 10);
            EXPECT_EQ(rec.data.size(), 8);
            EXPECT_EQ(rec.data[0], (uint8_t)count); // Check payload content
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

    // Case B: Range Subset [200, 400] -> Should satisfy start <= ts <= end
    // Logic: lower_bound(200) -> 200. Scan until > 400.
    // Expected: 200, 300, 400
    std::vector<uint64_t> results;
    db.range_query(200, 400, [&](const rosfs::Record& r) {
        results.push_back(r.timestamp);
        return true;
    });
    EXPECT_EQ(results.size(), 3);
    EXPECT_EQ(results[0], 200);
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