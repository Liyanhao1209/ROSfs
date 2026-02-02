#include <gtest/gtest.h>
#include <rosbag/bag.hpp>
#include <filesystem>
#include <vector>
#include <numeric>

namespace fs = std::filesystem;

class RosbagTest : public ::testing::Test {
protected:
    std::string bag_path;

    void SetUp() override {
        bag_path = "./test.bag";
        if (fs::exists(bag_path)) fs::remove(bag_path);
    }

    void TearDown() override {
        if (fs::exists(bag_path)) fs::remove(bag_path);
    }

    std::vector<uint8_t> make_data(size_t size) {
        return std::vector<uint8_t>(size, 0xAB);
    }
};

TEST_F(RosbagTest, BasicWriteAndStats) {
    rosbag::Bag bag;
    bag.open(bag_path, rosbag::BagMode::Write);
    
    // Write small messages, likely in one chunk
    for(int i=0; i<10; ++i) {
        bag.write("topic1", 1000 + i, make_data(100));
    }
    
    auto stats = bag.get_stats();
    EXPECT_EQ(stats.message_count, 10);
    
    bag.close();
    EXPECT_TRUE(fs::exists(bag_path));
}

TEST_F(RosbagTest, ChunkingMechanism) {
    rosbag::Bag bag;
    bag.open(bag_path, rosbag::BagMode::Write);
    
    // Default chunk size is 768KB.
    // Write 1MB of data to force chunk split
    std::vector<uint8_t> large_data(1024 * 512, 0xFF); // 512KB
    
    bag.write("large_topic", 100, large_data); // Chunk Buffer ~ 512KB
    bag.write("large_topic", 200, large_data); // Chunk Buffer > 768KB -> Flush -> Buffer ~ 256KB? No, second write triggers check before?
    // Implementation checks size > threshold *after* write? Or before?
    // My impl checks `if (chunk_buffer_.size() > THRESHOLD) flush()`.
    
    // 1st Write: Buffer 512KB.
    // 2nd Write: Check (512 < 768) -> No flush. Write 512KB. Buffer 1MB.
    // 3rd Write: Check (1MB > 768) -> Flush. Buffer Clear. Write small.
    
    bag.write("small_topic", 300, make_data(100));
    
    auto stats = bag.get_stats();
    // Should have flushed at least once
    EXPECT_GE(stats.chunk_count, 1);
    
    bag.close();
}

TEST_F(RosbagTest, ReadRangeQuery) {
    {
        rosbag::Bag bag;
        bag.open(bag_path, rosbag::BagMode::Write);
        for(int i=0; i<100; ++i) {
            bag.write("t1", 1000 + i*10, make_data(10));
        }
        bag.close();
    }
    
    {
        rosbag::Bag bag;
        bag.open(bag_path, rosbag::BagMode::Read);
        
        int count = 0;
        // Query [1200, 1500] -> 1200, 1210, ..., 1500. (31 items)
        bag.range_query(1200, 1500, [&](const rosbag::Message& m) {
            EXPECT_GE(m.timestamp, 1200);
            EXPECT_LE(m.timestamp, 1500);
            EXPECT_EQ(m.data.size(), 10);
            count++;
            return true;
        });
        EXPECT_EQ(count, 31);
    }
}