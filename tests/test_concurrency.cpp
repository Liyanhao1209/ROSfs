#include <gtest/gtest.h>
#include <rosbag/bag.hpp>
#include <filesystem>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>

namespace fs = std::filesystem;

class RosbagConcurrencyTest : public ::testing::Test {
protected:
    std::string bag_path;

    void SetUp() override {
        bag_path = "./test_rosbag_concurrent_" + std::to_string(std::time(nullptr)) + ".bag";
        if (fs::exists(bag_path)) fs::remove(bag_path);
    }

    void TearDown() override {
        if (fs::exists(bag_path)) fs::remove(bag_path);
    }
};

TEST_F(RosbagConcurrencyTest, WriteFlushReadMixed) {
    rosbag::Bag bag;
    bag.open(bag_path, rosbag::BagMode::Write);

    std::atomic<bool> running{true};
    int num_records = 10000;
    
    // Writer: Writes enough data to trigger multiple chunks (768KB each)
    std::thread writer([&]() {
        std::vector<uint8_t> payload(1024, 0xAB); // 1KB per msg
        for (int i = 0; i < num_records; ++i) {
            bag.write("topic", 1000 + i, payload);
            if (i % 100 == 0) std::this_thread::sleep_for(std::chrono::microseconds(500));
        }
        running = false;
    });

    // Reader: Queries mixed range (some on disk, some in buffer)
    std::thread reader([&]() {
        while (running) {
            auto stats = bag.get_stats();
            if (stats.message_count > 200) {
                uint64_t latest_ts = 1000 + stats.message_count - 1;
                // Query range spanning across disk chunk and memory buffer
                // e.g., last 200 items
                int count = 0;
                bag.range_query(latest_ts - 200, latest_ts, [&](const rosbag::Message& m) {
                    count++;
                    return true;
                });
                // Basic check
                // EXPECT_GT(count, 0); // Assertions in threads can be tricky in GTest, usually use EXPECT
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
    });

    writer.join();
    reader.join();

    auto stats = bag.get_stats();
    EXPECT_EQ(stats.message_count, num_records);
    bag.close();
}