#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <memory>
#include <functional>
#include <map>

namespace rosbag {

// 模拟 ROS 的消息定义，这里简化为二进制 blob
struct Message {
    uint64_t timestamp; // Nanoseconds
    std::string topic;
    std::vector<uint8_t> data;
};

// 读写模式
enum class BagMode {
    Write,
    Read
};

class Bag {
public:
    Bag();
    ~Bag();

    // 打开 Bag 文件
    void open(const std::string& path, BagMode mode);

    // 写入消息 (自动处理 Connection Record 和 Chunking)
    // 默认使用 ROS 标准的 768KB Chunk Size
    void write(const std::string& topic, uint64_t timestamp, const void* data, size_t size);
    
    // 辅助重载
    void write(const std::string& topic, uint64_t timestamp, const std::vector<uint8_t>& data);

    // 范围查询 (读取模式)
    // 基于 Bag 2.0 索引进行二分查找
    void range_query(uint64_t start_ts, uint64_t end_ts, 
                     std::function<bool(const Message&)> callback);

    // 关闭文件 (写入模式下会 Flush 剩余 Chunk 并写入 Index)
    void close();

    // 获取统计信息
    struct Stats {
        uint64_t file_size;
        uint64_t message_count;
        uint64_t chunk_count;
    };
    Stats get_stats() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace rosbag