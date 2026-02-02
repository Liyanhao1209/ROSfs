#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <memory>
#include <mutex>
#include <functional> // [修复] 添加了 functional 头文件

namespace rosfs {

// 定义 ROSfs 的数据记录结构
struct Record {
    uint64_t timestamp;
    std::vector<uint8_t> data;
};

// 核心存储引擎类
class ROSfsDB {
public:
    ROSfsDB();
    ~ROSfsDB();

    // 打开/创建数据库
    // path: 数据库目录路径 (e.g., "bench_out/_rosfs")
    // read_only: 读写模式控制
    void open(const std::string& path, bool read_only = false);

    // 写入一条消息
    void append(uint64_t timestamp, const std::vector<uint8_t>& data);

    // 范围查询
    // 返回时间范围内的所有记录
    // 回调函数 callback 接收每条记录，返回 false 则停止扫描
    void range_query(uint64_t start_ts, uint64_t end_ts, 
                     std::function<bool(const Record&)> callback);

    // 获取存储统计
    struct Stats {
        uint64_t data_file_size;
        uint64_t index_file_size;
        uint64_t total_records;
    };
    Stats get_stats() const;

    void close();

private:
    class Impl; // Pimpl 模式隐藏实现细节
    std::unique_ptr<Impl> impl_;
};

} // namespace rosfs