#include <iostream>
#include <iomanip>
#include <sstream>
#include "include/tskv/tskv.h"

using namespace tskv;

// 生成单调递增的时间戳格式 key (模拟真实场景)
std::string MakeTimestampKey(uint64_t ts) {
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(16) << ts;
    return oss.str();
}

int main() {
    std::string db_path = "/tmp/test_monotonic";
    system(("rm -rf " + db_path).c_str());
    
    Options opt;
    opt.create_if_missing = true;
    opt.write_buffer_size = 4096;  // 小 buffer 强制多次 flush
    opt.enable_wal = false;
    
    std::unique_ptr<DB> db;
    Status s = DB::Open(opt, db_path, &db);
    if (!s.ok()) {
        std::cout << "Open failed: " << s.ToString() << std::endl;
        return 1;
    }
    
    // 写入单调递增的时间戳 key
    const int N = 100;
    std::cout << "Writing " << N << " monotonic keys..." << std::endl;
    for (int i = 0; i < N; i++) {
        std::string key = MakeTimestampKey(1000000 + i);  // 模拟时间戳
        std::string value = "value" + std::to_string(i);
        db->Put(WriteOptions(), key, value);
    }
    
    db->Flush(FlushOptions());
    
    // 读取验证
    std::cout << "Reading back..." << std::endl;
    int found = 0;
    for (int i = 0; i < N; i++) {
        std::string key = MakeTimestampKey(1000000 + i);
        std::string value;
        if (db->Get(ReadOptions(), key, &value).ok()) {
            found++;
        }
    }
    std::cout << "Found: " << found << "/" << N << std::endl;
    
    return found == N ? 0 : 1;
}
