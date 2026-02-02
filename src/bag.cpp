#include <rosbag/bag.hpp>

#include <fstream>
#include <iostream>
#include <cstring>
#include <stdexcept>
#include <vector>
#include <map>
#include <algorithm>

namespace rosbag {

// -----------------------------------------------------------------------------
// Bag Format 2.0 Constants & Structs
// -----------------------------------------------------------------------------

const std::string BAG_HEADER = "#ROSBAG V2.0\n";

// Op Codes
const uint8_t OP_BAG_HEADER  = 0x03;
const uint8_t OP_CHUNK       = 0x05;
const uint8_t OP_CONNECTION  = 0x07;
const uint8_t OP_MSG_DATA    = 0x02;
const uint8_t OP_INDEX_DATA  = 0x04;
const uint8_t OP_CHUNK_INFO  = 0x06;

// 默认 Chunk 大小 (768KB, 参考 rosbag 默认值)
const size_t CHUNK_THRESHOLD = 768 * 1024; 

// 辅助：写入小端序整数
template<typename T>
void write_le(std::vector<uint8_t>& buf, T val) {
    size_t size = sizeof(T);
    size_t old_size = buf.size();
    buf.resize(old_size + size);
    std::memcpy(buf.data() + old_size, &val, size);
}

// [修复 1]: 参数类型改为 std::istream& 以兼容 fstream
template<typename T>
void read_le(std::istream& fs, T& val) {
    fs.read(reinterpret_cast<char*>(&val), sizeof(T));
}

// 辅助：Header 构造器
void build_header(std::vector<uint8_t>& buffer, const std::map<std::string, std::vector<uint8_t>>& fields) {
    uint32_t total_len = 0;
    for (const auto& kv : fields) {
        total_len += 4 + (uint32_t)kv.first.size() + 4 + (uint32_t)kv.second.size();
    }
    write_le(buffer, total_len);
    for (const auto& kv : fields) {
        write_le(buffer, (uint32_t)kv.first.size());
        buffer.insert(buffer.end(), kv.first.begin(), kv.first.end());
        write_le(buffer, (uint32_t)kv.second.size());
        buffer.insert(buffer.end(), kv.second.begin(), kv.second.end());
    }
}

// -----------------------------------------------------------------------------
// Implementation
// -----------------------------------------------------------------------------

class Bag::Impl {
public:
    std::string path_;
    BagMode mode_;
    
    // [修复 2]: 声明为 mutable 以便在 get_stats() const 中使用 seekg/tellg
    mutable std::fstream file_;
    
    // Writing State
    uint32_t conn_count_ = 0;
    uint32_t chunk_count_ = 0;
    uint64_t msg_count_ = 0;
    
    // Connection ID Map: Topic -> ID
    std::map<std::string, uint32_t> topic_to_conn_id_;
    
    // Current Chunk Buffer
    std::vector<uint8_t> chunk_buffer_;
    
    // Indexing for current chunk: conn_id -> list of (time, offset_in_chunk)
    struct IndexEntry { uint64_t time; uint32_t offset; };
    std::map<uint32_t, std::vector<IndexEntry>> current_chunk_indices_;
    
    // Global Index for Reading: conn_id -> list of (time, chunk_pos, offset_in_chunk)
    struct GlobalIndexEntry { uint64_t time; uint64_t chunk_pos; uint32_t offset; };
    std::map<uint32_t, std::vector<GlobalIndexEntry>> global_indices_;

    // Chunk Infos (for writing file footer)
    struct ChunkInfo {
        uint64_t pos;
        uint64_t start_time;
        uint64_t end_time;
        std::map<uint32_t, uint32_t> counts; // conn_id -> count
    };
    std::vector<ChunkInfo> chunk_infos_;

    Impl() {}

    void open(const std::string& path, BagMode mode) {
        path_ = path;
        mode_ = mode;
        
        if (mode_ == BagMode::Write) {
            file_.open(path, std::ios::out | std::ios::binary | std::ios::trunc);
            if (!file_) throw std::runtime_error("Failed to open bag for writing");
            
            // 1. Write Version Header
            file_.write(BAG_HEADER.c_str(), BAG_HEADER.size());
            
            // 2. Write Placeholder for Bag Header Record
            write_bag_header_record(0, 0, 0); 
        } else {
            file_.open(path, std::ios::in | std::ios::binary);
            if (!file_) throw std::runtime_error("Failed to open bag for reading");
            read_bag_index();
        }
    }

    void close() {
        if (mode_ == BagMode::Write && file_.is_open()) {
            flush_chunk(); // Write remaining messages
            
            uint64_t index_pos = file_.tellp();
            
            // Write Connection Records
            for (const auto& kv : topic_to_conn_id_) {
                write_connection_record(kv.second, kv.first);
            }
            
            // Write Chunk Info Records
            write_chunk_infos();
            
            // Rewrite Bag Header with correct index_pos
            file_.seekp(BAG_HEADER.size());
            write_bag_header_record(index_pos, conn_count_, chunk_count_);
            
            file_.close();
        }
    }

    // --- Writing Logic ---

    void write(const std::string& topic, uint64_t timestamp, const void* data, size_t size) {
        // Get or Create Connection ID
        uint32_t conn_id;
        if (topic_to_conn_id_.find(topic) == topic_to_conn_id_.end()) {
            conn_id = conn_count_++;
            topic_to_conn_id_[topic] = conn_id;
        } else {
            conn_id = topic_to_conn_id_[topic];
        }

        // Check Chunk Size
        if (chunk_buffer_.size() > CHUNK_THRESHOLD) {
            flush_chunk();
        }

        // Record Message in Index
        current_chunk_indices_[conn_id].push_back({timestamp, (uint32_t)chunk_buffer_.size()});
        
        // Append Message Data Record to Chunk Buffer
        std::map<std::string, std::vector<uint8_t>> header_fields;
        header_fields["op"] = {OP_MSG_DATA};
        
        std::vector<uint8_t> conn_bytes(4); std::memcpy(conn_bytes.data(), &conn_id, 4);
        header_fields["conn"] = conn_bytes;
        
        std::vector<uint8_t> time_bytes(8); std::memcpy(time_bytes.data(), &timestamp, 8);
        header_fields["time"] = time_bytes;
        
        std::vector<uint8_t> record_header;
        build_header(record_header, header_fields);
        
        // Write Record to Buffer
        uint32_t header_len = record_header.size();
        write_le(chunk_buffer_, header_len);
        chunk_buffer_.insert(chunk_buffer_.end(), record_header.begin(), record_header.end());
        
        uint32_t data_len = size;
        write_le(chunk_buffer_, data_len);
        const uint8_t* p = static_cast<const uint8_t*>(data);
        chunk_buffer_.insert(chunk_buffer_.end(), p, p + size);
        
        msg_count_++;
    }

    void flush_chunk() {
        if (chunk_buffer_.empty()) return;

        uint64_t chunk_pos = file_.tellp();
        
        // 1. Write Chunk Record
        std::map<std::string, std::vector<uint8_t>> header;
        header["op"] = {OP_CHUNK};
        header["compression"] = {'n','o','n','e'};
        std::vector<uint8_t> size_bytes(4); 
        uint32_t chunk_size = chunk_buffer_.size();
        std::memcpy(size_bytes.data(), &chunk_size, 4);
        header["size"] = size_bytes;
        
        write_record(header, chunk_buffer_); // Payload is the buffer
        
        // 2. Record Chunk Info
        ChunkInfo info;
        info.pos = chunk_pos;
        info.start_time = UINT64_MAX;
        info.end_time = 0;
        
        // 3. Write Index Data Records (following the chunk)
        for (const auto& kv : current_chunk_indices_) {
            uint32_t cid = kv.first;
            const auto& entries = kv.second;
            
            info.counts[cid] = entries.size();
            for (const auto& e : entries) {
                if (e.time < info.start_time) info.start_time = e.time;
                if (e.time > info.end_time) info.end_time = e.time;
            }
            
            // Construct Index Record Payload
            std::map<std::string, std::vector<uint8_t>> idx_header;
            idx_header["op"] = {OP_INDEX_DATA};
            idx_header["ver"] = {0x01, 0x00, 0x00, 0x00}; // ver=1
            
            std::vector<uint8_t> c_bytes(4); std::memcpy(c_bytes.data(), &cid, 4);
            idx_header["conn"] = c_bytes;
            
            std::vector<uint8_t> n_bytes(4); uint32_t cnt = entries.size();
            std::memcpy(n_bytes.data(), &cnt, 4);
            idx_header["count"] = n_bytes;
            
            std::vector<uint8_t> idx_payload;
            for (const auto& e : entries) {
                write_le(idx_payload, e.time);
                write_le(idx_payload, e.offset);
            }
            
            write_record(idx_header, idx_payload);
        }
        
        chunk_infos_.push_back(info);
        chunk_count_++;
        
        // Reset
        chunk_buffer_.clear();
        current_chunk_indices_.clear();
    }

    void write_bag_header_record(uint64_t index_pos, uint32_t conn_cnt, uint32_t chunk_cnt) {
        std::map<std::string, std::vector<uint8_t>> fields;
        fields["op"] = {OP_BAG_HEADER};
        
        std::vector<uint8_t> b;
        b.resize(8); std::memcpy(b.data(), &index_pos, 8); fields["index_pos"] = b;
        b.resize(4); std::memcpy(b.data(), &conn_cnt, 4); fields["conn_count"] = b;
        b.resize(4); std::memcpy(b.data(), &chunk_cnt, 4); fields["chunk_count"] = b;
        
        std::vector<uint8_t> empty_data;
        write_record(fields, empty_data);
    }
    
    void write_connection_record(uint32_t id, const std::string& topic) {
        std::map<std::string, std::vector<uint8_t>> fields;
        fields["op"] = {OP_CONNECTION};
        std::vector<uint8_t> b(4); std::memcpy(b.data(), &id, 4); fields["conn"] = b;
        std::vector<uint8_t> tb(topic.begin(), topic.end()); fields["topic"] = tb;
        
        std::vector<uint8_t> payload;
        write_record(fields, payload);
    }
    
    void write_chunk_infos() {
        for (const auto& info : chunk_infos_) {
            std::map<std::string, std::vector<uint8_t>> h;
            h["op"] = {OP_CHUNK_INFO};
            h["ver"] = {0x01, 0x00, 0x00, 0x00};
            
            std::vector<uint8_t> b;
            b.resize(8); std::memcpy(b.data(), &info.pos, 8); h["chunk_pos"] = b;
            b.resize(8); std::memcpy(b.data(), &info.start_time, 8); h["start_time"] = b;
            b.resize(8); std::memcpy(b.data(), &info.end_time, 8); h["end_time"] = b;
            b.resize(4); uint32_t cnt = info.counts.size(); std::memcpy(b.data(), &cnt, 4); h["count"] = b;
            
            std::vector<uint8_t> payload;
            for (const auto& kv : info.counts) {
                write_le(payload, kv.first);
                write_le(payload, kv.second);
            }
            write_record(h, payload);
        }
    }

    void write_record(const std::map<std::string, std::vector<uint8_t>>& header, 
                      const std::vector<uint8_t>& data) {
        std::vector<uint8_t> h_buf;
        build_header(h_buf, header);
        
        uint32_t hl = h_buf.size();
        uint32_t dl = data.size();
        
        file_.write(reinterpret_cast<char*>(&hl), 4);
        file_.write(reinterpret_cast<char*>(h_buf.data()), hl);
        file_.write(reinterpret_cast<char*>(&dl), 4);
        if (dl > 0) file_.write(reinterpret_cast<const char*>(data.data()), dl);
    }

    // --- Reading Logic ---
    
    void read_bag_index() {
        file_.seekg(BAG_HEADER.size());
        
        auto fields = read_header();
        uint32_t dl; read_le(file_, dl); file_.seekg(dl, std::ios::cur);
        
        if (fields.count("index_pos")) {
             uint64_t index_pos = *reinterpret_cast<const uint64_t*>(fields["index_pos"].data());
             if (index_pos > 0) {
                 file_.seekg(index_pos);
                 parse_connection_and_chunk_infos();
             }
        }
    }
    
    std::map<std::string, std::vector<uint8_t>> read_header() {
        uint32_t hl;
        if (!file_.read(reinterpret_cast<char*>(&hl), 4)) return {};
        std::vector<uint8_t> h_buf(hl);
        file_.read(reinterpret_cast<char*>(h_buf.data()), hl);
        
        std::map<std::string, std::vector<uint8_t>> fields;
        size_t pos = 0;
        while (pos < hl) {
            uint32_t fl = *reinterpret_cast<uint32_t*>(h_buf.data() + pos); pos += 4;
            std::string name(reinterpret_cast<char*>(h_buf.data() + pos), fl); pos += fl;
            
            uint32_t vl = *reinterpret_cast<uint32_t*>(h_buf.data() + pos); pos += 4;
            std::vector<uint8_t> val(h_buf.data() + pos, h_buf.data() + pos + vl); pos += vl;
            fields[name] = val;
        }
        return fields;
    }

    void parse_connection_and_chunk_infos() {
        while (file_.peek() != EOF) {
            auto h = read_header();
            uint32_t dl; read_le(file_, dl);
            
            if (h.empty() || h.count("op") == 0) { file_.seekg(dl, std::ios::cur); continue; }
            uint8_t op = h["op"][0];
            
            if (op == OP_CHUNK_INFO) {
                uint64_t chunk_pos = *reinterpret_cast<const uint64_t*>(h["chunk_pos"].data());
                
                std::vector<uint8_t> d(dl);
                file_.read(reinterpret_cast<char*>(d.data()), dl);
                
                load_chunk_index(chunk_pos);
            } else {
                file_.seekg(dl, std::ios::cur);
            }
        }
    }
    
    void load_chunk_index(uint64_t chunk_pos) {
        file_.seekg(chunk_pos);
        auto h = read_header(); 
        uint32_t size; read_le(file_, size);
        
        file_.seekg(size, std::ios::cur);
        
        while (true) {
            auto ih = read_header();
            if (ih.empty()) break; 
            if (ih["op"][0] != OP_INDEX_DATA) break; 
            
            uint32_t idl; read_le(file_, idl);
            std::vector<uint8_t> d(idl);
            file_.read(reinterpret_cast<char*>(d.data()), idl);
            
            uint32_t cid = *reinterpret_cast<const uint32_t*>(ih["conn"].data());
            uint32_t count = *reinterpret_cast<const uint32_t*>(ih["count"].data());
            
            size_t p = 0;
            for(uint32_t i=0; i<count; ++i) {
                uint64_t time = *reinterpret_cast<uint64_t*>(d.data() + p); p+=8;
                uint32_t off  = *reinterpret_cast<uint32_t*>(d.data() + p); p+=4;
                global_indices_[cid].push_back({time, chunk_pos, off});
            }
        }
    }

    void range_query(uint64_t start_ts, uint64_t end_ts, std::function<bool(const Message&)> callback) {
        for (const auto& kv : global_indices_) {
            const auto& entries = kv.second;
            auto it = std::lower_bound(entries.begin(), entries.end(), start_ts, 
                [](const GlobalIndexEntry& e, uint64_t t) { return e.time < t; });
                
            for (; it != entries.end(); ++it) {
                if (it->time > end_ts) break;
                
                file_.seekg(it->chunk_pos);
                auto ch = read_header();
                uint32_t cs; read_le(file_, cs);
                
                file_.seekg(it->offset, std::ios::cur);
                
                auto rh = read_header();
                uint32_t dl; read_le(file_, dl);
                
                if (rh["op"][0] == OP_MSG_DATA) {
                    Message msg;
                    msg.timestamp = *reinterpret_cast<const uint64_t*>(rh["time"].data());
                    msg.topic = "unknown"; // Benchmark simplification
                    msg.data.resize(dl);
                    file_.read(reinterpret_cast<char*>(msg.data.data()), dl);
                    
                    if (!callback(msg)) return;
                } else {
                    file_.seekg(dl, std::ios::cur);
                }
            }
        }
    }
    
    // [修复 3]: get_stats() 保持 const，但依赖 mutable file_
    Stats get_stats() const {
        Stats s;
        if (mode_ == BagMode::Write) {
             s.message_count = msg_count_;
             s.chunk_count = chunk_count_;
             s.file_size = file_.tellp();
        } else {
             file_.seekg(0, std::ios::end);
             s.file_size = file_.tellg();
             s.chunk_count = 0; 
             s.message_count = 0;
             for(auto& kv : global_indices_) s.message_count += kv.second.size();
        }
        return s;
    }
};

// -----------------------------------------------------------------------------
// Public Wrapper
// -----------------------------------------------------------------------------

Bag::Bag() : impl_(std::make_unique<Impl>()) {}
Bag::~Bag() = default;

void Bag::open(const std::string& path, BagMode mode) {
    impl_->open(path, mode);
}

void Bag::write(const std::string& topic, uint64_t timestamp, const void* data, size_t size) {
    impl_->write(topic, timestamp, data, size);
}

void Bag::write(const std::string& topic, uint64_t timestamp, const std::vector<uint8_t>& data) {
    write(topic, timestamp, data.data(), data.size());
}

void Bag::range_query(uint64_t start_ts, uint64_t end_ts, std::function<bool(const Message&)> callback) {
    impl_->range_query(start_ts, end_ts, callback);
}

void Bag::close() {
    impl_->close();
}

Bag::Stats Bag::get_stats() const {
    return impl_->get_stats();
}

} // namespace rosbag