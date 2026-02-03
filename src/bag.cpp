#include <rosbag/bag.hpp>
#include <fstream>
#include <iostream>
#include <cstring>
#include <stdexcept>
#include <vector>
#include <map>
#include <algorithm>
#include <mutex>
#include <iomanip>

namespace rosbag {

// [调试开关] 保持开启
#define ROSBAG_DEBUG 0

void debug_log(const std::string& msg) {
#if ROSBAG_DEBUG
    std::cerr << "[ROSBAG_DEBUG] " << msg << std::endl;
#endif
}

const std::string BAG_HEADER = "#ROSBAG V2.0\n";
const uint8_t OP_BAG_HEADER  = 0x03;
const uint8_t OP_CHUNK       = 0x05;
const uint8_t OP_CONNECTION  = 0x07;
const uint8_t OP_MSG_DATA    = 0x02;
const uint8_t OP_INDEX_DATA  = 0x04;
const uint8_t OP_CHUNK_INFO  = 0x06;
const size_t CHUNK_THRESHOLD = 768 * 1024; 

template<typename T> void write_le(std::vector<uint8_t>& buf, T val) {
    size_t size = sizeof(T);
    size_t old_size = buf.size();
    buf.resize(old_size + size);
    std::memcpy(buf.data() + old_size, &val, size);
}

template<typename T> bool read_le(std::istream& fs, T& val) {
    if (!fs.read(reinterpret_cast<char*>(&val), sizeof(T))) return false;
    return true;
}

// [修复] 移除内嵌的 total_len，调用者自己处理长度前缀
void build_header(std::vector<uint8_t>& buffer, const std::map<std::string, std::vector<uint8_t>>& fields) {
    for (const auto& kv : fields) {
        write_le(buffer, (uint32_t)kv.first.size());
        buffer.insert(buffer.end(), kv.first.begin(), kv.first.end());
        write_le(buffer, (uint32_t)kv.second.size());
        buffer.insert(buffer.end(), kv.second.begin(), kv.second.end());
    }
}

class Bag::Impl {
public:
    std::string path_;
    BagMode mode_;
    mutable std::fstream file_;
    mutable std::mutex mutex_; 
    
    uint32_t conn_count_ = 0;
    uint32_t chunk_count_ = 0;
    uint64_t msg_count_ = 0;
    
    std::map<std::string, uint32_t> topic_to_conn_id_;
    std::vector<uint8_t> chunk_buffer_;
    
    struct IndexEntry { uint64_t time; uint32_t offset; };
    std::map<uint32_t, std::vector<IndexEntry>> current_chunk_indices_;
    
    struct GlobalIndexEntry { uint64_t time; uint64_t chunk_pos; uint32_t offset; };
    std::map<uint32_t, std::vector<GlobalIndexEntry>> global_indices_;

    struct ChunkInfo {
        uint64_t pos; uint64_t start_time; uint64_t end_time;
        std::map<uint32_t, uint32_t> counts; 
    };
    std::vector<ChunkInfo> chunk_infos_;

    Impl() {}

    void open(const std::string& path, BagMode mode) {
        std::lock_guard<std::mutex> lock(mutex_);
        path_ = path; mode_ = mode;
        if (mode_ == BagMode::Write) {
            file_.open(path, std::ios::out | std::ios::in | std::ios::binary | std::ios::trunc);
            if (!file_) throw std::runtime_error("Failed to open bag for writing");
            file_.write(BAG_HEADER.c_str(), BAG_HEADER.size());
            write_bag_header_record(0, 0, 0); 
        } else {
            file_.open(path, std::ios::in | std::ios::binary);
            if (!file_) throw std::runtime_error("Failed to open bag for reading");
            read_bag_index();
        }
    }

    void close() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (mode_ == BagMode::Write && file_.is_open()) {
            flush_chunk(); 
            uint64_t index_pos = file_.tellp();
            for (const auto& kv : topic_to_conn_id_) write_connection_record(kv.second, kv.first);
            write_chunk_infos();
            file_.seekp(BAG_HEADER.size());
            write_bag_header_record(index_pos, conn_count_, chunk_count_);
            
            file_.flush(); // [修复] 确保刷盘
            file_.close();
        }
    }

    void write(const std::string& topic, uint64_t timestamp, const void* data, size_t size) {
        std::lock_guard<std::mutex> lock(mutex_); 
        
        uint32_t conn_id;
        if (topic_to_conn_id_.find(topic) == topic_to_conn_id_.end()) {
            conn_id = conn_count_++;
            topic_to_conn_id_[topic] = conn_id;
        } else {
            conn_id = topic_to_conn_id_[topic];
        }

        if (chunk_buffer_.size() > CHUNK_THRESHOLD) {
            flush_chunk();
        }

        current_chunk_indices_[conn_id].push_back({timestamp, (uint32_t)chunk_buffer_.size()});
        
        std::map<std::string, std::vector<uint8_t>> header_fields;
        header_fields["op"] = {OP_MSG_DATA};
        std::vector<uint8_t> conn_bytes(4); std::memcpy(conn_bytes.data(), &conn_id, 4); header_fields["conn"] = conn_bytes;
        std::vector<uint8_t> time_bytes(8); std::memcpy(time_bytes.data(), &timestamp, 8); header_fields["time"] = time_bytes;
        
        std::vector<uint8_t> record_header;
        build_header(record_header, header_fields);
        
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
        
        std::map<std::string, std::vector<uint8_t>> header;
        header["op"] = {OP_CHUNK}; header["compression"] = {'n','o','n','e'};
        std::vector<uint8_t> size_bytes(4); uint32_t chunk_size = chunk_buffer_.size();
        std::memcpy(size_bytes.data(), &chunk_size, 4); header["size"] = size_bytes;
        
        write_record(header, chunk_buffer_);
        
        ChunkInfo info; info.pos = chunk_pos; info.start_time = UINT64_MAX; info.end_time = 0;
        
        for (const auto& kv : current_chunk_indices_) {
            uint32_t cid = kv.first; const auto& entries = kv.second;
            info.counts[cid] = entries.size();
            for (const auto& e : entries) {
                if (e.time < info.start_time) info.start_time = e.time;
                if (e.time > info.end_time) info.end_time = e.time;
                global_indices_[cid].push_back({e.time, chunk_pos, e.offset});
            }
            std::map<std::string, std::vector<uint8_t>> idx_header;
            idx_header["op"] = {OP_INDEX_DATA}; idx_header["ver"] = {0x01, 0x00, 0x00, 0x00};
            std::vector<uint8_t> c_bytes(4); std::memcpy(c_bytes.data(), &cid, 4); idx_header["conn"] = c_bytes;
            std::vector<uint8_t> n_bytes(4); uint32_t cnt = entries.size(); std::memcpy(n_bytes.data(), &cnt, 4); idx_header["count"] = n_bytes;
            std::vector<uint8_t> idx_payload;
            for (const auto& e : entries) { write_le(idx_payload, e.time); write_le(idx_payload, e.offset); }
            write_record(idx_header, idx_payload);
        }
        chunk_infos_.push_back(info); chunk_count_++;
        chunk_buffer_.clear(); current_chunk_indices_.clear();
    }

    void write_bag_header_record(uint64_t index_pos, uint32_t conn_cnt, uint32_t chunk_cnt) {
        std::map<std::string, std::vector<uint8_t>> fields;
        fields["op"] = {OP_BAG_HEADER};
        std::vector<uint8_t> b(8); std::memcpy(b.data(), &index_pos, 8); fields["index_pos"] = b;
        std::vector<uint8_t> b4(4); std::memcpy(b4.data(), &conn_cnt, 4); fields["conn_count"] = b4;
        std::memcpy(b4.data(), &chunk_cnt, 4); fields["chunk_count"] = b4;
        write_record(fields, {});
    }
    
    void write_connection_record(uint32_t id, const std::string& topic) {
        std::map<std::string, std::vector<uint8_t>> fields;
        fields["op"] = {OP_CONNECTION};
        std::vector<uint8_t> b(4); std::memcpy(b.data(), &id, 4); fields["conn"] = b;
        std::vector<uint8_t> tb(topic.begin(), topic.end()); fields["topic"] = tb;
        write_record(fields, {});
    }
    
    void write_chunk_infos() {
        for (const auto& info : chunk_infos_) {
            std::map<std::string, std::vector<uint8_t>> h;
            h["op"] = {OP_CHUNK_INFO}; h["ver"] = {0x01, 0x00, 0x00, 0x00};
            std::vector<uint8_t> b(8); std::memcpy(b.data(), &info.pos, 8); h["chunk_pos"] = b;
            std::memcpy(b.data(), &info.start_time, 8); h["start_time"] = b;
            std::memcpy(b.data(), &info.end_time, 8); h["end_time"] = b;
            std::vector<uint8_t> b4(4); uint32_t cnt = info.counts.size(); std::memcpy(b4.data(), &cnt, 4); h["count"] = b4;
            std::vector<uint8_t> payload;
            for (const auto& kv : info.counts) { write_le(payload, kv.first); write_le(payload, kv.second); }
            write_record(h, payload);
        }
    }

    void write_record(const std::map<std::string, std::vector<uint8_t>>& header, const std::vector<uint8_t>& data) {
        std::vector<uint8_t> h_buf; build_header(h_buf, header);
        uint32_t hl = h_buf.size(); uint32_t dl = data.size();
        file_.write(reinterpret_cast<char*>(&hl), 4);
        file_.write(reinterpret_cast<char*>(h_buf.data()), hl);
        file_.write(reinterpret_cast<char*>(&dl), 4);
        if (dl > 0) file_.write(reinterpret_cast<const char*>(data.data()), dl);
    }

    void read_bag_index() {
        file_.seekg(BAG_HEADER.size());
        
        // [调试] 打印 Bag Header 位置
        debug_log("Seeked to header pos: " + std::to_string(file_.tellg()));
        
        auto fields = read_header();
        
        // [调试] 打印读到的字段
        if (fields.empty()) debug_log("Header fields EMPTY!");
        else {
            for(auto const& [key, val] : fields) {
                debug_log("  Field: " + key + " Len: " + std::to_string(val.size()));
            }
        }

        uint32_t dl; 
        if (!read_le(file_, dl)) { debug_log("Read first record dl failed"); return; }
        file_.seekg(dl, std::ios::cur);
        
        if (fields.find("index_pos") != fields.end() && fields["index_pos"].size() >= 8) {
             uint64_t index_pos = *reinterpret_cast<const uint64_t*>(fields["index_pos"].data());
             debug_log("Bag Header found. Index Pos: " + std::to_string(index_pos));
             if (index_pos > 0) { 
                 file_.seekg(index_pos); 
                 parse_connection_and_chunk_infos(); 
             }
        } else {
            debug_log("Bag Header NOT found or missing index_pos");
        }
    }
    
    std::map<std::string, std::vector<uint8_t>> read_header() {
        uint32_t hl; 
        if (!file_.read(reinterpret_cast<char*>(&hl), 4)) return {};
        
        std::vector<uint8_t> h_buf(hl); 
        if (!file_.read(reinterpret_cast<char*>(h_buf.data()), hl)) return {}; 
        
        std::map<std::string, std::vector<uint8_t>> fields;
        size_t pos = 0;
        while (pos < hl) {
            if (pos + 4 > hl) break; 
            uint32_t fl = *reinterpret_cast<uint32_t*>(h_buf.data() + pos); pos += 4;
            
            if (pos + fl > hl) break;
            std::string name(reinterpret_cast<char*>(h_buf.data() + pos), fl); pos += fl;
            
            if (pos + 4 > hl) break;
            uint32_t vl = *reinterpret_cast<uint32_t*>(h_buf.data() + pos); pos += 4;
            
            if (pos + vl > hl) break;
            std::vector<uint8_t> val(h_buf.data() + pos, h_buf.data() + pos + vl); pos += vl;
            fields[name] = val;
        }
        return fields;
    }

    void parse_connection_and_chunk_infos() {
        file_.clear();
        debug_log("Parsing Connection and Chunk Infos from: " + std::to_string(file_.tellg()));
        
        while (file_.peek() != EOF) {
            auto h = read_header(); 
            uint32_t dl; 
            if (!read_le(file_, dl)) break;

            if (h.empty() || h.find("op") == h.end() || h["op"].empty()) { 
                file_.seekg(dl, std::ios::cur); 
                continue; 
            }
            uint8_t op = h["op"][0];
            debug_log("Record OP: " + std::to_string((int)op));
            
            if (op == OP_CHUNK_INFO) {
                if (h.find("chunk_pos") != h.end() && h["chunk_pos"].size() >= 8) {
                    uint64_t chunk_pos = *reinterpret_cast<const uint64_t*>(h["chunk_pos"].data());
                    debug_log("Found Chunk Info. Chunk Pos: " + std::to_string(chunk_pos));
                    
                    std::vector<uint8_t> d(dl); 
                    if (!file_.read(reinterpret_cast<char*>(d.data()), dl)) break; 
                    
                    uint64_t next_record_pos = file_.tellg();
                    load_chunk_index(chunk_pos);
                    file_.clear(); 
                    file_.seekg(next_record_pos);
                } else {
                    file_.seekg(dl, std::ios::cur);
                }
            } else { 
                file_.seekg(dl, std::ios::cur); 
            }
        }
    }

    void load_chunk_index(uint64_t chunk_pos) {
        file_.clear();
        file_.seekg(chunk_pos); 
        debug_log("Loading Chunk Index at: " + std::to_string(chunk_pos));
        
        auto h = read_header(); 
        uint32_t size; 
        if (!read_le(file_, size)) { debug_log("Failed to read chunk size"); return; }
        file_.seekg(size, std::ios::cur); 
        
        while (true) {
            auto ih = read_header();
            if (ih.empty()) break; 
            if (ih.find("op") == ih.end() || ih["op"].empty()) break;
            
            uint8_t op = ih["op"][0];
            if (op != OP_INDEX_DATA) break; 
            
            uint32_t idl; 
            if (!read_le(file_, idl)) break;
            std::vector<uint8_t> d(idl); 
            if (!file_.read(reinterpret_cast<char*>(d.data()), idl)) break;
            
            if (ih.find("conn") == ih.end() || ih.find("count") == ih.end()) continue;

            uint32_t cid = *reinterpret_cast<const uint32_t*>(ih["conn"].data());
            uint32_t count = *reinterpret_cast<const uint32_t*>(ih["count"].data());
            
            debug_log("  Loaded Index Data. Conn: " + std::to_string(cid) + " Count: " + std::to_string(count));

            size_t p = 0;
            for(uint32_t i=0; i<count; ++i) {
                if (p + 12 > d.size()) break; 
                uint64_t time = *reinterpret_cast<uint64_t*>(d.data() + p); p+=8;
                uint32_t off  = *reinterpret_cast<uint32_t*>(d.data() + p); p+=4;
                global_indices_[cid].push_back({time, chunk_pos, off});
            }
        }
    }

    void range_query(uint64_t start_ts, uint64_t end_ts, std::function<bool(const Message&)> callback) {
        std::lock_guard<std::mutex> lock(mutex_);
        file_.clear(); 

        debug_log("Range Query: " + std::to_string(start_ts) + " - " + std::to_string(end_ts));
        
        // 1. Query Global Index
        for (const auto& kv : global_indices_) {
            const auto& entries = kv.second;
            auto it = std::lower_bound(entries.begin(), entries.end(), start_ts, 
                [](const GlobalIndexEntry& e, uint64_t t) { return e.time < t; });
            for (; it != entries.end(); ++it) {
                if (it->time > end_ts) break;
                
                file_.seekg(it->chunk_pos); 
                auto ch = read_header(); 
                uint32_t cs; 
                if (!read_le(file_, cs)) break;
                
                file_.seekg(it->offset, std::ios::cur);
                
                auto rh = read_header(); 
                uint32_t dl; 
                if (!read_le(file_, dl)) break;
                
                if (!rh.empty() && rh.find("op") != rh.end() && !rh["op"].empty() && rh["op"][0] == OP_MSG_DATA) {
                    Message msg; 
                    if (rh.find("time") != rh.end() && rh["time"].size() >= 8) 
                        msg.timestamp = *reinterpret_cast<const uint64_t*>(rh["time"].data());
                    else msg.timestamp = 0;
                        
                    msg.topic = "topic_0"; 
                    msg.data.resize(dl);
                    if (!file_.read(reinterpret_cast<char*>(msg.data.data()), dl)) break; 
                    if (!callback(msg)) return;
                } else { 
                    file_.seekg(dl, std::ios::cur); 
                }
            }
        }

        // 2. Query Buffer (For Mixed Mode)
        for (const auto& kv : current_chunk_indices_) {
            const auto& entries = kv.second;
            auto it = std::lower_bound(entries.begin(), entries.end(), start_ts, 
                [](const IndexEntry& e, uint64_t t) { return e.time < t; });
            for (; it != entries.end(); ++it) {
                if (it->time > end_ts) break;
                size_t offset = it->offset;
                if (offset + 4 > chunk_buffer_.size()) continue;
                uint32_t hl = *reinterpret_cast<const uint32_t*>(&chunk_buffer_[offset]); offset += 4;
                offset += hl; // Skip header
                if (offset + 4 > chunk_buffer_.size()) continue;
                uint32_t dl = *reinterpret_cast<const uint32_t*>(&chunk_buffer_[offset]); offset += 4;
                if (offset + dl > chunk_buffer_.size()) continue;
                Message msg; msg.timestamp = it->time; msg.topic = "topic_0";
                msg.data.assign(chunk_buffer_.begin() + offset, chunk_buffer_.begin() + offset + dl);
                if (!callback(msg)) return;
            }
        }
    }
    
    Stats get_stats() const {
        std::lock_guard<std::mutex> lock(mutex_); 
        Stats s;
        if (mode_ == BagMode::Write) {
             s.message_count = msg_count_;
             s.chunk_count = chunk_count_;
             s.file_size = file_.tellp();
        } else {
             file_.seekg(0, std::ios::end);
             s.file_size = file_.tellg();
             s.chunk_count = 0; s.message_count = 0;
             for(auto& kv : global_indices_) s.message_count += kv.second.size();
        }
        return s;
    }
};

Bag::Bag() : impl_(std::make_unique<Impl>()) {}
Bag::~Bag() = default;
void Bag::open(const std::string& path, BagMode mode) { impl_->open(path, mode); }
void Bag::write(const std::string& topic, uint64_t timestamp, const void* data, size_t size) { impl_->write(topic, timestamp, data, size); }
void Bag::write(const std::string& topic, uint64_t timestamp, const std::vector<uint8_t>& data) { write(topic, timestamp, data.data(), data.size()); }
void Bag::range_query(uint64_t start_ts, uint64_t end_ts, std::function<bool(const Message&)> callback) { impl_->range_query(start_ts, end_ts, callback); }
void Bag::close() { impl_->close(); }
Bag::Stats Bag::get_stats() const { return impl_->get_stats(); }

} // namespace rosbag