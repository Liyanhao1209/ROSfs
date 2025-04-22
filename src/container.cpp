#include "rosbag/container.h"
#include "rosbag/message_instance.h"
#include "rosbag/query.h"
#include "rosbag/view.h"


#include <signal.h>
#include <assert.h>
#include <iomanip>
#include <boost/filesystem.hpp>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include <errno.h>
#include <dirent.h>


using std::map;
using std::priority_queue;
using std::string;
using std::vector;
using std::multiset;
using boost::format;
using boost::shared_ptr;
using ros::M_string;
using ros::Time;

namespace rosbag {


// Container::Container() { init(); }

Container::Container(std::string const& filename, int root_fd, TIT &&ti, ChunkedFile* header) :
        dir_name_{filename}, 
        mode_{BagMode::Write}, 
        root_fd_{root_fd}, 
        time_index{std::move(ti)}, 
        header_brick{header},
        compression_{compression::Uncompressed},
        container_size_{0},
        connection_count_{0},
        file_header_pos_{0},
        index_data_pos_{0},
        chunk_count_{0},
        msg_start_time_{0},
        is_recording_{false},
        current_buffer_{0}
{}

Container::~Container() { 
    // close();
}

// open and insert header file and write version
std::unique_ptr<Container> Container::create(std::string const& filename) {
    ROS_INFO("Opening Container, mkdir first");
    int res = mkdir(filename.c_str(), 0755);
    if (res == -1) {
        ROS_ERROR("%d %s\n", errno, strerror(errno));
        ROS_ERROR("Container mkdir failed");
        return nullptr;
    }
    
    int root_fd = ::open(filename.c_str(), O_PATH); // open container directory
    if (root_fd == -1) {
        ROS_ERROR("Open container dir failed");
        return nullptr;
    }  
    
    // init header brick for the container
    ChunkedFile* header = new ChunkedFile();
    // ROS_INFO("header path: %s", brickname.c_str());
    auto time_index = KeyValueStore<ROSTimeStamp,TIData>::create(root_fd, "time_index", 0x1000);
    if (time_index.get_fd() == -1) {
        ROS_ERROR("create time index failed");
        return nullptr;
    }

    return std::unique_ptr<Container>(new Container{filename, root_fd, std::move(time_index), header});
}

std::unique_ptr<Container> Container::open(std::string const& filename) {
    ROS_INFO("Opening Container");

    // ROS_INFO("Opening Container: Open Container dir");
    int root_fd = ::open(filename.c_str(), O_PATH | O_DIRECTORY);
    if (root_fd == -1) {
        ROS_ERROR("Failed to open container directory: %s (%s)", 
                 filename.c_str(), strerror(errno));
        return nullptr;
    }

    // ROS_INFO("Opening Container: restoring Container time index");
    auto time_index = KeyValueStore<ROSTimeStamp,TIData>::open(root_fd,"time_index",0x1000);
    if (time_index.get_fd() == -1) {
        ROS_ERROR("Failed to open time index");
        ::close(root_fd);
        return nullptr;
    }

    // ROS_INFO("Opening Container: restoring Container metadata brick");
    ChunkedFile* header_brick = new ChunkedFile();
    std::string header_path = filename + "/2147483647.brick";
    header_brick->openReadWrite(header_path);

    auto container = std::unique_ptr<Container>(new Container(
        filename,root_fd,std::move(time_index),header_brick
    ));

    // ROS_INFO("Opening Container: restoring Container metadata file header offset && index data offset");
    // restore file_header_pos_ && index_data_pos_
    std::string version_str = std::string("#ROSBAG V") + VERSION + std::string("\n");
    container->file_header_pos_ = version_str.size(); // in this version,13B
    container->index_data_pos_ = container->file_header_pos_+FILE_HEADER_LENGTH+4+4; // since there's padding space in the file header segment
    // ,the index_data_pos_ = file_header_pos_ + FILE_HEADER_LENGTH +4 (header_len) + 4(data_len)
    
    // ROS_INFO("Opening Container: restoring connection count info");
    // find out the connection_count_
    M_string file_header;
    container->seek(header_brick,container->file_header_pos_);
    container->readHeader(header_brick,file_header);
    container->connection_count_ = container->fromHeaderString<uint32_t>(file_header[CONNECTION_COUNT_FIELD_NAME]);
    // std::cerr<<"connection_count_:"<<container->connection_count_<<std::endl;
    // restore connections_ map
    // ROS_INFO("Opening Container: restoring connections map info");
    container->seek(header_brick,container->index_data_pos_);
    container->readConnectionRecords(header_brick,container->connection_count_);

    // ROS_INFO("Opening Container: restoring Container metadata chunk info");
    // restore chunk_info_
    M_string chunk_info_header;
    container->readHeader(header_brick,chunk_info_header);
    container->chunk_info_.pos = container->fromHeaderString<uint64_t>(chunk_info_header[CHUNK_POS_FIELD_NAME]);
    container->chunk_info_.start_time = container->fromHeaderString(chunk_info_header[START_TIME_FIELD_NAME]);
    container->chunk_info_.end_time = container->fromHeaderString(chunk_info_header[END_TIME_FIELD_NAME]);

    // ROS_INFO("Opening Container: restoring Container connection message count info");
    // restore chunk_info_.connection_counts
    auto chunk_connection_count = container->fromHeaderString<uint32_t>(chunk_info_header[COUNT_FIELD_NAME]);
    // std::cerr<<"chunk_connection_count:"<<chunk_connection_count<<std::endl;
    // redundant unpack but necessary
    // to skip data_len
    // ROS_INFO("Opening Container: skip connection message info data length");
    header_brick->seek(4,std::ios_base::cur);
    // std::cerr<<"loop"<<std::endl;
    for(uint32_t i=0;i<chunk_connection_count;i++){
        uint32_t connection_id,count;

        container->read(header_brick, reinterpret_cast<char*>(&connection_id),4);
        container->read(header_brick, reinterpret_cast<char*>(&count),4);

        container->chunk_info_.connection_counts.insert({connection_id,count});
    }

    // ROS_INFO("Opening Container: restoring brick files handler");
    // restore brick files
    container->brick_files_.insert({INT_MAX, header_brick});
    DIR* dir = opendir(filename.c_str());
    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        std::string name = entry->d_name;
        // std::cerr<<"name="<<name<<std::endl;
        if (name=="time_index"){
            continue;
        }

        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        size_t dot_pos = name.find('.');
        // std::cerr<<"namesub="<<name.substr(0,dot_pos)<<std::endl;
        unsigned long conn_id = std::stoul(name.substr(0, dot_pos));
        if (conn_id >= INT_MAX) continue;

        ChunkedFile* brick = new ChunkedFile();
        std::string brick_path = filename + "/" + name;
        brick->openReadWrite(brick_path);
        container->brick_files_.insert({(uint32_t)conn_id,brick});
    }
    // std::cerr<<"brick files len"<<container->brick_files_.size()<<std::endl;

    closedir(dir);
    return container;
}

// open brick file for each topic and insert it to the map
void Container::openBrick(ChunkedFile* brick, uint32_t conn_id) {
    auto brick_path = dir_name_ + "/" + std::to_string(conn_id) + ".brick";
    // std::cerr<<dir_name_<<std::endl;
    brick->openWrite(brick_path);
    brick_files_.insert({conn_id, brick});
}

// init the header file for the container
void Container::openWrite() {
    auto header_name = dir_name_ + "/2147483647.brick";
    header_brick->openWrite(header_name);
    startWriting();
}

void Container::startWriting() { 
    writeVersion();
    file_header_pos_ = header_brick->getOffset();
    // std::cout<<"file_header_pos_"<<file_header_pos_<<std::endl; 13
    writeFileHeaderRecord(header_brick);
    index_data_pos_ = header_brick->getOffset();
    // std::cout<<"index_data_pos_"<<index_data_pos_<<std::endl; 4117
    brick_files_.insert({INT_MAX, header_brick});
}

// Writing

void Container::writeVersion() {
    string version = string("#ROSBAG V") + VERSION + string("\n");
    write(header_brick, version);
}

void Container::writeFileHeaderRecord(ChunkedFile *file) {
    connection_count_ = connections_.size();
    // chunk_count_      = chunks_.size();

    // CONSOLE_BRIDGE_logDebug("Writing FILE_HEADER [%llu]: index_pos=%llu connection_count=%d chunk_count=%d",
    //           (unsigned long long) file_.getOffset(), (unsigned long long) index_data_pos_, connection_count_, chunk_count_);
    
    // Write file header record
    M_string header;
    header[OP_FIELD_NAME]               = toHeaderString(&OP_FILE_HEADER);
    header[INDEX_POS_FIELD_NAME]        = toHeaderString(&index_data_pos_);
    header[CONNECTION_COUNT_FIELD_NAME] = toHeaderString(&connection_count_);
    header[CHUNK_COUNT_FIELD_NAME]      = toHeaderString(&chunk_count_);
    header[START_TIME_FIELD_NAME]       = toHeaderString(&start_time_);
    header[END_TIME_FIELD_NAME]         = toHeaderString(&end_time_);
    // ROS_INFO("wfh:start_time_ sec: %u , nsec: %u", start_time_.sec,start_time_.nsec);
    // ROS_INFO("wfh:end_time_ sec: %u , nsec: %u", end_time_.sec,end_time_.nsec);
    // encryptor_->addFieldsToFileHeader(header);

    boost::shared_array<uint8_t> header_buffer;
    uint32_t header_len;
    ros::Header::write(header, header_buffer, header_len);
    uint32_t data_len = 0;
    if (header_len < FILE_HEADER_LENGTH)
        data_len = FILE_HEADER_LENGTH - header_len;
    write(file, (char*) &header_len, 4);
    write(file, (char*) header_buffer.get(), header_len);
    write(file, (char*) &data_len, 4);
    
    // Pad the file header record out
    if (data_len > 0) {
        string padding;
        padding.resize(data_len, ' ');
        write(file, padding);
    }
}


// Record header I/O

void Container::readHeader(ChunkedFile* file,ros::M_string& header){
    uint32_t header_len;
    file->read((char*)&header_len,4);

    boost::shared_array<uint8_t> header_buffer(new uint8_t[header_len]);
    file->read((char*)header_buffer.get(), header_len);
    ros::Header header_parser;
    std::string error_msg;

    if (header_parser.parse(header_buffer, header_len, error_msg)) {
        auto parsed_header = header_parser.getValues();
        if (parsed_header) {
            header = *parsed_header;
        } else {
            std::cerr << "Parsed header is empty." << std::endl;
        }
    } else {
        std::cerr << "Failed to parse header: " << error_msg << std::endl;
    }
}

void Container::writeHeader(ChunkedFile *file, M_string const& fields) {
    boost::shared_array<uint8_t> header_buffer;
    uint32_t header_len;
    ros::Header::write(fields, header_buffer, header_len);
    write(file, (char*) &header_len, 4);
    write(file, (char*) header_buffer.get(), header_len);
}

void Container::writeDataLength(ChunkedFile *file, uint32_t data_len) {
    write(file, (char*) &data_len, 4);
}

void Container::appendHeaderToBuffer(Buffer& buf, M_string const& fields) {
    boost::shared_array<uint8_t> header_buffer;
    uint32_t header_len;
    ros::Header::write(fields, header_buffer, header_len);

    uint32_t offset = buf.getSize();

    buf.setSize(buf.getSize() + 4 + header_len);

    memcpy(buf.getData() + offset, &header_len, 4);
    offset += 4;
    memcpy(buf.getData() + offset, header_buffer.get(), header_len);
}

void Container::appendDataLengthToBuffer(Buffer& buf, uint32_t data_len) {
    uint32_t offset = buf.getSize();

    buf.setSize(buf.getSize() + 4);

    memcpy(buf.getData() + offset, &data_len, 4);
}

// periodically write index from memory to disk
void Container::dumpIndex() {
    boost::unique_lock<boost::mutex> lock(index_mutex_);
    stopWriting();

    while (!index_buffer_.empty()) {
        auto index = index_buffer_.front();
        time_index.insert(index.time, rosbag::TIData{.conn=index.conn, .offset = index.offset});
        // time_index.insert(index.conn, TIData{.msg_no=index.msg_no, .offset = index.offset});
        index_buffer_.pop();
    }
    lock.release()->unlock();
}


void Container::writeConnectionRecords() { 
    for (auto i : connections_) {
        ConnectionInfo const* connection_info = i.second;
        writeConnectionRecord(connection_info);
    }
}

void Container::writeConnectionRecord(ConnectionInfo const* connection_info) {
    
    // always write connection info to header brick
    // ChunkedFile* header_brick = brick_files_[INT_MAX];
    M_string header;
    header[OP_FIELD_NAME]         = toHeaderString(&OP_CONNECTION);
    header[TOPIC_FIELD_NAME]      = connection_info->topic;
    header[CONNECTION_FIELD_NAME] = toHeaderString(&connection_info->id);

    writeHeader(header_brick, header);
    writeHeader(header_brick, *connection_info->header);
}

void Container::readConnectionRecord(ChunkedFile* file,ConnectionInfo* connection_info) {
    M_string header;

    // std::cerr<<"reading connection record header"<<std::endl;
    readHeader(file,header);

    // std::cerr<<"unpacking connection record header"<<std::endl;
    connection_info->topic = header[TOPIC_FIELD_NAME];
    connection_info->id = fromHeaderString<uint32_t>(header[CONNECTION_FIELD_NAME]);

    // std::cerr<<"reading connection info header"<<std::endl;
    connection_info->header = boost::make_shared<ros::M_string>();
    readHeader(file,*connection_info->header);

    // std::cerr<<"maintaining topic&&header mapping"<<std::endl;
    // restore topic_connections_ids_ && header_connections_ids_
    topic_connection_ids_.insert({connection_info->topic,connection_info->id});
    header_connection_ids_.insert({*connection_info->header,connection_info->id});
}

void Container::readConnectionRecords(ChunkedFile* file,std::size_t conn_count){
    for(std::size_t i = 0;i<conn_count;i++){
        ConnectionInfo* conn_info = new ConnectionInfo();
        readConnectionRecord(file,conn_info);
        connections_.insert({conn_info->id,conn_info});
    }
}

void Container::appendConnectionRecordToBuffer(Buffer& buf, ConnectionInfo const* connection_info) {
    M_string header;
    header[OP_FIELD_NAME]         = toHeaderString(&OP_CONNECTION);
    header[TOPIC_FIELD_NAME]      = connection_info->topic;
    header[CONNECTION_FIELD_NAME] = toHeaderString(&connection_info->id);
    appendHeaderToBuffer(buf, header);

    appendHeaderToBuffer(buf, *connection_info->header);
}

void Container::writeChunkInfoRecords() {
    // Write the chunk info header
    M_string header;
    uint32_t chunk_connection_count = chunk_info_.connection_counts.size();
    header[OP_FIELD_NAME]         = toHeaderString(&OP_CHUNK_INFO);
    header[VER_FIELD_NAME]        = toHeaderString(&CHUNK_INFO_VERSION);
    header[CHUNK_POS_FIELD_NAME]  = toHeaderString(&chunk_info_.pos);
    header[START_TIME_FIELD_NAME] = toHeaderString(&chunk_info_.start_time);
    header[END_TIME_FIELD_NAME]   = toHeaderString(&chunk_info_.end_time);
    header[COUNT_FIELD_NAME]      = toHeaderString(&chunk_connection_count);

    writeHeader(header_brick, header);

    writeDataLength(header_brick, 8 * chunk_connection_count);

    // Write the topic names and counts
    for (auto const &i:chunk_info_.connection_counts) {
        uint32_t connection_id = i.first;
        uint32_t count         = i.second;

        write(header_brick, (char*) &connection_id, 4);
        write(header_brick, (char*) &count, 4);
    }
}

void Container::close() {
    // std::cerr<<"Container closed"<<std::endl;
    ROS_INFO("Container closed");
    dumpIndex();
    stopWriting();
    // close each brick file and delete object
    for (auto &con_f : brick_files_) {
        con_f.second->close();
        delete con_f.second;
    }
    for (auto &i : connections_) {
        delete i.second;
    }
    ::close(root_fd_);
    brick_files_.clear();
    connections_.clear();
}

// update bag header and connection info
void Container::stopWriting() {
    seek(header_brick, index_data_pos_);
    writeConnectionRecords();
    writeChunkInfoRecords();
    seek(header_brick, file_header_pos_);
    writeFileHeaderRecord(header_brick);
}

uint64_t Container::getSize()     const { return container_size_; }


std::string Container::toHeaderString(Time const* field) const { 
    uint64_t packed_time = (((uint64_t) field->nsec) << 32) + field->sec;
    return toHeaderString(&packed_time);
}

// Low-level I/O

void Container::write(ChunkedFile *file, string const& s) { 
    write(file, s.c_str(), s.length()); 
}

void Container::write(ChunkedFile *file, char const* s, std::streamsize n) { 
    file->write((char*) s, n);
}

void Container::seek(ChunkedFile *file, uint64_t pos, int origin) const { 
    file->seek(pos, origin);      
}

void Container::read(ChunkedFile* file, char* b, std::streamsize n) const {
    return file->read(b, n); 
}

void Container::read(ChunkedFile* file, std::string& s, std::streamsize n) const {
    s.resize(n);
    read(file, &s[0], n);
}


} // namespace rosbag
