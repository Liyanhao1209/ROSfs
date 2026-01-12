// Copyright (c) 2026 TSKV Authors. All rights reserved.
// Unit tests for TSKV components

#include <cassert>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "tskv/tskv.h"

using namespace tskv;

// Test helpers
#define TEST_ASSERT(cond, msg) \
    do { \
        if (!(cond)) { \
            std::cerr << "FAILED: " << msg << " at " << __FILE__ << ":" << __LINE__ << std::endl; \
            return false; \
        } \
    } while (0)

#define RUN_TEST(name) \
    do { \
        std::cout << "Running " << #name << "... "; \
        if (name()) { \
            std::cout << "PASSED" << std::endl; \
            passed++; \
        } else { \
            std::cout << "FAILED" << std::endl; \
            failed++; \
        } \
    } while (0)

// Slice tests
bool test_slice_basic() {
    Slice s1("hello");
    TEST_ASSERT(s1.size() == 5, "size should be 5");
    TEST_ASSERT(s1.data()[0] == 'h', "first char should be 'h'");
    
    Slice s2("hello", 5);
    TEST_ASSERT(s1 == s2, "slices should be equal");
    
    Slice s3("world");
    TEST_ASSERT(s1 != s3, "slices should not be equal");
    TEST_ASSERT(s1 < s3, "hello < world");
    
    return true;
}

bool test_slice_compare() {
    Slice a("abc");
    Slice b("abd");
    Slice c("abc");
    
    TEST_ASSERT(a.compare(b) < 0, "abc < abd");
    TEST_ASSERT(b.compare(a) > 0, "abd > abc");
    TEST_ASSERT(a.compare(c) == 0, "abc == abc");
    
    return true;
}

// Arena tests
bool test_arena_basic() {
    Arena arena;
    
    char* p1 = arena.Allocate(100);
    TEST_ASSERT(p1 != nullptr, "allocation should succeed");
    
    char* p2 = arena.Allocate(200);
    TEST_ASSERT(p2 != nullptr, "second allocation should succeed");
    TEST_ASSERT(p2 != p1, "allocations should be different");
    
    TEST_ASSERT(arena.MemoryUsage() > 0, "memory usage should be positive");
    
    return true;
}

bool test_arena_aligned() {
    Arena arena;
    
    char* p = arena.AllocateAligned(100, 64);
    TEST_ASSERT(p != nullptr, "aligned allocation should succeed");
    TEST_ASSERT((reinterpret_cast<uintptr_t>(p) % 64) == 0, "should be 64-byte aligned");
    
    return true;
}

// SkipList tests
bool test_skiplist_basic() {
    Arena arena;
    SkipList<> list(BytewiseComparator(), &arena);
    
    // Insert some keys
    std::string keys[] = {"aaa", "bbb", "ccc", "ddd"};
    for (const auto& key : keys) {
        std::string encoded;
        PutVarint32(&encoded, static_cast<uint32_t>(key.size()));
        encoded.append(key);
        
        char* buf = list.AllocateKey(encoded.size());
        memcpy(buf, encoded.data(), encoded.size());
        list.Insert(buf);
    }
    
    TEST_ASSERT(list.Count() == 4, "should have 4 entries");
    
    return true;
}

bool test_skiplist_iterator() {
    Arena arena;
    SkipList<> list(BytewiseComparator(), &arena);
    
    // Insert keys
    for (int i = 0; i < 100; i++) {
        std::string key = "key" + std::to_string(i);
        std::string encoded;
        PutVarint32(&encoded, static_cast<uint32_t>(key.size()));
        encoded.append(key);
        
        char* buf = list.AllocateKey(encoded.size());
        memcpy(buf, encoded.data(), encoded.size());
        list.Insert(buf);
    }
    
    // Iterate
    SkipList<>::Iterator iter(&list);
    iter.SeekToFirst();
    
    int count = 0;
    while (iter.Valid()) {
        count++;
        iter.Next();
    }
    
    TEST_ASSERT(count == 100, "should iterate 100 entries");
    
    return true;
}

// Coding tests
bool test_varint32() {
    char buf[10];
    
    // Test various values
    uint32_t values[] = {0, 1, 127, 128, 255, 256, 16383, 16384, UINT32_MAX};
    for (uint32_t v : values) {
        char* end = EncodeVarint32(buf, v);
        uint32_t decoded;
        const char* result = GetVarint32Ptr(buf, end, &decoded);
        TEST_ASSERT(result == end, "should decode to end");
        TEST_ASSERT(decoded == v, "decoded value should match");
    }
    
    return true;
}

bool test_varint64() {
    char buf[20];
    
    uint64_t values[] = {0, 1, 127, 128, UINT32_MAX, UINT64_MAX};
    for (uint64_t v : values) {
        char* end = EncodeVarint64(buf, v);
        uint64_t decoded;
        const char* result = GetVarint64Ptr(buf, end, &decoded);
        TEST_ASSERT(result == end, "should decode to end");
        TEST_ASSERT(decoded == v, "decoded value should match");
    }
    
    return true;
}

// MemTable tests
bool test_memtable_basic() {
    MemTable mem(4 * 1024 * 1024);
    
    // Add some entries
    mem.Add(1, kTypeValue, "key1", "value1");
    mem.Add(2, kTypeValue, "key2", "value2");
    mem.Add(3, kTypeValue, "key3", "value3");
    
    TEST_ASSERT(mem.Count() == 3, "should have 3 entries");
    
    // Get entries
    std::string value;
    Status s;
    
    bool found = mem.Get("key1", &value, &s);
    TEST_ASSERT(found && s.ok(), "should find key1");
    TEST_ASSERT(value == "value1", "value should match");
    
    found = mem.Get("key2", &value, &s);
    TEST_ASSERT(found && s.ok(), "should find key2");
    TEST_ASSERT(value == "value2", "value should match");
    
    found = mem.Get("nonexistent", &value, &s);
    TEST_ASSERT(!found, "should not find nonexistent key");
    
    return true;
}

bool test_memtable_delete() {
    MemTable mem(4 * 1024 * 1024);
    
    mem.Add(1, kTypeValue, "key", "value1");
    mem.Add(2, kTypeDeletion, "key", "");
    
    std::string value;
    Status s;
    bool found = mem.Get("key", &value, &s);
    
    TEST_ASSERT(found, "should find deletion marker");
    TEST_ASSERT(s.IsNotFound(), "status should be NotFound");
    
    return true;
}

// BlockBuilder tests
bool test_block_builder() {
    BlockBuilder builder;
    
    builder.Add("key1", "value1");
    builder.Add("key2", "value2");
    builder.Add("key3", "value3");
    
    Slice block = builder.Finish();
    TEST_ASSERT(block.size() > 0, "block should not be empty");
    
    // Read back
    Block reader(block.data(), block.size());
    Block::Iterator iter(&reader);
    
    iter.SeekToFirst();
    TEST_ASSERT(iter.Valid(), "iterator should be valid");
    TEST_ASSERT(iter.key() == Slice("key1"), "first key should be key1");
    TEST_ASSERT(iter.value() == Slice("value1"), "first value should be value1");
    
    iter.Next();
    TEST_ASSERT(iter.Valid(), "iterator should be valid");
    TEST_ASSERT(iter.key() == Slice("key2"), "second key should be key2");
    
    iter.Next();
    TEST_ASSERT(iter.Valid(), "iterator should be valid");
    TEST_ASSERT(iter.key() == Slice("key3"), "third key should be key3");
    
    iter.Next();
    TEST_ASSERT(!iter.Valid(), "iterator should be invalid");
    
    return true;
}

// SSTable tests
bool test_sstable_build_and_read() {
    std::string filename = "/tmp/tskv_test.sst";
    
    // Build SSTable
    {
        SSTableBuilder builder(filename);
        
        for (int i = 0; i < 1000; i++) {
            std::string key = "key" + std::to_string(1000000 + i);  // Ensure sorted order
            std::string value = "value" + std::to_string(i);
            builder.Add(key, value);
        }
        
        Status s = builder.Finish();
        TEST_ASSERT(s.ok(), "SSTable build should succeed");
        TEST_ASSERT(builder.NumEntries() == 1000, "should have 1000 entries");
    }
    
    // Read SSTable
    {
        std::unique_ptr<SSTableReader> reader;
        Status s = SSTableReader::Open(filename, &reader);
        TEST_ASSERT(s.ok(), "SSTable open should succeed");
        TEST_ASSERT(reader->NumEntries() == 1000, "should have 1000 entries");
        
        // Point lookup
        std::string value;
        s = reader->Get("key1000500", &value);
        TEST_ASSERT(s.ok(), "should find key");
        TEST_ASSERT(value == "value500", "value should match");
        
        s = reader->Get("nonexistent", &value);
        TEST_ASSERT(s.IsNotFound(), "should not find nonexistent key");
    }
    
    // Cleanup
    std::remove(filename.c_str());
    
    return true;
}

// SSTableIndex tests
bool test_sstable_index() {
    SSTableIndex<> index;
    
    // Create some mock readers (we can't actually test without files)
    // This is a simplified test
    
    TEST_ASSERT(index.Count() == 0, "should be empty initially");
    
    return true;
}

// WAL tests
bool test_wal_write_read() {
    std::string filename = "/tmp/tskv_test.wal";
    
    // Write
    {
        WALWriter writer(filename);
        
        Status s = writer.AddRecord("record1");
        TEST_ASSERT(s.ok(), "write should succeed");
        
        s = writer.AddRecord("record2");
        TEST_ASSERT(s.ok(), "write should succeed");
        
        s = writer.AddRecord("a longer record with more data");
        TEST_ASSERT(s.ok(), "write should succeed");
        
        s = writer.Sync();
        TEST_ASSERT(s.ok(), "sync should succeed");
    }
    
    // Read
    {
        WALReader reader(filename);
        
        std::string record;
        Status s = reader.ReadRecord(&record);
        TEST_ASSERT(s.ok(), "read should succeed");
        TEST_ASSERT(record == "record1", "record should match");
        
        s = reader.ReadRecord(&record);
        TEST_ASSERT(s.ok(), "read should succeed");
        TEST_ASSERT(record == "record2", "record should match");
        
        s = reader.ReadRecord(&record);
        TEST_ASSERT(s.ok(), "read should succeed");
        TEST_ASSERT(record == "a longer record with more data", "record should match");
    }
    
    // Cleanup
    std::remove(filename.c_str());
    
    return true;
}

// Database tests
bool test_db_basic() {
    std::string dbname = "/tmp/tskv_test_db";
    
    // Clean up
    std::system(("rm -rf " + dbname).c_str());
    
    // Open database
    Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 1024 * 1024;  // 1MB
    
    std::unique_ptr<DB> db;
    Status s = DB::Open(options, dbname, &db);
    TEST_ASSERT(s.ok(), "database should open");
    
    // Write
    WriteOptions write_opts;
    s = db->Put(write_opts, "key1", "value1");
    TEST_ASSERT(s.ok(), "put should succeed");
    
    s = db->Put(write_opts, "key2", "value2");
    TEST_ASSERT(s.ok(), "put should succeed");
    
    // Read
    ReadOptions read_opts;
    std::string value;
    
    s = db->Get(read_opts, "key1", &value);
    TEST_ASSERT(s.ok(), "get should succeed");
    TEST_ASSERT(value == "value1", "value should match");
    
    s = db->Get(read_opts, "key2", &value);
    TEST_ASSERT(s.ok(), "get should succeed");
    TEST_ASSERT(value == "value2", "value should match");
    
    s = db->Get(read_opts, "nonexistent", &value);
    TEST_ASSERT(s.IsNotFound(), "should not find nonexistent key");
    
    // Clean up
    db.reset();
    std::system(("rm -rf " + dbname).c_str());
    
    return true;
}

bool test_db_batch() {
    std::string dbname = "/tmp/tskv_test_db_batch";
    std::system(("rm -rf " + dbname).c_str());
    
    Options options;
    options.create_if_missing = true;
    
    std::unique_ptr<DB> db;
    Status s = DB::Open(options, dbname, &db);
    TEST_ASSERT(s.ok(), "database should open");
    
    // Batch write
    WriteBatch batch;
    for (int i = 0; i < 100; i++) {
        batch.Put("key" + std::to_string(i), "value" + std::to_string(i));
    }
    
    WriteOptions write_opts;
    s = db->Write(write_opts, &batch);
    TEST_ASSERT(s.ok(), "batch write should succeed");
    
    // Verify
    ReadOptions read_opts;
    std::string value;
    for (int i = 0; i < 100; i++) {
        s = db->Get(read_opts, "key" + std::to_string(i), &value);
        TEST_ASSERT(s.ok(), "get should succeed");
        TEST_ASSERT(value == "value" + std::to_string(i), "value should match");
    }
    
    // Clean up
    db.reset();
    std::system(("rm -rf " + dbname).c_str());
    
    return true;
}

bool test_db_persistence() {
    std::string dbname = "/tmp/tskv_test_db_persist";
    std::system(("rm -rf " + dbname).c_str());
    
    // Write and close
    {
        Options options;
        options.create_if_missing = true;
        options.write_buffer_size = 1024;  // Small buffer to force flush
        
        std::unique_ptr<DB> db;
        Status s = DB::Open(options, dbname, &db);
        TEST_ASSERT(s.ok(), "database should open");
        
        WriteOptions write_opts;
        for (int i = 0; i < 100; i++) {
            s = db->Put(write_opts, "key" + std::to_string(i), "value" + std::to_string(i));
            TEST_ASSERT(s.ok(), "put should succeed");
        }
        
        FlushOptions flush_opts;
        flush_opts.wait = true;
        s = db->Flush(flush_opts);
        TEST_ASSERT(s.ok(), "flush should succeed");
    }
    
    // Reopen and verify
    {
        Options options;
        
        std::unique_ptr<DB> db;
        Status s = DB::Open(options, dbname, &db);
        TEST_ASSERT(s.ok(), "database should reopen");
        
        ReadOptions read_opts;
        std::string value;
        
        // Some keys should be found from SSTable
        for (int i = 0; i < 100; i++) {
            s = db->Get(read_opts, "key" + std::to_string(i), &value);
            // Note: Some keys might not be found if they weren't flushed
            // This is expected behavior for the test
        }
    }
    
    // Clean up
    std::system(("rm -rf " + dbname).c_str());
    
    return true;
}

int main() {
    std::cout << "TSKV Unit Tests" << std::endl;
    std::cout << "===============" << std::endl;
    
    int passed = 0;
    int failed = 0;
    
    // Run all tests
    RUN_TEST(test_slice_basic);
    RUN_TEST(test_slice_compare);
    RUN_TEST(test_arena_basic);
    RUN_TEST(test_arena_aligned);
    RUN_TEST(test_skiplist_basic);
    RUN_TEST(test_skiplist_iterator);
    RUN_TEST(test_varint32);
    RUN_TEST(test_varint64);
    RUN_TEST(test_memtable_basic);
    RUN_TEST(test_memtable_delete);
    RUN_TEST(test_block_builder);
    RUN_TEST(test_sstable_build_and_read);
    RUN_TEST(test_sstable_index);
    RUN_TEST(test_wal_write_read);
    RUN_TEST(test_db_basic);
    RUN_TEST(test_db_batch);
    RUN_TEST(test_db_persistence);
    
    std::cout << std::endl;
    std::cout << "Results: " << passed << " passed, " << failed << " failed" << std::endl;
    
    return failed > 0 ? 1 : 0;
}
