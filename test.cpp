#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <cassert>
#include <fstream>
#include <sstream>
#include <random>
#include <algorithm>

#include "RTree.h"
#include "Type.h"
#include "Node.h"

using namespace SpatialStorage;

struct TestConfig {
    int dimensions = 2;      
    int key_size = 4 * sizeof(double);  
    int value_size = sizeof(uint64_t);  
    int block_size = 4096;   
    int test_count = 1000;   
    std::string data_file;   
    bool use_file = false;   
};

enum class Operation {
    INSERT,
    DELETE,
    OVERLAP_SEARCH,
    COMPRISE_SEARCH
};

struct TestData {
    Operation op;
    KeyType<double> key;
    uint64_t value;
    
    TestData(Operation o, const std::vector<double>& k, uint64_t v = 0)
        : op(o), key(k), value(v) {}
};

class BruteForceSearch {
private:
    std::vector<std::pair<KeyType<double>, uint64_t>> data;
    
public:
    void insert(const KeyType<double>& key, uint64_t value) {
        for (auto& item : data) {
            if (item.first == key) {
                item.second = value; 
                return;
            }
        }
        data.emplace_back(key, value);
    }
    
    bool remove(const KeyType<double>& key) {
        for (auto it = data.begin(); it != data.end(); ++it) {
            if (it->first == key) {
                data.erase(it);
                return true;
            }
        }
        return false;
    }
    
    std::vector<std::pair<KeyType<double>, uint64_t>> overlap_search(const KeyType<double>& query) {
        std::vector<std::pair<KeyType<double>, uint64_t>> result;
        for (const auto& item : data) {
            if (item.first.IsOverlap(query)) {
                result.push_back(item);
            }
        }
        return result;
    }
    
    std::vector<std::pair<KeyType<double>, uint64_t>> comprise_search(const KeyType<double>& query) {
        std::vector<std::pair<KeyType<double>, uint64_t>> result;
        for (const auto& item : data) {
            if (query >= item.first) { 
                result.push_back(item);
            }
        }
        return result;
    }
    
    size_t size() const { return data.size(); }
};

std::vector<TestData> generate_test_data(int count, int dimensions) {
    std::vector<TestData> test_data;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<double> coord_dist(0.0, 100.0);
    std::uniform_int_distribution<int> op_dist(0, 3);
    std::uniform_int_distribution<uint64_t> value_dist(1, 10000);
    
    for (int i = 0; i < count; ++i) {
        Operation op = static_cast<Operation>(op_dist(gen));
        std::vector<double> key_data;
        
        for (int j = 0; j < dimensions * 2; ++j) {
            double coord = coord_dist(gen);
            key_data.push_back(coord);
        }
        
        for (int d = 0; d < dimensions; ++d) {
            if (key_data[d] > key_data[d + dimensions]) {
                std::swap(key_data[d], key_data[d + dimensions]);
            }
        }
        
        uint64_t value = (op == Operation::INSERT) ? value_dist(gen) : 0;
        test_data.emplace_back(op, key_data, value);
    }
    
    return test_data;
}

std::vector<TestData> read_test_data_from_file(const std::string& filename, int dimensions) {
    std::vector<TestData> test_data;
    std::ifstream file(filename);
    
    if (!file.is_open()) {
        std::cerr << "无法打开文件: " << filename << std::endl;
        return test_data;
    }
    
    std::string line;
    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::string op_str;
        iss >> op_str;
        
        Operation op;
        if (op_str == "INSERT") op = Operation::INSERT;
        else if (op_str == "DELETE") op = Operation::DELETE;
        else if (op_str == "OVERLAP_SEARCH") op = Operation::OVERLAP_SEARCH;
        else if (op_str == "COMPRISE_SEARCH") op = Operation::COMPRISE_SEARCH;
        else continue;
        
        std::vector<double> key_data;
        double coord;
        for (int i = 0; i < dimensions * 2; ++i) {
            if (iss >> coord) {
                key_data.push_back(coord);
            }
        }
        
        uint64_t value = 0;
        if (op == Operation::INSERT) {
            iss >> value;
        }
        
        if (key_data.size() == dimensions * 2) {
            test_data.emplace_back(op, key_data, value);
        }
    }
    
    return test_data;
}

bool compare_results(
    const std::vector<std::pair<KeyType<double>, uint64_t>>& brute_force_result,
    const std::vector<KeyValuePair<KeyType<double>, uint64_t>>& rtree_result,
    const std::string& operation_info = "")
{
    if (brute_force_result.size() != rtree_result.size()) {
        std::cout << "\n=== 结果不一致 ===" << std::endl;
        std::cout << "暴力搜索找到 " << brute_force_result.size() << " 个结果" << std::endl;
        std::cout << "R树搜索找到 " << rtree_result.size() << " 个结果" << std::endl;
        
        // 打印暴力搜索的所有结果
        std::cout << "\n暴力搜索结果:" << std::endl;
        for (size_t i = 0; i < brute_force_result.size(); ++i) {
            const auto& key = brute_force_result[i].first;
            const auto& data = key.getData();
            std::cout << "  " << i << ": [";
            for (size_t j = 0; j < data.size() / 2; ++j) {
                std::cout << "(" << data[j] << "," << data[j + data.size()/2] << ")";
                if (j < data.size()/2 - 1) std::cout << " ";
            }
            std::cout << "] value=" << brute_force_result[i].second << std::endl;
        }
        
        // 打印R树搜索的所有结果
        std::cout << "\nR树搜索结果:" << std::endl;
        for (size_t i = 0; i < rtree_result.size(); ++i) {
            const auto& key = rtree_result[i].key;
            const auto& data = key.getData();
            std::cout << "  " << i << ": [";
            for (size_t j = 0; j < data.size() / 2; ++j) {
                std::cout << "(" << data[j] << "," << data[j + data.size()/2] << ")";
                if (j < data.size()/2 - 1) std::cout << " ";
            }
            std::cout << "] value=" << rtree_result[i].value << std::endl;
        }
        
        // 找出缺失的条目
        if (brute_force_result.size() > rtree_result.size()) {
            std::cout << "\nR树缺失的条目:" << std::endl;
            for (const auto& bf_item : brute_force_result) {
                bool found = false;
                for (const auto& rt_item : rtree_result) {
                    if (bf_item.first == rt_item.key && 
                        bf_item.second == rt_item.value) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    const auto& data = bf_item.first.getData();
                    std::cout << "  [";
                    for (size_t j = 0; j < data.size() / 2; ++j) {
                        std::cout << "(" << data[j] << "," << data[j + data.size()/2] << ")";
                        if (j < data.size()/2 - 1) std::cout << " ";
                    }
                    std::cout << "] value=" << bf_item.second << std::endl;
                }
            }
        }
        
        return false;
    }
    
    std::vector<std::pair<KeyType<double>, uint64_t>> bf_sorted = brute_force_result;
    std::vector<KeyValuePair<KeyType<double>, uint64_t>> rt_sorted = rtree_result;
    
    auto key_to_string = [](const KeyType<double>& key) {
        std::string result;
        const auto& data = key.getData();
        for (size_t i = 0; i < data.size() / 2; ++i) {
            result += "(" + std::to_string(data[i]) + "," + std::to_string(data[i + data.size()/2]) + ")";
            if (i < data.size()/2 - 1) result += " ";
        }
        return result;
    };
    
    std::sort(bf_sorted.begin(), bf_sorted.end(), 
              [&](const std::pair<KeyType<double>, uint64_t>& a, 
                  const std::pair<KeyType<double>, uint64_t>& b) { 
                  return key_to_string(a.first) < key_to_string(b.first); 
              });
    
    std::sort(rt_sorted.begin(), rt_sorted.end(), 
              [&](const KeyValuePair<KeyType<double>, uint64_t>& a,  
                  const KeyValuePair<KeyType<double>, uint64_t>& b) { 
                  return key_to_string(a.key) < key_to_string(b.key); 
              });
    
    for (size_t i = 0; i < bf_sorted.size(); ++i) {
        bool key_mismatch = (bf_sorted[i].first != rt_sorted[i].key);
        bool value_mismatch = (bf_sorted[i].second != rt_sorted[i].value);
        
        if (key_mismatch || value_mismatch) {
            std::cout << "\n=== 结果不一致 ===" << std::endl;
            std::cout << "在第 " << i << " 个结果处不匹配:" << std::endl;
            
            std::cout << "暴力搜索结果:" << std::endl;
            const auto& bf_key = bf_sorted[i].first;
            const auto& bf_data = bf_key.getData();
            std::cout << "  Key: [";
            for (size_t j = 0; j < bf_data.size() / 2; ++j) {
                std::cout << "(" << bf_data[j] << "," << bf_data[j + bf_data.size()/2] << ")";
                if (j < bf_data.size()/2 - 1) std::cout << " ";
            }
            std::cout << "]" << std::endl;
            std::cout << "  Value: " << bf_sorted[i].second << std::endl;
            
            std::cout << "R树搜索结果:" << std::endl;
            const auto& rt_key = rt_sorted[i].key;
            const auto& rt_data = rt_key.getData();
            std::cout << "  Key: [";
            for (size_t j = 0; j < rt_data.size() / 2; ++j) {
                std::cout << "(" << rt_data[j] << "," << rt_data[j + rt_data.size()/2] << ")";
                if (j < rt_data.size()/2 - 1) std::cout << " ";
            }
            std::cout << "]" << std::endl;
            std::cout << "  Value: " << rt_sorted[i].value << std::endl;
            
            if (key_mismatch) {
                std::cout << "错误类型: Key不匹配" << std::endl;
            }
            if (value_mismatch) {
                std::cout << "错误类型: Value不匹配" << std::endl;
            }
            
            return false;
        }
    }
    
    return true;
}

void run_test(const TestConfig& config) {
    std::cout << "=== R树测试开始 ===" << std::endl;
    std::cout << "维度: " << config.dimensions << std::endl;
    std::cout << "测试数量: " << config.test_count << std::endl;
    std::cout << "块大小: " << config.block_size << std::endl;
    
    std::vector<TestData> test_data;
    if (config.use_file && !config.data_file.empty()) {
        std::cout << "从文件读取测试数据: " << config.data_file << std::endl;
        test_data = read_test_data_from_file(config.data_file, config.dimensions);
        if (test_data.empty()) {
            std::cout << "文件为空或读取失败，使用随机数据" << std::endl;
            test_data = generate_test_data(config.test_count, config.dimensions);
        }
    } else {
        std::cout << "生成随机测试数据" << std::endl;
        test_data = generate_test_data(config.test_count, config.dimensions);
    }
    
    std::cout << "初始化对拍..." << std::endl;
    auto rtree = RTree<double, uint64_t>::create(AT_FDCWD, "test_rtree.index", 
                                      config.key_size, config.value_size,
                                      config.block_size, config.dimensions);
    
    BruteForceSearch brute_force;
    std::cout << "对拍初始化完成" << std::endl;
    
    int success_count = 0;
    int total_operations = test_data.size();
    double total_rtree_time = 0.0;
    double total_brute_force_time = 0.0;
    
    for (size_t i = 0; i < test_data.size(); ++i) {
        const auto& data = test_data[i];
        bool success = true;
        
        std::cout << "\n操作 " << (i + 1) << "/" << test_data.size() << ": ";
        
        switch (data.op) {
            case Operation::INSERT: {
                std::cout << "INSERT ";
                const auto& key_data = data.key.getData();
                for (double coord : key_data) {
                    std::cout << coord << " ";
                }
                std::cout << "value=" << data.value;
                
                auto start = std::chrono::high_resolution_clock::now();
                KeyValuePair<KeyType<double>, uint64_t> kvp{data.key, data.value};
                rtree.Insert(kvp);
                auto end = std::chrono::high_resolution_clock::now();
                double rtree_time = std::chrono::duration<double, std::milli>(end - start).count();
                
                start = std::chrono::high_resolution_clock::now();
                brute_force.insert(data.key, data.value);
                end = std::chrono::high_resolution_clock::now();
                double bf_time = std::chrono::duration<double, std::milli>(end - start).count();
                
                total_rtree_time += rtree_time;
                total_brute_force_time += bf_time;
                
                std::cout << " - R树: " << rtree_time << "ms, 暴力: " << bf_time << "ms";
                break;
            }
            
            case Operation::DELETE: {
                std::cout << "DELETE ";
                const auto& key_data = data.key.getData();
                for (double coord : key_data) {
                    std::cout << coord << " ";
                }
                
                auto start = std::chrono::high_resolution_clock::now();
                KeyValuePair<KeyType<double>, uint64_t> kvp{data.key, 0};
                bool rtree_result = rtree.Delete(kvp);
                auto end = std::chrono::high_resolution_clock::now();
                double rtree_time = std::chrono::duration<double, std::milli>(end - start).count();
                
                start = std::chrono::high_resolution_clock::now();
                bool bf_result = brute_force.remove(data.key);
                end = std::chrono::high_resolution_clock::now();
                double bf_time = std::chrono::duration<double, std::milli>(end - start).count();
                
                total_rtree_time += rtree_time;
                total_brute_force_time += bf_time;
                
                if (rtree_result != bf_result) {
                    std::cout << " - 错误: 删除结果不一致 (R树: " << rtree_result 
                              << ", 暴力: " << bf_result << ")";
                    success = false;
                } else {
                    std::cout << " - R树: " << rtree_time << "ms, 暴力: " << bf_time << "ms";
                }
                break;
            }
            
            case Operation::OVERLAP_SEARCH: {
                std::cout << "OVERLAP_SEARCH ";
                const auto& key_data = data.key.getData();
                for (double coord : key_data) {
                    std::cout << coord << " ";
                }

                auto start = std::chrono::high_resolution_clock::now();
                auto rtree_result = rtree.Overlap_Search(data.key);
                auto end = std::chrono::high_resolution_clock::now();
                double rtree_time = std::chrono::duration<double, std::milli>(end - start).count();
                
                start = std::chrono::high_resolution_clock::now();
                auto bf_result = brute_force.overlap_search(data.key);
                end = std::chrono::high_resolution_clock::now();
                double bf_time = std::chrono::duration<double, std::milli>(end - start).count();
                
                total_rtree_time += rtree_time;
                total_brute_force_time += bf_time;
                
                if (!compare_results(bf_result, rtree_result)) {
                    std::cout << " - 错误: 搜索结果不一致 (R树找到 " << rtree_result.size()
                              << " 个, 暴力找到 " << bf_result.size() << " 个)";
                    success = false;
                } else {
                    std::cout << " - 找到 " << bf_result.size() << " 个结果, R树: " 
                              << rtree_time << "ms, 暴力: " << bf_time << "ms";
                }
                break;
            }
            
            case Operation::COMPRISE_SEARCH: {
                std::cout << "COMPRISE_SEARCH ";
                const auto& key_data = data.key.getData();
                for (double coord : key_data) {
                    std::cout << coord << " ";
                }
                
                auto start = std::chrono::high_resolution_clock::now();
                auto rtree_result = rtree.Comprise_Search(data.key);
                auto end = std::chrono::high_resolution_clock::now();
                double rtree_time = std::chrono::duration<double, std::milli>(end - start).count();
                
                start = std::chrono::high_resolution_clock::now();
                auto bf_result = brute_force.comprise_search(data.key);
                end = std::chrono::high_resolution_clock::now();
                double bf_time = std::chrono::duration<double, std::milli>(end - start).count();
                
                total_rtree_time += rtree_time;
                total_brute_force_time += bf_time;
                
                if (!compare_results(bf_result, rtree_result)) {
                    std::cout << " - 错误: 搜索结果不一致 (R树找到 " << rtree_result.size()
                              << " 个, 暴力找到 " << bf_result.size() << " 个)";
                    success = false;
                } else {
                    std::cout << " - 找到 " << bf_result.size() << " 个结果, R树: " 
                              << rtree_time << "ms, 暴力: " << bf_time << "ms";
                }
                break;
            }
        }
        
        if (success) {
            success_count++;
        } else {
            std::cout << " [失败]";
        }
    }
    
    std::cout << "\n\n=== 测试结果 ===" << std::endl;
    rtree.PrintTree();
    std::cout << "总操作数: " << total_operations << std::endl;
    std::cout << "成功操作: " << success_count << std::endl;
    std::cout << "成功率: " << (success_count * 100.0 / total_operations) << "%" << std::endl;
    std::cout << "R树总时间: " << total_rtree_time << "ms" << std::endl;
    std::cout << "最终数据量: " << brute_force.size() << " 个条目" << std::endl;

    if (unlink("test_rtree.index") == 0) {
        std::cout << "已删除测试文件: test_rtree.index" << std::endl;
    } else {
        std::cout << "删除测试文件失败: test_rtree.index" << std::endl;
    }
}

void interactive_test() {
    TestConfig config;
    
    std::cout << "=== R树交互式测试 ===" << std::endl;
    std::cout << "选择输入方式:" << std::endl;
    std::cout << "1. 随机生成测试数据" << std::endl;
    std::cout << "2. 从文件读取测试数据" << std::endl;
    
    int choice;
    std::cin >> choice;
    
    if (choice == 2) {
        config.use_file = true;
        std::cout << "输入数据文件路径: ";
        std::cin >> config.data_file;
    } else {
        std::cout << "输入测试数据数量: ";
        std::cin >> config.test_count;
    }
    
    std::cout << "输入维度数: ";
    std::cin >> config.dimensions;
    
    run_test(config);
}

int main(int argc, char* argv[]) {
    if (argc > 1) {
        TestConfig config;
        
        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            if (arg == "-f" && i + 1 < argc) {
                config.use_file = true;
                config.data_file = argv[++i];
            } else if (arg == "-n" && i + 1 < argc) {
                config.test_count = std::stoi(argv[++i]);
            } else if (arg == "-d" && i + 1 < argc) {
                config.dimensions = std::stoi(argv[++i]);
            } else if (arg == "-b" && i + 1 < argc) {
                config.block_size = std::stoi(argv[++i]);
            }
        }
        
        run_test(config);
    } else {
        interactive_test();
    }
    
    return 0;
}