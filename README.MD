该README由AI生成
# R-Tree 磁盘存储实现

## 项目简介

这是一个基于 C++ 实现的磁盘存储 R-Tree 索引库，支持高效的空间数据存储和查询。项目采用内存映射文件技术，将 R-Tree 结构持久化到磁盘，同时提供内存级别的访问性能。

### 主要特性

- **磁盘持久化**: 使用内存映射文件技术，R-Tree 结构完全持久化到磁盘
- **空间查询**: 支持重叠查询（Overlap Search）和包含查询（Comprise Search）
- **维度灵活**: 支持任意维度的空间数据
- **惰性删除**: 采用惰性删除策略，避免频繁的树结构调整

## 组件架构

### 核心组件
R-Tree/

├── RTree.h/RTree.cpp # R-Tree 主要实现

├── Node.h # 节点管理和处理

├── Type.h # 空间数据类型定义

├── FileCache.h/FileCache.cpp # 文件缓存管理

├── test.cpp # 测试和验证程序

└── visual.cpp # 可视化演示程序


### 架构说明

1. **FileCache**: 基于内存映射的文件缓存层，负责磁盘与内存之间的数据交换
2. **NodeHandler**: 节点处理器，封装了对 R-Tree 节点的各种操作
3. **RTree**: 核心 R-Tree 实现，提供插入、删除、查询等操作
4. **KeyType**: 通用的空间数据类型，支持多维矩形区域

### 磁盘布局
+-------------------+

| Index Header | (4096字节)

+-------------------+

| Root Node | (block_size)

+-------------------+

| Child Nodes | (block_size * n)

+-------------------+

每个节点包含：
- NodeHeader: 节点元数据（类型、条目数、文件偏移）
- KV 对数组: 内部节点存储子节点指针，叶子节点存储实际数据

## 编译构建

### 依赖要求

- C++17 兼容编译器
- CMake 3.10+
- SFML (仅可视化组件需要)

### 构建步骤

```bash
# 克隆项目
git clone <repository-url>
cd R-Tree

# 创建构建目录
mkdir build && cd build

# 配置项目
cmake ..

# 编译
make

# 运行测试
./test

# 运行可视化演示 (需要SFML)
./visual
```

## 测试指南
### 自动测试
项目包含完整的测试框架，支持多种测试模式：

#### 交互式测试
```bash
./test
```

#### 命令行测试
```bash
./test -n 1000 -d 2                    # 1000个随机操作，2维数据
./test -f operations.txt -d 2          # 从文件读取测试数据
```

### 测试数据格式
测试文件格式示例：
```PlainText
text
INSERT 10.0 20.0 30.0 40.0 100
INSERT 50.0 60.0 70.0 80.0 200
OVERLAP_SEARCH 15.0 25.0 35.0 45.0
DELETE 10.0 20.0 30.0 40.0
COMPRISE_SEARCH 0.0 0.0 100.0 100.0
```

### 验证机制
暴力搜索对比: 所有 R-Tree 查询结果都与暴力搜索对比验证

性能统计: 记录每个操作的执行时间

内存泄漏检测: 自动清理测试文件

## 样例代码

```cpp
#include "RTree.h"
#include "Type.h"

using namespace SpatialStorage;

// 创建 R-Tree 实例
auto rtree = RTree<double, uint64_t>::create(
    AT_FDCWD, 
    "my_index.index", 
    4 * sizeof(double),    // key 大小 (2维矩形)
    sizeof(uint64_t),      // value 大小
    4096,                  // 块大小
    2                      // 维度
);

// 准备数据
std::vector<double> rect_data = {10.0, 20.0, 30.0, 40.0}; // [x1, y1, x2, y2]
KeyType<double> key(rect_data);
uint64_t value = 12345;

// 创建键值对
KeyValuePair<KeyType<double>, uint64_t> kvp(key, value);

// 插入数据
rtree.Insert(kvp);

// 重叠查询
std::vector<double> query_rect = {15.0, 25.0, 35.0, 45.0};
KeyType<double> query_key(query_rect);
auto results = rtree.Overlap_Search(query_key);

// 包含查询
auto contained_results = rtree.Comprise_Search(query_key);

// 删除数据
rtree.Delete(kvp);

// 打印树结构
rtree.PrintTree();

// 获取所有条目
auto all_entries = rtree.GetAllEntries();
std::cout << "总条目数: " << all_entries.size() << std::endl;
```

## 可视化演示
```bash
# 准备操作文件
echo "INSERT 10 20 30 40 100" > operations.txt
echo "INSERT 50 60 70 80 200" >> operations.txt
echo "OVERLAP_SEARCH 15 25 35 45" >> operations.txt

# 运行可视化
./visual operations.txt
```