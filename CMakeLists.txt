cmake_minimum_required(VERSION 3.14)
project(rosbag_core)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 1. 核心库
add_library(rosbag_core STATIC
    src/bag.cpp
)

target_include_directories(rosbag_core PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# 开启常用警告
if(MSVC)
    target_compile_options(rosbag_core PRIVATE /W4)
else()
    target_compile_options(rosbag_core PRIVATE -Wall -Wextra)
endif()

# 2. 单元测试
option(BUILD_ROSBAG_TESTS "Build rosbag unit tests" ON)
if(BUILD_ROSBAG_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()