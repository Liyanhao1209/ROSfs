cmake_minimum_required(VERSION 3.14) # FetchContent 需要 3.11+
project(rosfs_core)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ------------------------------------------------------------------------------
# 1. Core Library (rosfs)
# ------------------------------------------------------------------------------
add_library(rosfs STATIC
    src/rosfs.cpp
)

# 配置 include 路径，让外部 target_link_libraries 后能自动找到头文件
target_include_directories(rosfs PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# 开启编译警告
if(MSVC)
    target_compile_options(rosfs PRIVATE /W4)
else()
    target_compile_options(rosfs PRIVATE -Wall -Wextra)
endif()

# ------------------------------------------------------------------------------
# 2. Unit Tests
# ------------------------------------------------------------------------------
option(BUILD_ROSFS_TESTS "Build rosfs unit tests" ON)

if(BUILD_ROSFS_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()