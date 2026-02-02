include(FetchContent)

message(STATUS "Fetching GoogleTest...")

FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)
# 强制 GTest 使用共享 CRT
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(googletest)

# 定义测试可执行文件
add_executable(rosfs_test test_rosfs.cpp)

# 链接核心库和 GTest
target_link_libraries(rosfs_test PRIVATE 
    rosfs 
    GTest::gtest_main
)

# 注册到 CTest
include(GoogleTest)
gtest_discover_tests(rosfs_test)