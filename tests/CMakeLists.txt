include(FetchContent)

message(STATUS "Fetching GoogleTest...")
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# 必须先 include 才能使用 gtest_discover_tests
include(GoogleTest)

# =========================================================
# 合并所有测试到一个可执行文件
# =========================================================
add_executable(rosfs_all_tests 
    test_rosfs.cpp 
    test_concurrency.cpp
)

target_link_libraries(rosfs_all_tests PRIVATE 
    rosfs 
    GTest::gtest_main
)

# 注册这个唯一的测试程序
gtest_discover_tests(rosfs_all_tests)