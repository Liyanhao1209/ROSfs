include(FetchContent)

message(STATUS "Fetching GoogleTest...")
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

include(GoogleTest)

# =========================================================
# 合并所有测试到一个可执行文件
# =========================================================
add_executable(rosbag_all_tests 
    test_rosbag.cpp
    test_concurrency.cpp
)

target_link_libraries(rosbag_all_tests PRIVATE 
    rosbag_core 
    GTest::gtest_main
)

gtest_discover_tests(rosbag_all_tests)