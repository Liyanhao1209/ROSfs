cmake_minimum_required(VERSION 3.14)
project(tskv VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Compiler flags for high performance
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -march=native -mtune=native")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fsanitize=address,undefined")

# Add compile options
add_compile_options(
    -Wall
    -Wextra
    -Wpedantic
    -Wno-unused-parameter
)

# TSKV library (header-only for now)
add_library(tskv INTERFACE)
target_include_directories(tskv INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# Link pthread for threading support
find_package(Threads REQUIRED)
target_link_libraries(tskv INTERFACE Threads::Threads)

# Benchmarks
option(TSKV_BUILD_BENCHMARKS "Build benchmarks" ON)
if(TSKV_BUILD_BENCHMARKS)
    add_executable(tskv_benchmark benchmark/benchmark.cpp)
    target_link_libraries(tskv_benchmark PRIVATE tskv)
    target_compile_options(tskv_benchmark PRIVATE -O3 -march=native)
endif()

# Tests
option(TSKV_BUILD_TESTS "Build tests" ON)
if(TSKV_BUILD_TESTS)
    enable_testing()
    
    add_executable(tskv_test test/test_main.cpp)
    target_link_libraries(tskv_test PRIVATE tskv)
    add_test(NAME tskv_test COMMAND tskv_test)
endif()

# Examples
add_executable(tskv_example test/example.cpp)
target_link_libraries(tskv_example PRIVATE tskv)

# Install
include(GNUInstallDirs)
install(TARGETS tskv
    EXPORT tskvTargets
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(DIRECTORY include/tskv
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(EXPORT tskvTargets
    FILE tskvTargets.cmake
    NAMESPACE tskv::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/tskv
)

# Package config
include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/tskvConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/tskvConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/tskvConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/tskv
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/tskvConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/tskvConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/tskv
)
