cmake_minimum_required(VERSION 3.10)

# ---------------------------------------------------------------------------
# Leanstore DB
# ---------------------------------------------------------------------------
project(
  LeanStore
  VERSION 0.1
  LANGUAGES CXX)

# ---------------------------------------------------------------------------
# Detect Architecture
# ---------------------------------------------------------------------------
if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  add_compile_options(-Wall -Wextra -mavx2 -mcx16 -m64)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -msse4 -msha")
else()
  add_compile_options(-Wall -Wextra)
endif()

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -fPIC -Wno-stringop-overflow")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG -O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  message(STATUS "Setting build type to `Debug` as none was specified.")
  set(CMAKE_BUILD_TYPE "Debug")
endif()

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" AND CMAKE_BUILD_TYPE MATCHES Debug)
  add_compile_options(-fstandalone-debug)
endif()

# ---------------------------------------------------------------------------
# Enable debug_mode flag (can be set via CMake command line or GUI)
# ---------------------------------------------------------------------------
option(DEBUG_MODE "Enable debug mode" OFF)

# If debug_mode is enabled, define DEBUG_MODE for the compiler
if(DEBUG_MODE)
  message(STATUS "Debug mode enabled.")
  add_definitions(-DDEBUG_MODE)  # Define DEBUG_MODE for the compiler
else()
  message(STATUS "Debug mode disabled.")
endif()

# ---------------------------------------------------------------------------
# Exmap Support Detection
# ---------------------------------------------------------------------------
include(CheckIncludeFiles)

check_include_files("linux/exmap.h" HAVE_LINUX_EXMAP)

if(HAVE_LINUX_EXMAP)
  message(STATUS "Exmap support detected. Enabling...")
  add_definitions(-DUSE_EXMAP)
  set(ENABLE_EXMAP ON)  # Store as a CMake variable
else()
  message(STATUS "Exmap support not found. Disabling...")
  set(ENABLE_EXMAP OFF)  # Store as a CMake variable
endif()

# Pass the flag to the compiler
if(ENABLE_EXMAP)
  add_definitions(-DENABLE_EXMAP)
endif()
# ---------------------------------------------------------------------------
# Expected directory structure.
# ---------------------------------------------------------------------------
set(BUILD_SUPPORT_DIR ${CMAKE_SOURCE_DIR}/build_support)
set(SRC_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/src/include ${CMAKE_SOURCE_DIR}/benchmark/src/include)
set(THIRD_PARTY_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/share_libs/include)

# ---------------------------------------------------------------------------
# Third-Parties & Dependencies
# ---------------------------------------------------------------------------
find_package(Threads REQUIRED)
find_package(TBB REQUIRED)
find_package(GTest REQUIRED)
find_package(gflags REQUIRED)
find_package(ZLIB REQUIRED)
find_package(benchmark REQUIRED)
find_package(PkgConfig REQUIRED)
add_subdirectory(share_libs)

# Add liburing
pkg_check_modules(LIBURING REQUIRED liburing)

set(DB_CLIENT_LIBS wiredtiger sqlite3 pqxx mysqlcppconn)
set(COMPRESS_LIBS
    ${ZLIB_LIBRARIES}
    zstd
    lz4
    sha256
    fsst
    ssl
    crypto
    gcrypt)

set(CORE_LIBS
    Threads::Threads
    uring
    gflags
    benchmark::benchmark
    tbb
    fmt
    yyjson
    typefold
    croaring
    fuse)

set(THIRD_PARTY_DEPENDENCIES "${CORE_LIBS}" "${DB_CLIENT_LIBS}" "${COMPRESS_LIBS}")

# ---------------------------------------------------------------------------
# clang-format & clang-tidy
# ---------------------------------------------------------------------------
find_program(CLANG_FORMAT_BIN NAMES clang-format clang-format-16)
if("${CLANG_FORMAT_BIN}" STREQUAL "CLANG_FORMAT_BIN-NOTFOUND")
  message(WARNING "LeanStore couldn't find clang-format.")
else()
  message(STATUS "LeanStore found clang-format at ${CLANG_FORMAT_BIN}")
endif()

find_program(
  CLANG_TIDY_BIN
  NAMES clang-tidy clang-tidy-16)

if("${CLANG_TIDY_BIN}" STREQUAL "CLANG_TIDY_BIN-NOTFOUND")
  message(WARNING "LeanStore couldn't find clang-tidy.")
else()
  message(STATUS "LeanStore found clang-tidy at ${CLANG_TIDY_BIN}")
endif()

# ---------------------------------------------------------------------------
# Make Targets
# ---------------------------------------------------------------------------
add_custom_target(format ${BUILD_SUPPORT_DIR}/run_clang_format.py ${CLANG_FORMAT_BIN} --source_dirs
                         ${CMAKE_SOURCE_DIR}/benchmark,${CMAKE_SOURCE_DIR}/src,${CMAKE_SOURCE_DIR}/test --fix --quiet)

add_custom_target(check-format ${BUILD_SUPPORT_DIR}/run_clang_format.py ${CLANG_FORMAT_BIN}
                               --source_dirs ${CMAKE_SOURCE_DIR}/benchmark,${CMAKE_SOURCE_DIR}/src,${CMAKE_SOURCE_DIR}/test --quiet)

add_custom_target(check-tidy ${BUILD_SUPPORT_DIR}/run_clang_tidy.py -clang-tidy-binary
                             ${CLANG_TIDY_BIN} -p ${CMAKE_BINARY_DIR} -only-diff)

add_custom_target(check-tidy-full ${BUILD_SUPPORT_DIR}/run_clang_tidy.py -clang-tidy-binary
                                  ${CLANG_TIDY_BIN} -p ${CMAKE_BINARY_DIR})

# ---------------------------------------------------------------------------
# Sources & Benchmark
# ---------------------------------------------------------------------------
file(GLOB_RECURSE LEANSTORE_SOURCE_FILES src/**.cc src/**/**.cc src/**/**.h)
add_library(leanstore STATIC ${LEANSTORE_SOURCE_FILES})
target_link_libraries(leanstore PUBLIC ${THIRD_PARTY_DEPENDENCIES})
target_include_directories(leanstore PUBLIC ${SRC_INCLUDE_DIR} ${THIRD_PARTY_INCLUDE_DIR})

add_subdirectory(benchmark)

# ---------------------------------------------------------------------------
# Testing & Sanitizer
# ---------------------------------------------------------------------------
option(ENABLE_TESTING "Build tests" OFF)
if(ENABLE_TESTING)
  enable_testing()
  set(CMAKE_BUILD_TYPE "DEBUG")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DENABLE_TESTING -D_FILE_OFFSET_BITS=64")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DENABLE_TESTING -D_FILE_OFFSET_BITS=64")
  add_subdirectory(test)
endif()

option(SANITIZER "Compile leanstore with sanitizer" OFF)
if(SANITIZER)
  message(STATUS "LeanStore enable sanitizer")
  target_compile_options(leanstore PUBLIC -fsanitize=address)
  target_link_libraries(leanstore PUBLIC asan)
endif()
