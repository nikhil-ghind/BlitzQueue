cmake_minimum_required(VERSION 3.16)
project(BlitzQueue VERSION 1.0.0 LANGUAGES CXX)

# ─────────────────────────────────────────────────────────────────────────────
# Global settings
# ─────────────────────────────────────────────────────────────────────────────

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# ─────────────────────────────────────────────────────────────────────────────
# Compiler flags
# ─────────────────────────────────────────────────────────────────────────────

add_compile_options(
  -Wall -Wextra -Wpedantic
  $<$<CONFIG:Release>:-O3 -march=native>
  $<$<CONFIG:Debug>:-g -fsanitize=address,thread>
)
add_link_options(
  $<$<CONFIG:Debug>:-fsanitize=address,thread>
)

# ─────────────────────────────────────────────────────────────────────────────
# Dependencies
# ─────────────────────────────────────────────────────────────────────────────

# Threads
find_package(Threads REQUIRED)

# Boost (header-only Asio + system)
find_package(Boost 1.66 REQUIRED COMPONENTS system thread)

# gRPC + Protobuf
find_package(PkgConfig REQUIRED)
pkg_check_modules(GRPC REQUIRED grpc++)
pkg_check_modules(PROTOBUF REQUIRED protobuf)

find_program(PROTOC protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

# ─────────────────────────────────────────────────────────────────────────────
# Proto codegen
# ─────────────────────────────────────────────────────────────────────────────

set(PROTO_DIR  ${CMAKE_SOURCE_DIR}/proto)
set(PROTO_FILE ${PROTO_DIR}/queue.proto)
set(PROTO_OUT  ${CMAKE_BINARY_DIR}/proto_gen)
file(MAKE_DIRECTORY ${PROTO_OUT})

add_custom_command(
  OUTPUT
    ${PROTO_OUT}/queue.pb.cc
    ${PROTO_OUT}/queue.pb.h
    ${PROTO_OUT}/queue.grpc.pb.cc
    ${PROTO_OUT}/queue.grpc.pb.h
  COMMAND ${PROTOC}
    --proto_path=${PROTO_DIR}
    --cpp_out=${PROTO_OUT}
    --grpc_out=${PROTO_OUT}
    --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
    ${PROTO_FILE}
  DEPENDS ${PROTO_FILE}
  COMMENT "Generating protobuf/gRPC sources"
)

add_library(blitz_proto STATIC
  ${PROTO_OUT}/queue.pb.cc
  ${PROTO_OUT}/queue.grpc.pb.cc
)
target_include_directories(blitz_proto PUBLIC ${PROTO_OUT})
target_link_libraries(blitz_proto PUBLIC
  ${PROTOBUF_LIBRARIES}
  ${GRPC_LIBRARIES}
)
target_compile_options(blitz_proto PRIVATE ${PROTOBUF_CFLAGS} ${GRPC_CFLAGS})

# ─────────────────────────────────────────────────────────────────────────────
# Interface library for the header-only queue
# ─────────────────────────────────────────────────────────────────────────────

add_library(blitz_queue INTERFACE)
target_include_directories(blitz_queue INTERFACE
  $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)
target_compile_features(blitz_queue INTERFACE cxx_std_17)

# ─────────────────────────────────────────────────────────────────────────────
# Executables
# ─────────────────────────────────────────────────────────────────────────────

# gRPC server
add_executable(blitz-server src/server.cpp)
target_link_libraries(blitz-server PRIVATE
  blitz_queue blitz_proto Threads::Threads)
target_include_directories(blitz-server PRIVATE ${PROTO_OUT})

# gRPC CLI client
add_executable(blitz-client src/client.cpp)
target_link_libraries(blitz-client PRIVATE
  blitz_queue blitz_proto Threads::Threads)
target_include_directories(blitz-client PRIVATE ${PROTO_OUT})

# Standalone MPMC benchmark
add_executable(blitz-bench src/benchmark.cpp)
target_link_libraries(blitz-bench PRIVATE blitz_queue Threads::Threads)

# Boost.Asio worker pool demo
add_executable(blitz-worker-pool src/async_worker_pool.cpp)
target_link_libraries(blitz-worker-pool PRIVATE
  blitz_queue
  Boost::system
  Boost::thread
  Threads::Threads
)
target_include_directories(blitz-worker-pool PRIVATE ${Boost_INCLUDE_DIRS})

# ─────────────────────────────────────────────────────────────────────────────
# Install rules
# ─────────────────────────────────────────────────────────────────────────────

include(GNUInstallDirs)

install(TARGETS blitz-server blitz-client blitz-bench blitz-worker-pool
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
install(DIRECTORY include/blitz_queue
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────

message(STATUS "BlitzQueue ${PROJECT_VERSION}")
message(STATUS "  Build type : ${CMAKE_BUILD_TYPE}")
message(STATUS "  C++ std    : ${CMAKE_CXX_STANDARD}")
message(STATUS "  Boost      : ${Boost_VERSION}")
message(STATUS "  gRPC libs  : ${GRPC_LIBRARIES}")
