cmake_minimum_required(VERSION 3.16)
project(KV LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

#导出 compile_commands.json：编辑器（clangd / C/C++ 插件）据此拿到准确的 include 路径
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

#按层分组：网络层 / 存储层
set(NET_SRC
src/net/reactor.cpp
src/net/connection.cpp
src/base/buffer.cpp)

set(STORAGE_SRC
src/storage/storage.cpp
src/storage/record.cpp)

add_executable(kvserver
src/main.cpp
${NET_SRC})

add_executable(kvclient
src/tools/client.cpp)

add_executable(test_storage
tests/test_storage.cpp
${STORAGE_SRC})

target_include_directories(kvserver PRIVATE src)
target_include_directories(kvclient PRIVATE src)
target_include_directories(test_storage PRIVATE src)
