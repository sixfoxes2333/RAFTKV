cmake_minimum_required(VERSION 3.16)
project(KV LANGUAGES CXX)

find_package(Threads REQUIRED)   #StorageWorker 的后台线程

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

#导出 compile_commands.json：编辑器（clangd / C/C++ 插件）据此拿到准确的 include 路径
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

#编译警告：必须写在所有 add_executable 之前，否则对已创建的 target 不生效（add_compile_options 只影响之后创建的 target）
add_compile_options(-Wall -Wextra)

#按层分组：网络层 / 存储层 / 应用层（协议编解码 + 后台存储线程）
set(NET_SRC
src/net/reactor.cpp
src/net/connection.cpp
src/base/buffer.cpp)

set(STORAGE_SRC
src/storage/storage.cpp
src/storage/record.cpp)

set(PROTO_SRC
src/net/protocol.cpp)

set(APP_SRC
src/app/storage_worker.cpp)

add_executable(kvserver
src/main.cpp
${NET_SRC}
${STORAGE_SRC}
${PROTO_SRC}
${APP_SRC})
target_link_libraries(kvserver PRIVATE Threads::Threads)   #StorageWorker 的后台线程

add_executable(kvclient
src/tools/client.cpp
${PROTO_SRC})

add_executable(test_storage
tests/test_storage.cpp
${STORAGE_SRC})

add_executable(test_noncopyable
tests/test_noncopyable.cpp
${NET_SRC})

add_executable(test_recovery
tests/test_recovery.cpp
${STORAGE_SRC})

add_executable(crash_writer
tests/crash_writer.cpp
${STORAGE_SRC})

add_executable(test_protocol
tests/test_protocol.cpp
src/net/protocol.cpp)

add_executable(test_storage_worker
tests/test_storage_worker.cpp
src/app/storage_worker.cpp
src/net/protocol.cpp
${STORAGE_SRC})

add_executable(kvbench
tests/kvbench.cpp
${PROTO_SRC})
target_include_directories(kvserver PRIVATE src)
target_include_directories(kvclient PRIVATE src)
target_include_directories(test_storage PRIVATE src)
target_include_directories(test_noncopyable PRIVATE src)
target_include_directories(test_recovery PRIVATE src)
target_include_directories(crash_writer PRIVATE src)
target_include_directories(test_protocol PRIVATE src)
target_include_directories(test_storage_worker PRIVATE src)
target_link_libraries(test_storage_worker PRIVATE Threads::Threads)
target_include_directories(kvbench PRIVATE src)
target_link_libraries(kvbench PRIVATE Threads::Threads)

#Raft：term/vote 持久化（单测 + LD_PRELOAD 故障注入在 raft_state_fault_test.py）
add_executable(test_raft_state
tests/test_raft_state.cpp
src/raft/state.cpp
src/storage/record.cpp)
target_include_directories(test_raft_state PRIVATE src)


#Raft：日志存储（单测 + LD_PRELOAD 故障注入在 logstore_fault_test.py）
add_executable(test_logstore
tests/test_logstore.cpp
src/raft/logstore.cpp)
target_include_directories(test_logstore PRIVATE src)


# Raft Core：初始化、发起选举与一次性内存分配失败
add_executable(test_raft_core
    tests/test_raft_core.cpp
    src/raft/core.cpp)
target_include_directories(test_raft_core PRIVATE src)
add_executable(test_raft_core_alloc
    tests/test_raft_core_alloc.cpp
    src/raft/core.cpp)
target_include_directories(test_raft_core_alloc PRIVATE src)

# Raft Core：内存三节点选举演练（无 socket、无真实计时器）
add_executable(test_raft_election
    tests/test_raft_election.cpp
    src/raft/core.cpp)
target_include_directories(test_raft_election PRIVATE src)

# Raft Core：日志复制请求的打包与接收（内存日志，无 socket、无磁盘）
add_executable(test_raft_append
    tests/test_raft_append.cpp
    src/raft/core.cpp)
target_include_directories(test_raft_append PRIVATE src)

# Raft 复制落地：真实 LogStore/StateStore 执行日志动作并生成回复
add_executable(test_raft_append_persist
    tests/test_raft_append_persist.cpp
    src/raft/core.cpp
    src/raft/apply.cpp
    src/raft/state.cpp
    src/raft/logstore.cpp
    src/storage/record.cpp)
target_include_directories(test_raft_append_persist PRIVATE src)
