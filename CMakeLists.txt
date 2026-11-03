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
# kvserver 的 Raft 模式使用正式 Worker/网络/持久化实现。
target_sources(kvserver PRIVATE
    src/raft/worker.cpp
    src/raft/config.cpp
    src/raft/core.cpp
    src/raft/apply.cpp
    src/raft/state.cpp
    src/raft/logstore.cpp
    src/raft/transport.cpp
    src/raft/rpc_codec.cpp
    src/raft/rpc_validate.cpp
    src/raft/command.cpp
    src/raft/state_machine.cpp
    src/app/storage_worker.cpp
    src/storage/storage.cpp
    src/net/protocol.cpp)

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

# M3：leader 处理复制回复；内存日志/保存模型，不使用 socket
add_executable(test_raft_append_response
    tests/test_raft_append_response.cpp
    src/raft/core.cpp)
target_include_directories(test_raft_append_response PRIVATE src)

# M4 第一步：提案构造、保存后同步，以及复用M1–M3的真实存储闭环
add_executable(test_raft_proposal
    tests/test_raft_proposal.cpp
    src/raft/core.cpp
    src/raft/apply.cpp
    src/raft/state.cpp
    src/raft/logstore.cpp
    src/storage/record.cpp)
target_include_directories(test_raft_proposal PRIVATE src)

# M4 第二步：从已确认的副本进度判断leader能提交到哪里
add_executable(test_raft_commit
    tests/test_raft_commit.cpp
    src/raft/core.cpp)
target_include_directories(test_raft_commit PRIVATE src)

# M4 follower提交与完整提交通知：真实存储，测试按顺序驱动Core
add_executable(test_raft_follower_commit
    tests/test_raft_follower_commit.cpp
    src/raft/core.cpp
    src/raft/apply.cpp
    src/raft/state.cpp
    src/raft/logstore.cpp
    src/storage/record.cpp)
target_include_directories(test_raft_follower_commit PRIVATE src)


# M6 第1步：Raft RPC 帧编解码（纯函数，无 socket）
add_executable(test_raft_rpc_codec
    tests/test_raft_rpc_codec.cpp
    src/raft/rpc_codec.cpp)
target_include_directories(test_raft_rpc_codec PRIVATE src)

# M6 第2步：四类 RPC 正文编解码（固定长度 / 条目数两道闸 / 截断 / 布尔严格）
add_executable(test_raft_rpc_codec_body
    tests/test_raft_rpc_codec_body.cpp
    src/raft/rpc_codec.cpp)
target_include_directories(test_raft_rpc_codec_body PRIVATE src)


# M6 第3步：入站语义校验（规则表 + 真实存储证明绕过校验的后果）
add_executable(test_raft_rpc_validate
    tests/test_raft_rpc_validate.cpp
    src/raft/rpc_validate.cpp
    src/raft/core.cpp
    src/raft/apply.cpp
    src/raft/state.cpp
    src/raft/logstore.cpp
    src/storage/record.cpp)
target_include_directories(test_raft_rpc_validate PRIVATE src)

# M6 第4步：Raft 配置解析（纯字符串，无 socket/文件系统）
add_executable(test_raft_config
    tests/test_raft_config.cpp
    src/raft/config.cpp)
target_include_directories(test_raft_config PRIVATE src)

# M6 第5步：Raft 非阻塞 transport（真实 loopback socket）
add_executable(test_raft_transport
    tests/test_raft_transport.cpp
    src/raft/transport.cpp
    src/raft/config.cpp
    src/raft/rpc_codec.cpp
    src/raft/rpc_validate.cpp
    src/raft/core.cpp
    src/storage/record.cpp)
target_include_directories(test_raft_transport PRIVATE src)
target_link_options(test_raft_transport PRIVATE
    "-Wl,--wrap=send" "-Wl,--wrap=connect")

# M7：Worker 启动恢复、缺失/损坏文件拒绝与数据目录排他锁
add_executable(test_raft_worker_init
    tests/test_raft_worker_init.cpp
    src/raft/worker.cpp
    src/raft/apply.cpp
    src/raft/config.cpp
    src/raft/core.cpp
    src/raft/state.cpp
    src/raft/logstore.cpp
    src/raft/transport.cpp
    src/raft/rpc_codec.cpp
    src/raft/rpc_validate.cpp
    src/raft/command.cpp
    src/raft/state_machine.cpp
    src/app/storage_worker.cpp
    src/storage/storage.cpp
    src/net/protocol.cpp
    src/storage/record.cpp)
target_include_directories(test_raft_worker_init PRIVATE src)
target_link_libraries(test_raft_worker_init PRIVATE Threads::Threads)


# M5：可控消息队列、3/5 Core对象、真实Raft存储重启与发布顺序
add_executable(test_raft_core_scenarios
    tests/test_raft_core_scenarios.cpp
    src/raft/core.cpp
    src/raft/apply.cpp
    src/raft/state.cpp
    src/raft/logstore.cpp
    src/storage/record.cpp)
target_include_directories(test_raft_core_scenarios PRIVATE src)

# M7：实际 Worker 线程测试和真实进程测试驱动，共用生产源码
set(RAFT_WORKER_TEST_SRC
    src/raft/worker.cpp
    src/raft/config.cpp
    src/raft/core.cpp
    src/raft/apply.cpp
    src/raft/state.cpp
    src/raft/logstore.cpp
    src/raft/transport.cpp
    src/raft/rpc_codec.cpp
    src/raft/rpc_validate.cpp
    src/raft/command.cpp
    src/raft/state_machine.cpp
    src/app/storage_worker.cpp
    src/storage/storage.cpp
    src/net/protocol.cpp
    src/storage/record.cpp)
add_executable(test_raft_worker_live
    tests/test_raft_worker_live.cpp ${RAFT_WORKER_TEST_SRC})
target_include_directories(test_raft_worker_live PRIVATE src)
target_link_libraries(test_raft_worker_live PRIVATE Threads::Threads)
target_link_options(test_raft_worker_live PRIVATE "-Wl,--wrap=epoll_wait")
add_executable(raft_worker_node
    tests/raft_worker_node.cpp ${RAFT_WORKER_TEST_SRC})
target_include_directories(raft_worker_node PRIVATE src)
target_link_libraries(raft_worker_node PRIVATE Threads::Threads)

# CRC 首次调用并发回归：进程放行 8 个线程前不预热 crc32。
add_executable(test_crc32_threads
    tests/test_crc32_threads.cpp src/storage/record.cpp)
target_include_directories(test_crc32_threads PRIVATE src)
target_link_libraries(test_crc32_threads PRIVATE Threads::Threads)

# M8：日志命令编解码（复用客户端 payload，内部 Noop 单字节0）
add_executable(test_raft_command
    tests/test_raft_command.cpp src/raft/command.cpp src/net/protocol.cpp)
target_include_directories(test_raft_command PRIVATE src)

# M8：派生 KV 的 Rebuild 打开方式；默认 Recover 保持不变
add_executable(test_storage_rebuild
    tests/test_storage_rebuild.cpp
    src/storage/storage.cpp src/storage/record.cpp
    src/app/storage_worker.cpp src/net/protocol.cpp)
target_include_directories(test_storage_rebuild PRIVATE src)
target_link_libraries(test_storage_rebuild PRIVATE Threads::Threads)

# M8：状态机只执行连续已提交命令，完成 KV 后推进 lastApplied
add_executable(test_raft_state_machine
    tests/test_raft_state_machine.cpp
    src/raft/state_machine.cpp src/raft/command.cpp
    src/storage/storage.cpp src/storage/record.cpp
    src/app/storage_worker.cpp src/net/protocol.cpp)
target_include_directories(test_raft_state_machine PRIVATE src)
target_link_libraries(test_raft_state_machine PRIVATE Threads::Threads)
