# RaftKV：基于 Raft 的持久化键值存储

使用 **C++17** 实现的固定成员 Raft KV，支持 `SET` / `GET` / `DEL`，包含选举、日志复制、持久化、提交与状态机执行，以及节点重启恢复。

网络层使用 **epoll Reactor**，后台 **RaftWorker** 驱动协议与复制，底层使用 **Bitcask 风格的追加日志与内存索引**。同一程序也保留单机模式，支持 group commit。

## 功能概览

- **Raft 协议**：任期与投票、日志新旧检查、冲突后缀修复、复制进度维护、当前任期多数派提交。
- **持久化与恢复**：HardState 和 Raft 日志落盘，CRC 校验、残尾处理、损坏拒绝启动、数据目录独占。
- **KV 执行链路**：SET/GET/DEL 均作为日志命令，提交后由状态机顺序执行；新 Leader 完成当前任期 no-op 的提交与执行后开放业务。
- **节点间通信**：非阻塞 TCP、连接握手、RPC ID、超时重试与迟到回复处理。
- **运行时处理**：check-quorum、客户端请求超时、后台结果通知及连接身份校验。
- **验证**：CTest 注册 29 个 C++ 测试目标，包含真实三进程选主、读写与 Leader 故障切换。

## 架构与请求链路

```text
客户端 SET / GET / DEL
        │
        ▼
Reactor 线程
  epoll → 连接读写 → 切帧与协议解析
        │ 请求队列
        ▼
RaftWorker 线程
  RaftCore：选举 / 复制进度 / 提交推进
     ├── StateStore / LogStore：任期、投票与日志持久化
     ├── Transport：与其他 Raft 节点通信
     └── StateMachine：顺序执行已提交命令
                 │
                 ▼
          StorageWorker 线程 → kv.cache
                 │ 完成结果
                 ▼
RaftWorker → 结果队列 + eventfd → Reactor → 原客户端连接
```

RaftWorker 持有 Core、Raft 存储、Transport 和状态机。客户端 Reactor 通过请求/结果队列及状态接口与后台交互；回包时使用 `(fd, connection_id)` 匹配连接。

### 数据文件

| 文件 | 内容 |
|---|---|
| `raft.state` | 当前任期与投票记录 |
| `raft.log` | Raft 日志条目 |
| `kv.cache` | 日志命令执行后生成的派生 KV 数据 |

Raft 模式启动时重建 `kv.cache`，仅执行经协议确认提交的日志前缀，不把本地日志尾部直接当成已提交数据。

单机模式走 `Reactor → StorageWorker → 数据文件`，不经过 Raft；`KV_SYNC_MODE` 只控制该模式。

## 构建与运行

需要 Linux、支持 C++17 的编译器、CMake 3.16+ 和 pthread。

```bash
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure
```

### 三进程集群测试

```bash
./build/cluster_test ./build/kvserver
```

测试启动三个实际 `kvserver` 进程，完成选主、SET/GET，再 SIGKILL Leader，等待新 Leader 并读取之前已确认的值。

### 手工启动 Raft 集群

先创建各节点的数据目录：

```bash
mkdir -p /tmp/raftkv-demo/n1 /tmp/raftkv-demo/n2 /tmp/raftkv-demo/n3
```

在三个终端中分别设置相同的成员列表：

```bash
export KV_RAFT_PEERS='1@127.0.0.1:19001,2@127.0.0.1:19002,3@127.0.0.1:19003'
```

随后在对应终端运行一条命令：

```bash
# 终端 1
KV_PORT=9001 KV_RAFT_ID=1 KV_RAFT_DATA=/tmp/raftkv-demo/n1 ./build/kvserver

# 终端 2
KV_PORT=9002 KV_RAFT_ID=2 KV_RAFT_DATA=/tmp/raftkv-demo/n2 ./build/kvserver

# 终端 3
KV_PORT=9003 KV_RAFT_ID=3 KV_RAFT_DATA=/tmp/raftkv-demo/n3 ./build/kvserver
```

| 环境变量 | 含义 |
|---|---|
| `KV_PORT` | 客户端监听端口，默认 9006 |
| `KV_RAFT_ID` | 本节点 ID |
| `KV_RAFT_PEERS` | 固定成员列表，格式为逗号分隔的 `id@host:port`，必须包含自己 |
| `KV_RAFT_DATA` | 本节点已存在的数据目录，不可与其他节点共用 |

Raft 监听端口取自成员列表中本节点的条目，没有独立的 `KV_RAFT_PORT`。客户端请求应发给当前可服务的 Leader；当前未实现自动转发。

### 单机模式

不设置三个 `KV_RAFT_*` 环境变量时运行单机模式。以下命令使用默认客户端端口 9006：

```bash
# 终端 1：启动服务，Ctrl+C 停止
./build/kvserver /tmp/raftkv-single.data

# 终端 2：客户端操作
./build/kvclient set name sixfoxes
./build/kvclient get name
./build/kvclient del name
```

单机模式支持 `KV_SYNC_MODE=always|everysec|off`：分别为每批同步后确认、周期同步和不主动同步。默认 `always`；`everysec/off` 的成功回复不表示该次写入已经完成 fsync。

## 正确性验证

当前测试全部为 C++，由 CTest 驱动。

| 层次 | 代表测试 | 覆盖内容 |
|---|---|---|
| 存储与恢复 | `test_storage`、`test_recovery`、`test_raft_state`、`test_logstore` | 编码格式、CRC、任期投票落盘、日志追加与截断、残尾和损坏处理 |
| 协议核心 | `test_raft_election`、`test_raft_append*`、`test_raft_commit`、`test_raft_follower_commit` | 选举、日志匹配、响应进度与提交规则 |
| 多节点 Core 场景 | `test_raft_core_scenarios` | 3/5 节点可控消息队列中的选举、复制与追平 |
| 网络协议 | `test_raft_rpc_codec*`、`test_raft_rpc_validate`、`test_raft_transport` | 编解码、非法输入、真实 loopback TCP 的握手与分帧 |
| 状态机与 Worker | `test_raft_state_machine`、`test_raft_worker_init`、`test_raft_worker_live` | KV 执行、启动恢复、线程生命周期、RPC ID、超时与迟到回复 |
| 进程级集群 | `cluster_test` | 三个实际服务进程的选主、SET/GET 与杀 Leader 后读取已确认值 |

当前三进程测试不等同于完整的网络分区、掉电或线性一致性验收。早期网络与磁盘故障注入驱动已从当前目录移除，可在 Git 历史中查找；历史结果与当前 CTest 覆盖分开看待。

### 修复记录

- **CRC 首次初始化竞争**：原实现使用共享可变表和初始化标志。通过并发首次调用与 TSan 复现后，改为函数内 `static const` 数组初始化；`test_crc32_threads` 保留冷启动回归。
- **Transport 连接生命周期 UAF**：发送失败或回复回调关闭连接后，外层仍可能使用已失效引用。曾通过受控失败路径与 ASan 复现，并修正关闭后的控制流。

上述 sanitizer 结果来自相应修复阶段，不代表当前全部业务路径都经过 sanitizer 验证。

## 性能记录：单机模式

以下为 `storage-v1.2` 阶段的单机模式历史测量，**不作为 Raft 集群吞吐或延迟数据**。

环境：WSL2 / Linux，64B value，键空间 1000，8 条连接、每连接 pipeline=16，使用 C++ 工具 `kvbench`。

| SET 场景 | 吞吐 |
|---|---:|
| 批提交前，每条写同步 | 170.5 ops/s |
| group commit，`always` | 10,281 ops/s |
| group commit，`everysec` | 98,210 ops/s |

`always` 与 `everysec` 的确认时机不同，不应只按吞吐比较。当前 Leader 提案路径尚未实现跨客户端的日志 group commit；Follower 接收多条日志的批量持久化与之不同。

压测工具入口：

```bash
./build/kvbench --help
```

## 当前边界

- **固定成员**：未实现动态成员变更。
- **日志不回收**：未实现快照、前缀压缩和 InstallSnapshot；Raft 恢复需要重新回放经确认提交的前缀，Bitcask 文件也未做 compaction。
- **读也走日志**：未实现 ReadIndex 或 Lease Read，GET 同样承担复制与日志写入开销。
- **无请求去重**：超时、断连或 `Error` 只表示未确认成功，不保证命令没有提交；重试没有 exactly-once 保证。
- **资源约束尚未闭合**：现有预算未完整覆盖 GET 完成结果和 Buffer 保留容量，Raft Transport 的发送预算及连接/帧超时仍待补充。
- **测试范围有限**：当前仓库未提供完整的集群磁盘故障、网络分区与线性一致性验证工具。
- **可信环境使用**：无认证、无 TLS；keydir 常驻内存，单条客户端消息上限为 16MiB。

## 源码导航

```text
src/
  main.cpp                 程序入口、运行模式与客户端请求接线
  base/                    缓冲区、大端编解码与上限常量
  net/                     epoll Reactor、连接与客户端协议
  storage/                 Bitcask 风格存储、记录格式与恢复
  app/                     StorageWorker
  raft/
    core/                  RaftCore、HardState、LogStore 与提交通知
    protocol/              RPC、日志命令编解码与语义校验
    transport/             节点配置与非阻塞 TCP
    node/                  RaftWorker 与 StateMachine
  tools/                   kvclient
tests/                     C++ 测试及 kvbench
CMakeLists.txt
```
