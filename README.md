# KV — 单节点持久化 KV 存储（C++17 / epoll）

一个从零手写的持久化键值存储服务：epoll 事件循环 + 后台存储线程 + 二进制协议 + Bitcask 风格日志存储。
支持 `SET` / `GET` / `DEL`；默认 `always` 模式按批 `fsync` 后确认写入，另有 `everysec/off` 模式。

> 当前状态：单节点 KV（`storage-v1.2`）+ 三节点 Raft KV（`raft-v1`）。测试全部为 C++，由 CTest 驱动：**29 个目标全绿**。
> 真三进程已验证：选主、经 Raft 提交的 SET/GET、SIGKILL leader 后新 leader 仍能读到已确认写；另有 Worker 层真实 TCP 的 ID/超时/迟到回复围栏。
> 已知边界（见 §5）：日志不回收、无 ReadIndex/请求去重/InstallSnapshot；**网络与磁盘故障注入的注入器不在仓库内**，`kill -9` 不等于掉电。

```bash
./build/kvserver /tmp/db.data &          # 启动（默认 ./kvdb.data）
./build/kvclient set name sixfoxes       # → OK      （退出码 0）
./build/kvclient get name                # → sixfoxes（退出码 0）
./build/kvclient get nokey               # → NOT_FOUND（退出码 1）
kill -9 %1 && ./build/kvserver /tmp/db.data &   # 重启后数据仍在
```

Raft 模式的三进程验收（纯 C++，自动分配端口并清理进程）：

```bash
./build/cluster_test ./build/kvserver
```

手工起单个 Raft 节点需要四个环境变量：`KV_PORT`、`KV_RAFT_ID`、`KV_RAFT_PEERS`、`KV_RAFT_DATA`。
Raft 数据目录须已存在，PEERS 包含自己且格式为 `id@host:port`，Raft 监听端口从自己那条取得（没有独立的 KV_RAFT_PORT）。不设这三个 `KV_RAFT_*` 变量即为单机模式。

---


## 1. 架构

下图为单机 KV 路径。Raft 模式由独立 RaftWorker 拥有 Core/存储/transport，客户端 Reactor 通过加锁快照观察状态；客户端请求经日志提交后由派生 KV 执行，结果回传给原连接。

```text
                    ┌───────────────────────── Reactor 线程 ─────────────────────────┐
  TCP 客户端 ──────►│ epoll_wait → Connection.on_read → 按 4 字节长度头切帧            │
   SET/GET/DEL      │            → parse_request（协议校验）                        │
                    │            → StorageWorker::submit(StorageTask)  ← 只入队，不等 │
                    │            ↑                                                  │
                    │ eventfd 可读 → take_completed()                               │
                    │            → find_connection(fd, connection_id)  ← 身份校验     │
                    │            → encode_response → 发送队列 → EPOLLOUT 驱动写出     │
                    └───────────────────────────────────────────────────────────────┘
                                       │ 任务队列（bounded）      ▲ 完成队列 + eventfd
                    ┌──────────────────▼──────────────────────────┴───────────────────┐
                    │                  StorageWorker 线程（唯一一个）                  │
                    │  put / get / del → fsync → 更新内存索引（keydir）                │
                    └───────────────────────────────────────────────────────────────┘
                                       │
                              append-only 日志文件（Bitcask）
                              [header_crc][payload_crc][type][klen][vlen][key][value]
```

代码分层（`src/` 下每层只依赖左边）：

| 层 | 文件 | 职责 |
|---|---|---|
| base | `buffer.{h,cpp}` `be32.h` `limits.h` | 字节缓冲、大端编解码、全局上限常量 |
| storage | `record.{h,cpp}` `storage.{h,cpp}` | 记录编解码（两级 CRC）、追加写/预读、恢复、墓碑删除 |
| net | `reactor.{h,cpp}` `connection.{h,cpp}` `protocol.{h,cpp}` | epoll 事件循环、连接生命周期、协议编解码 |
| app | `storage_worker.{h,cpp}` | 后台存储线程 + eventfd 完成通知（Storage 的唯一访问者） |
| tools | `client.cpp` | `kvclient` 命令行客户端 |
| raft | `state` / `logstore` / `core` / `rpc_codec` / `rpc_validate` / `config` / `transport` / `worker` | Raft 持久化、协议状态、编解码与入站校验、非阻塞 TCP、后台事件驱动 |

---

## 2. 关键设计取舍

### 2.1 存储引擎：Bitcask（追加日志 + 内存索引），不是 LSM

- 写入 = 追加一条记录 + `fsync`，然后更新内存 `keydir`（key → 文件偏移/长度）。
- 读 = 查内存索引 → `pread` 精确读一条记录 → 校验 CRC。
- 删除 = 追加一条墓碑记录（崩溃恢复时才能区分"删过"和"从没写过"）。

**为什么不用 LSM**：LSM 真正的难点在 compaction 的正确性，自研大约要 2–4 周；
而本项目的主线是 Raft，换引擎既不会提高一致性能力，也**不会消除 Raft 日志**
（Raft 日志本身是另一份日志）。所以这里选择把 Bitcask 做扎实，接口上留出可替换空间。

**代价（已量化）**：keydir 必须常驻内存 —— 1M 个小 key 约 100MB 量级（**估算**，未实测）；
另外日志只追加不回收，需要后续做 compaction（未实现，见"已知限制"）。

### 2.2 记录格式 v2：头校验和，用来区分"残尾"和"损坏"

```text
[header_crc:4][payload_crc:4][type:1][key_len:4][val_len:4][key][value]
 └─ 覆盖后 13 字节 ─┘          └──── 被 payload_crc 覆盖 ────┘
```

恢复时的判定表：

| 情况 | 判定 | 动作 |
|---|---|---|
| 剩余字节 < 17（头都不完整） | 只可能发生在文件尾 → **残尾** | `ftruncate` 截断，继续 |
| `header_crc` 不匹配 | **损坏**（不是残尾，因为中间记录不可能只剩半个头） | 拒绝启动，文件不动 |
| 头通过但剩余 < 整条长度 | 残尾 | 截断，继续 |
| `payload_crc` 不匹配 | 损坏 | 拒绝启动 |

代价：每条记录多 4 字节（小记录 15→19 字节，+26.7%；1KB value 只 +0.4%）。
相对每条的 `fsync`（毫秒级）可以忽略。

### 2.3 失败语义：分"普通失败"和"提交结果不确定"

```text
写失败 + 回滚（ftruncate）成功   → 普通失败：文件回到写入前，对象仍可用
写失败 + 回滚失败 / fsync 失败   → poisoned：结果不确定，拒绝一切后续写，good() 返回 false
```

`poisoned` 在服务层会变成**非 0 退出码**，而不是静默回一堆 `Error` —— 让运维能看见。

### 2.4 存储操作全部收敛到一个后台线程（无锁），代价是串行

Reactor 线程只做协议解析和投递，`Storage` 只被 worker 线程访问，因此存储层**不需要任何锁**；
两个线程之间只有有界队列 + eventfd 通知。收益用一个可证伪的实验验证：

```text
注入 800ms 慢 fsync，同时用另一个连接发一个"不经过存储"的请求（协议错误 → 直接回 Error）
  空闲基线快路径    0.05ms
  慢 fsync 期间    0.10ms      ← 事件循环没有被 fsync 堵住
  慢 fsync 期间新建连接 0.10ms  ← accept 也不受影响
```

**代价也必须说清楚**：单 worker 意味着存储操作串行。同一个实验里，另一个连接的 `GET`
要排在慢 `SET` 后面，实测 **658ms**。【批提交之前】压测还能看到：`SET` 吞吐与并发/流水线**无关** ——
闭环 C=1 是 179 ops/s，流水线 p=16 C=8 也只有 171 ops/s，而 P50 从 4.6ms 涨到 543ms；
做完 group commit（§2.10）后，同样口径是 **10 281 ops/s、P50 9.1ms**（见 §3.1）。

### 2.5 所有"会增长的东西"都有上限（内存 / 连接 / 时间）

对端**不读、发一半、猛灌**都不能把服务器拖垮。现在的上限（都有围栏测试）：

| 位置 | 上限 | 超限行为 | 围栏 |
|---|---|---|---|
| 发送队列 `out_` | 高水位 2MB 暂停读；低水位 512KB 恢复（迟滞）；硬上限 17MB | 高水位 → 真的摘掉内核 `EPOLLIN`（转成 TCP 背压）；硬上限 → 关连接 | `net_outqueue` / `net_pause_read` |
| 后台任务队列 | **1024 条** 且 全局 **64MB** / 单连接 **32MB**（请求体字节） | 明确回 `Error`；该连接还有在途请求时同时关连接 | `reply_order` / `queue_bytes` |
| 收包缓冲 `in_` | 单连接天然 ≤ 4+`MAX_MESSAGE` ≈ 16MB；**全局 128MB** | 关掉正在长大的那条连接 | `in_buffer_budget` |
| 半条帧 | **30s 无进展**（`KV_FRAME_TIMEOUT_MS`） | 关连接（slow-loris 防护） | `net_frame_timeout` |
| 单条消息 | `MAX_MESSAGE` 16MB | 协议错误 → 关连接 | `kv_protocol_test` |

实测：正常压测下服务器 `VmHWM` **3.6MB**；
20 条连接各发 14MB "半条帧"（合计 280MB）时，全局收包预算逐条关连接、进程存活、`VmHWM` 139.9MB（外部限制 192MB）。

三个细节值得记：

- **条数和字节两条上限都要**：条数兜"任务结构体本身"的开销（几字节的请求也占约 100 字节结构体），字节兜请求体；
- **字节记账要含"正在执行"的那条**：worker 手里那条仍占内存，只有完成结果被 Reactor 取走才算离开队列（否则账会一直涨、最后假满）；
- **单连接上限必须 > 单条最大消息**：否则最大消息永远排不进队列（这是"防护"和"功能"的边界）。

### 2.6 异步结果的配对：`connection_id`

后台线程完成请求时，发起它的连接可能已经关闭，而 fd 号可能已被新连接复用。
所以每个 `Connection` 由 Reactor 分配一个单调递增的 `connection_id`，
回包前用 `(fd, connection_id)` 查找，对不上就丢弃 —— 否则会把 A 的结果发给 B。

### 2.7 半关闭语义：待发数据发完才关，且不能漏掉"在途请求"

对端 `shutdown(SHUT_WR)` 后仍然要收完回复。异步化之后"发送队列为空"**不再等于**"可以关闭"：
请求可能还在后台排队。因此 `Connection` 记录在途请求数，关闭条件为：

```text
peer_closed && out_ 为空 && pending_requests == 0
```

（这条 bug 被当时的半关闭大回复用例逮到，而它的注释在四天前就预言了这个场景。）

### 2.8 回复顺序 = 请求顺序（异步化之后必须显式保证）

协议里没有请求 id，客户端**只能按顺序**把回复和请求配对 —— 所以顺序就是语义。
把存储搬到后台线程之后，服务端就有了两条回包路径：正常请求走完成队列（晚），
解析失败当场回 `Error`（早）—— 于是流水线下错误回复会插队，客户端会把 `Error` 记到
一条其实写成功了的请求上，而且**无法察觉**。

修法（方案 A）：解析失败也投一个"合成任务"占位，让它的错误回复和正常回复走**同一条**完成队列。
顺带定下过载语义：只有"这条连接还有在途请求"（顺序真的会被破坏）时才回 `Error` 并关连接；
全新连接只回 `Error`、连接保留（否则过载时会把所有客户端一起杀掉）。

### 2.9 两个"不许静默"的例子

- **打开即加锁**：`recover()` 会 `ftruncate` 残尾，是**有副作用**的恢复，它的正确性依赖"我是这个库唯一的所有者"。
  第二个实例带着同样逻辑进场，会把"正在被写的那条记录"当成崩溃残尾剪掉（实测：数据静默消失）。
  所以 `Storage` 在 `open` 之后、`recover` 之前 `flock(LOCK_EX|LOCK_NB)`，拿不到就拒绝启动。
- **注册失败要响亮**：`epoll_ctl(ADD)` 失败曾经只打印一行就 `return`，调用者完全不知道 ——
  后果是进程挂在 `epoll_wait` 里**活着但不服务**（实测：监听 fd 失败时端口都没了；
  worker 的 eventfd 失败时端口还能连、但永远不回包）。现在注册失败就是非 0 退出。

---

### 2.10 持久化策略：三种提交模式（`KV_SYNC_MODE`）

单条 fsync 要 4.6ms，所以"每条写各自 fsync"把写入钉在 ~180 ops/s。批提交让**一次 fsync 覆盖一批**；
三种模式共用同一个批提交器，区别只在"什么时候落盘"：

| 模式 | 什么时候 fsync | 契约 | 实测（p=16 C=8） |
|---|---|---|---:|
| `always`（默认） | 每批提交前 | 已确认的写，掉电也不丢 | 10 281 ops/s |
| `everysec` | 最多每秒一次 | 掉电最多丢 1 秒（页缓存语义） | 98 210 ops/s |
| `off` | 不主动落盘 | 只用于对照实验 | 101 489 ops/s |

注意 `everysec` 和 `off` 几乎同速（98 210 vs 101 489，差 3%）：**"每秒落一次盘"的成本可以忽略，
真正的成本是"每条写都落盘"** —— 这也是工业界普遍把每秒一次作为默认档的原因（Redis 的 `appendfsync everysec` 同理）。

两个刻意的设计决定：

- **攒批不等待**：只搬"此刻已经排在队列里的任务"，不为了凑更大的批而睡一会儿 —— 所以单条请求的延迟不变
  （闭环 P50 仍是 4.6ms），收益全部来自并发/流水线带来的自然批量；
- **一批失败要一起失败**：这一批的 fsync 失败时，批里**所有写**都回 `Error`（不能只报一半，
  让另一半个客户端当成成功）——"提交结果不确定"与"普通失败"严格区分（见 §2.3）。

一个容易混淆的点：`kill -9` 与**掉电**不是一回事 —— `kill -9` 只杀进程，**页缓存还在内核里**，
所以 `everysec` 模式下已确认的写仍然活着（本项目的 `kill -9` 测试证明的是"库能打开、数据没被写坏"）；
`always` 才是"连内核页缓存都丢了（掉电）也不丢"。

---

## 3. 实测数据

环境：WSL2 / Linux，64 字节 value，键空间 1000，客户端为 **C++ 压测客户端 `tests/kvbench`**
（每线程一条连接、请求模板预热时预编码、**逐请求真实延迟**（发送时刻入队、FIFO 配对）、
样本合并后再算分位数，并报告客户端 CPU 以确认客户端不是瓶颈）。服务端 `TCP_NODELAY` 已开。

```bash
cmake --build build -j && ./build/kvserver /tmp/kv_bench.data &
./build/kvbench --op set --conns 1 --pipeline 1 --keyspace 1000 --value-size 64 --duration 5   # 先预置键空间
./build/kvbench --op get --conns 8 --pipeline 16 --keyspace 1000 --duration 3
```

### 3.1 写入：一次 fsync 提交一批（批提交前 vs 后的对照）

| 口径 | QPS | 平均 | P50 | P90 | P99 |
|---|---:|---:|---:|---:|---:|
| 口径 | QPS | 平均 | P50 | P99 |
|---|---:|---:|---:|---:|
| **批提交前**：SET 流水线 p=16 C=8 | 170.5 | 543ms | **543ms** | 582ms |
| SET 闭环 C=1（一次 fsync 的单条延迟） | 178.2 | 4.67ms | **4.60ms** | 5.66ms |
| SET 流水线 p=16 C=1（`always`） | 1 299 | 7.2ms | 8.96ms | 10.1ms |
| SET 流水线 p=16 C=8（`always`） | **10 281** | 9.3ms | 9.12ms | 10.9ms |
| SET 流水线 p=16 C=1（`everysec`） | 48 811 | 142µs | 134µs | 313µs |
| SET 流水线 p=16 C=8（`everysec`） | **98 210** | 903µs | 833µs | 1.39ms |

→ **一次 fsync 提交一批**之后：写入 **170.5 → 10 281 ops/s（60×）**，P50 **543ms → 9.1ms（59×）**。
→ **单条请求的延迟没变**（闭环 P50 仍是 4.60ms = 一次 fsync 的本体）：攒批只取"已经排队的请求"，
  不等待新请求 —— 这是刻意设计（等待会引入固定延迟，见 §2.10）。
→ `everysec` 模式（最多每秒落一次盘）进一步到 **98 210 ops/s**，代价是契约变成"掉电最多丢 1 秒"。

### 3.2 读取：流水线才是重点（旧口径"~38k 上限"是闭环假象）

| 口径 | QPS | P50 | P99 |
|---|---:|---:|---:|
| GET 闭环 C=1 | 5 602 | 127µs | 184µs |
| GET 闭环 C=4 | 20 159 | 147µs | 224µs |
| GET 闭环 C=8 | 24 827 | 232µs | 345µs |
| GET 闭环 C=16 | 26 182 | 441µs | 648µs |
| GET 闭环 C=64 | 28 482 | 1 628µs | 2 143µs |
| **GET 流水线 p=16 C=1** | **51 343** | 120µs | 238µs |
| **GET 流水线 p=16 C=8** | **84 968** | 1 000µs | 1 502µs |

→ 同并发对比：C=1 **5.6k → 51.3k（9.2×）**；C=8 **24.8k → 85.0k（3.4×）**。
闭环客户端量到的是 RTT 而不是服务器能力 —— 所以"单 worker 上限 38k"是假象，
真正的上限要开流水线才量得出来（这台机器上 p=64、`off` 模式约 **124k ops/s**；此时 Reactor 线程已 100%，见 §5）。

（历史：缺 `TCP_NODELAY` 时，同一口径 p=16 C=1 只有约 300 ops/s —— Nagle 与延迟 ACK 把
小回复攒住 40ms。加一行 `setsockopt(TCP_NODELAY)` 之后是 51 343。当时的围栏要求"流水线吞吐 ≥ 2× 闭环"，实测比值 8.9。）

### 3.3 混合与其他

| 口径 | 结果 |
|---|---|
| MIX 流水线 p=16 C=8（20% 写） | 874.2 QPS，P50 1.51ms，P99 505ms（读快、写把尾巴拖长） |
| 服务器 VmHWM（压测中） | 3.6MB |
| 客户端 CPU（p=16 C=8） | 3s 墙钟内 4.3s CPU ≈ 1.4 核（客户端不是瓶颈） |

### 3.4 其他已验证行为

> 下表是历史结论。驱动它们的 Python + `LD_PRELOAD` 注入器已从仓库移除（仍在 git 历史中），
> 当前仓库只能复跑 §4 的 C++ 测试。带 `kill -9` 的条目证明的是"进程崩溃可恢复"，不等于"掉电不丢"。

| 项目 | 结果 |
|---|---|
| 背压/暂停读 | 客户端被 TCP 流控挡住（连续 EAGAIN）、连接存活；客户端读走数据后恢复 |
| 回复顺序 | 第 i 个回复严格对应第 i 个请求；过载只回 1 个 `Error` 后关连接 |
| 数据文件锁 | 第二个实例拒绝打开、不动文件；`kill -9` 后锁自动释放、恢复流程正常 |
| 队列字节预算 | `RLIMIT_AS=192MiB` + 29 笔 4MiB 写：不再 `bad_alloc`；峰值 VmHWM 44MB、明确 `Error` |
| 收包缓冲预算 | 20×14MB 半条帧：进程存活、VmHWM 139.9MB（限制 192MB）、11/20 连接被关 |
| 半条帧超时 | 卡住的连接 1s 内被关；4MB 分块发 4s（每块间隔 100ms）的请求不被误杀 |
| epoll 注册失败 | 退出码 1 + 日志明确（不再"活着但不服务"） |
| 客户端帧长 | 假服务器声明 4GB/16MB+1：客户端 0.00s 退出、码 2（不再挂死/崩溃） |
| fd 耗尽 | `RLIMIT_NOFILE=32` 后观察窗口 CPU 0.00–0.14s（不空转） |
| 崩溃注入 | 27/27：`kill -9` 可恢复、残尾可截断、位翻转 fail closed |
| 故障注入 | 6/6：fsync 失败 → poisoned；写失败+回滚成功 → 普通失败；回滚失败 → poisoned |
| 端到端 | `kill -9` 后重启数据仍在；已删除的 key 仍为 `NotFound`；重启后的写入同样持久化 |
| Sanitizer | 历史：ASan/UBSan 通过；TSan 5/5 无竞争（Reactor ⇄ worker）。当前仓内没有 sanitizer 脚本 |

---

## 4. 测试与工具

全部测试是 C++，用 CTest 驱动：

```bash
cmake -S . -B build && cmake --build build -j
ctest --test-dir build --output-on-failure    # 29 个目标
./build/kvbench --help                        # 压测客户端（见 §3.1/3.2 的口径）
./build/kvserver /tmp/db.data                 # 单机模式
```

| 目标 | 覆盖 |
|---|---|
| `test_storage` / `test_recovery` | 记录格式、文件头、毒化语义、残尾截断与损坏拒绝 |
| `test_protocol` | 客户端协议编解码的黄金字节与非法输入 |
| `test_storage_worker` | 后台线程 FIFO、连接身份透传、eventfd 清空、析构 drain |
| `test_crc32_threads` | 8 线程并发首次建表（曾用 TSan 复现数据竞争） |
| `test_raft_state` / `test_logstore` | 任期投票与日志的落盘格式、失败语义 |
| `test_raft_core` / `_alloc` / `test_raft_election` | 初始化、发起选举、计票、分配失败 |
| `test_raft_append*` / `_proposal` / `_commit` / `_follower_commit` | 打包、接收、回复进度、提案与提交规则 |
| `test_raft_core_scenarios` | 3/5 节点可控队列的多轮换届与追平 |
| `test_raft_rpc_codec*` / `_validate` / `_config` | 帧与正文编解码、入站语义、配置解析 |
| `test_raft_transport` | 真实 loopback：握手、四类帧、半包/粘包、坏帧断连 |
| `test_raft_command` / `_state_machine` / `storage_rebuild` | 日志命令、只执行连续已提交命令、派生 KV 重建 |
| `test_raft_worker_init` / `_live` | 启动恢复与目录锁；实际线程 + 真实 TCP（含超时换号、迟到回复、check-quorum 退位） |
| `cluster_test` | fork/exec 三个真 `kvserver`：选主、SET/GET、SIGKILL leader 后仍可读 |

约定：**每个修复都配一条先红后绿的围栏**；构建失败不运行旧二进制。

---

---

## 5. 已知限制

1. **数据集受内存限制**：keydir 全量常驻内存（1M 小 key ≈ 100MB 量级，估算值，未实测）。
2. **存储层日志不回收**：Bitcask 追加日志只增不减，尚未实现 compaction（需要合并 + hint 文件重建索引）。
3. **写入已做批提交（group commit）**：见 §3.1 —— `always` 10 281 ops/s、`everysec` 98 210 ops/s；
   单条请求延迟仍是 4.6ms（一次 fsync 的本体）；收益随并发/流水线深度提高（批量越大越划算）。
4. **单 Reactor 是天花板（≈124k ops/s）**：p=64、`off` 模式下 SET **124 506** / GET **123 599** ops/s ——
   读写同速 ⇒ 瓶颈是两者共用的那条链路。实测线程 CPU：**Reactor 线程 ~100% 单核，worker 线程只有 55%**。
   再往上需要多 Reactor（`SO_REUSEPORT`，先解决键空间分片）或读旁路（GET 由 Reactor 直接读索引，
   代价是打破"存储只被 worker 线程访问"这个不变量，需要分片 / RCU 之类的设计）。
   另外：C=8 × p=256 = 2048 条在途会超过 `max_pending` 1024，触发过载保护回 `Error` ——
   深管道要配合上限调整才有意义（见 §2.5）。
5. **Raft 日志不回收**：`raft.log` 只追加，没有快照/日志压缩，重启时把已提交日志全量重放到派生 KV。
   解决思路（未实现）：定期把派生 KV 物化为快照并记录覆盖的 index/term，截断该点之前的日志（`LogStore` 需要引入 baseIndex 偏移），落后到快照点之前的节点通过 InstallSnapshot 直接接收快照。
6. **Raft 模式的读写语义**：所有 SET/GET/DEL 都作为日志命令提交后执行，GET 也经共识（未实现 ReadIndex/LeaseRead，读延迟与写相同）；只有 leader 服务。
   `Error` 只表示"没有确认成功"，**不保证命令未提交**：写可能在 leader 退位前已经复制到多数派。客户端没有请求去重，重试前需自行考虑。
7. **无认证/无 TLS**：仅适用于可信内网。
8. **`MAX_MESSAGE` 16MB**：单条消息上限，超过则视为协议错误并关闭连接。
9. **收包预算是"总量"而不是"公平分配"**：128MB 被半条帧吃满时，被关掉的是"正在长大的那条连接"，
   可能包含合法但很慢的客户端；半条帧超时（30s）负责把这种占用限制在时间上。
10. **半条帧超时粒度 1s**：靠 `epoll_wait` 的 1s 周期扫描实现，所以 800ms 的超时会在大约 1s 内生效。
11. **三种提交模式已实现**（`KV_SYNC_MODE=always|everysec|off`，默认 `always`）：
    `always` = 回复前这批必须落盘；`everysec` = 最多每秒落一次（契约"最多丢 1 秒"）；`off` = 只用于对照实验。
    非法取值拒绝启动。该设置只控制单机模式；Raft HardState/日志按自身路径同步落盘，派生 KV 用 `Off`（持久性由 Raft 日志与多数派保证）。
12. **测试范围**：仓库内只有 C++ 单测与一个三进程集群测试；网络背压/半帧超时/崩溃与故障注入那些场景原先由 Python + `LD_PRELOAD` 注入器驱动，已从仓库移除（仍在 git 历史中）。所以 §3.4 的部分结论没有可重复执行的围栏。
13. **封版前资源整改仍未完成**：GET 完成结果与 Buffer 保留容量尚未被请求/未消费输入预算完整覆盖；Raft transport 的发送预算、连接/帧超时等仍有待办。

---

## 6. 目录结构

```text
src/
  base/       buffer.{h,cpp}  be32.h  limits.h
  storage/    record.{h,cpp}  storage.{h,cpp}            Bitcask 引擎
  net/        reactor.{h,cpp} connection.{h,cpp} protocol.{h,cpp}
  app/        storage_worker.{h,cpp}                     单机后台线程
  raft/
    core/      core / state / logstore / logview / apply  协议状态与持久化
    protocol/  rpc / rpc_codec / rpc_validate / command   编解码与入站校验
    transport/ transport / config                         非阻塞网络
    node/      worker / state_machine                     Raft 节点与 KV 执行
  tools/      client.cpp                                 kvclient
  main.cpp                                               kvserver
tests/        CTest 目标（全 C++）
CMakeLists.txt
README.md
```

## 7. 后续计划

1. ~~**group commit**~~ ✅ 已完成（`storage-v1.2`）：一次 fsync 提交一批，写入 170 → 10 281 ops/s
   （`always`）/ 98 210 ops/s（`everysec`），单条延迟不变（见 §2.10 / §3.1）。
2. **Raft**：M1–M9 已通过各自验收并封 `raft-v1`（含 check-quorum 与客户端有界失败）。下一轮优先级：Raft 日志侧批量组提交 → 客户端请求去重 → 日志截断/快照（需 baseIndex 与 InstallSnapshot）→ 集群级磁盘故障注入。
3. 可靠性验证：网络分区/丢包/重排、客户端请求去重、线性一致性检查。
4. 快照与日志压缩；ReadIndex / Lease Read 优化只读。
5. 多 Reactor（提高读上限）与存储引擎接口化 + compaction，必要时对比 LSM 后端。
