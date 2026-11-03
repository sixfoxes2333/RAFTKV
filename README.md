# KV — 单节点持久化 KV 存储（C++17 / epoll）

一个从零手写的持久化键值存储服务：epoll 事件循环 + 后台存储线程 + 二进制协议 + Bitcask 风格日志存储。
支持 `SET` / `GET` / `DEL`；默认 `always` 模式按批 `fsync` 后确认写入，另有 `everysec/off` 模式。

> 当前状态：单节点 KV 已封版（`storage-v1.2`）；Raft 的持久化、Core 与非阻塞 transport 已接入生产 RaftWorker，**M7 运行层验收通过**。
> 11/03 完整普通回归 **47/47**，配置的 ASan/UBSan runner 通过；CRC 冷启动并发与 Worker 9 组 TSan 通过，原 StorageWorker ×5/单机 E2E TSan 也通过。覆盖范围见 §4。
> 三个实际 `kvserver` 已验证选主、SIGKILL leader 后重选、同目录重启加入及 130 条旧日志追平。RPC ID、同连接超时换编号、旧回复过滤与高任期退位已通过实际 Worker/TCP 测试。
> **下一步 M8：已提交 KV 执行与一致恢复。当前 Raft 模式的 KV 请求统一返回 Error，applied=0；单机模式继续支持 SET/GET/DEL。** M9 故障封版与其他已知整改仍未完成，尚无 `raft-v1`。

```bash
./build/kvserver /tmp/db.data &          # 启动（默认 ./kvdb.data）
./build/kvclient set name sixfoxes       # → OK      （退出码 0）
./build/kvclient get name                # → sixfoxes（退出码 0）
./build/kvclient get nokey               # → NOT_FOUND（退出码 1）
kill -9 %1 && ./build/kvserver /tmp/db.data &   # 重启后数据仍在
```

Raft 运行层的独立验收（脚本自行创建节点目录和互不重复的端口）：

```bash
python3 tests/raft_server_startup_test.py
python3 tests/raft_server_cluster_test.py
```

手动运行需配置 `KV_PORT`、`KV_RAFT_ID`、`KV_RAFT_PEERS`、`KV_RAFT_DATA`。Raft 数据目录须已存在，PEERS 包含自己，Raft 监听端口从自身条目取得；不使用独立的 KV_RAFT_PORT。单机启动时不设置三个 KV_RAFT_* 变量。
---

## 1. 架构

下图为单机 KV 路径。Raft 模式由独立 RaftWorker 拥有 Core/存储/transport，客户端 Reactor 通过加锁快照观察状态；KV 提案/apply 在 M8 接入。

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

（这条 bug 被 `net_halfclose_flush_test.py` 逮到，而它的注释在四天前就预言了这个场景。）

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
小回复攒住 40ms。加一行 `setsockopt(TCP_NODELAY)` 之后是 51 343。围栏 `net_nodelay_test.py`
要求"流水线吞吐 ≥ 2× 闭环"，实测比值 8.9。）

### 3.3 混合与其他

| 口径 | 结果 |
|---|---|
| MIX 流水线 p=16 C=8（20% 写） | 874.2 QPS，P50 1.51ms，P99 505ms（读快、写把尾巴拖长） |
| 服务器 VmHWM（压测中） | 3.6MB |
| 客户端 CPU（p=16 C=8） | 3s 墙钟内 4.3s CPU ≈ 1.4 核（客户端不是瓶颈） |

### 3.4 其他已验证行为

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
| Sanitizer | ASan/UBSan **PASS**；TSan **5/5 无数据竞争**（Reactor 线程 ⇄ worker 线程） |

---

## 4. 测试与工具

```bash
bash tests/run_all.sh            # 47 项普通回归
bash tests/run_all.sh --slow     # 追加：崩溃注入 + 故障注入 + 慢 fsync 注入
bash tests/run_all.sh --asan     # 追加：ASan/UBSan（服务器 + 端到端场景）
bash tests/run_asan.sh           # 21 个测试目标 + Worker/kvserver 进程场景 + 单机网络场景
bash tests/run_tsan.sh           # TSan：worker 单测 x5 + 服务器端到端
./build/kvbench --help           # 压测客户端（见 3.1/3.2 的口径）
```

10/22 验证：第二轮完整普通回归 32 项通过，ASan/UBSan runner 通过（含 M3 回复测试）。首轮批提交吞吐比例未达阈值，原阈值独立复跑及第二轮整套通过，原因尚未定位；性能阈值不是环境无关保证。ASan runner 不运行全部普通用例，且关闭 LeakSanitizer；本轮未重跑 TSan。

10/25 M4验收：完整普通35项一次通过，配置的ASan/UBSan runner通过（含M4、背压、单节点E2E）。提案7组、leader提交12组、follower提交9组；leader用例新增五Core多轮换届的Figure8类轨迹，保存用内存模型；follower用例使用真实StateStore/LogStore，包含三个Core对象两轮提交通知。均非真实网络集群。保存失败覆盖目录失效和保存后term查询失败，未声称复合pwrite/fsync故障已穷尽。分配失败目标只跑普通测试，LeakSanitizer关闭，本轮未跑TSan。

10/25 M5验收：最新完整普通36项通过；新Core队列场景12组、follower/R4共10组均普通与ASan/UBSan通过。覆盖隔离换届、少数派不提交、64+64+2追平、旧回复配对、真实文件重开并重建Core等，不含TCP/生产Worker/KV apply。配置ASan runner中途有一次背压启动探测失败（各单元目标均通过，保留日志，后来服务器日志出现bind失败），原配置复测通过；占用/启动时序根因未确诊，未改阈值。

10/26 M6第1步：新增 `tests/test_raft_rpc_codec.cpp`（8组）与 `src/raft/rpc*.{h,cpp}`——4B长度头+1B类型的帧、17MiB正文上限、半帧/粘包/未知类型均拒绝。普通37/37、配置ASan/UBSan通过；纯函数，无socket，不代表任何节点间通信已接通。

10/26 M6第2步：四类RPC正文（投票请求32B、回复9B、复制请求44B定长头+变长条目、复制回复9B）。条目index不上线，由Core重算；布尔字段只认0/1；条目数两道闸（MAX_RPC_ENTRIES=4096 + 剩余字节/12），避免一条17MiB报文放大出上百万对象。测试9组，最大到4096条与每个截断点。普通38/38、配置ASan/UBSan通过；仍未接入socket。

10/28 M6第3步：`rpc_validate` 拦下语义自相矛盾的入站请求（term/ID 非 0、空日志 index0⇔term0、`prev+条数` 不溢出、`entry.term∈[1,request.term]` 且批次内非递减）。新增测试用真实存储证明：绕过校验时 term-4 条目确实被写入并回 success，随后用存储值重建 Core 时 `init` 拒绝启动；走校验则日志保持为空、Core 仍可用。修正一处把合法空日志投票判为非法（会让全新集群永远选不出 leader）的缩进错误。普通39/39、配置ASan/UBSan通过；仍未接入socket。

10/28 M6第4步：`raft::parse_raft_config` 解析 `KV_RAFT_ID`/`KV_RAFT_PEERS`/`KV_RAFT_DATA`，PEERS 包含自己，监听端口取自身条目；ID/端口/成员关系/重复项/空目录等失败不改出参。host 仅检查基本语法，transport 当前使用 IPv4。环境加载器不猜缺省值。历史普通40/40、配置ASan/UBSan通过。

10/28 测试夹具隔离：`net_backpressure`/`net_emfile`/`net_halfclose_flush` 改为各自指定 `/tmp` 数据文件，避免读取工作目录已有的大库。随后普通与配置 ASan 门禁通过。此前把 timeout 下的服务器总运行时长当作启动耗时、据此认定 10/25 失败根因，证据不足，撤回该根因结论；这些固定测试路径仍不支持并发复用。

10/28 M6第5步：`RaftTransport` 非阻塞收发（真实 loopback）。新增 Hello 帧类型在连接建立时互报节点 ID；只有 ID 小的一方发起连接，避免双方同时连上互判重复；收包按 4 字节长度切帧，请求类在 transport 内解码+语义校验后才回调；`send` 对未连接 peer 直接返回失败，不排队。7 组测试：握手、四类帧字节一致、半帧不提前派发、一次写两帧、坏帧（4GiB 长度/首帧非 Hello/谎报自己 ID/非法条目任期）全部断连、peer_down 通知、4MiB 卡住的 peer 不影响另一条。普通41/41、配置ASan/UBSan通过。仍未接 Worker，不能选主。

11/03 M7：RPC 业务帧增加 8B 编号（Hello 不变），Worker 持有原复制请求和截止时间；持久化成功后释放依赖消息。后台启动结果通过条件变量交给 main，运行快照加锁复制，signalfd/timerfd 接入客户端 Reactor，后台错误非零退出。真实进程验收覆盖就绪/PID/快照新鲜度、两种信号干净退出、预留 fd 失败退出、14 类坏配置先于存储拒绝、三节点选举/重选/重启/追平；Raft KV 入口保持 Error。CRC 首次并发建表的 TSan 数据竞争已实际红→绿。

本轮 ASan/UBSan：21 个测试目标、Worker 三进程、kvserver 入口/三进程和原背压/单机 E2E；关闭 LSan，极小 RLIMIT_NOFILE 的预留 fd 用例仅普通运行。TSan：CRC 8 线程首次调用、Worker 9 组、原 StorageWorker ×5 与单机 kvserver E2E；不是全部普通测试均运行 TSan。

| 文件 | 作用 |
|---|---|
| `tests/test_storage.cpp` / `test_recovery.cpp` | 存储单元与恢复（含格式 v2 黄金值 / 残尾截断 / 损坏拒绝） |
| `tests/test_protocol.cpp` | 协议编解码（黄金字节串 + 往返 + 非法输入） |
| `tests/test_storage_worker.cpp` | 后台线程：FIFO、身份透传、eventfd 清空、析构 drain、跨重启 |
| `tests/kvproto.py` | 协议的 Python 实现（测试共用）；`start_server` 会**验证自己启动的进程**并做协议握手 |
| `tests/kvbench.cpp` | C++ 压测客户端（逐请求真实延迟、合并样本算分位数、报告客户端 CPU） |
| `tests/kv_protocol_test.py` | 端到端：语义 / 流水线 / 1MB / 协议错误 / 并发 / `kill -9` 重启 |
| `tests/kv_client_test.py` `kvclient_fake_server_test.py` `kvclient_limits_test.py` | CLI 契约：退出码、二进制 value、巨大帧长、字段超限 |
| `tests/net_nodelay_test.py` | 流水线围栏：depth=16 必须显著快于 depth=1 |
| `tests/net_pause_read_test.py` | 背压：客户端必须被流控挡住、连接不许被上限杀掉 |
| `tests/net_outqueue_test.py` | 发送队列：硬上限 + 水位恢复 + 内存有界 |
| `tests/db_lock_test.py` | 数据文件互斥：第二个实例不许动正在用的库 |
| `tests/reply_order_test.py` | 回复顺序 + 过载语义 |
| `tests/queue_bytes_test.py` `in_buffer_budget_test.py` `net_frame_timeout_test.py` | 三条内存/时间上限（后台队列字节、收包全局字节、半条帧超时） |
| `tests/group_commit_test.py` | 批提交三模式：批量提速 / 单条延迟不变 / 批 fsync 失败整批不可信 / everysec 节流与 1 秒契约 / off |
| `tests/net_epoll_add_fail_test.py` `startup_probe_test.py` | 注册失败要响亮退出；脚手架自身的启动探测 |
| `tests/net_*_test.py`（半关闭 / EMFILE / 启动失败） | 网络层边界 |
| `tests/crash_inject_test.py` `crash_writer.cpp` | 崩溃注入（`kill -9` / 截断 / 位翻转） |
| `tests/test_raft_state.cpp` `raft_state_fault_test.py` | Raft term/vote 持久化：大端格式 / 损坏必须拒绝 / 写失败语义 / 系统调用顺序 |
| `tests/test_logstore.cpp` `logstore_fault_test.py` | Raft 日志存储：追加/读取/截断/恢复、残尾修复与完整损坏拒绝、pwrite/fsync/ftruncate 失败、批 fsync 计数与调用顺序 |
| `tests/test_raft_core.cpp` `test_raft_core_alloc.cpp` | Core 初始化与发起选举：任期边界、自投票、目标列表、角色与一次性分配失败 |
| `tests/test_raft_election.cpp` | 内存三节点选举演练：选主 / 分票 / 更高任期退位 / 重复票去重 / 少数派无法当选 |
| `tests/test_raft_append.cpp` | M1/M2 内存演练：打包上限、前一条匹配、冲突和追加动作 |
| `tests/test_raft_append_persist.cpp` | M2 真实 LogStore/StateStore 基本执行：存任期、截断/追加、失败停止、日志重开；不是 KV apply |
| `tests/test_raft_append_response.cpp` | M3 九组：原批次确认范围、回退/迟到/重复回复、高任期输出、成员隔离及多批追平；使用内存日志和保存模型 |
| `tests/test_raft_proposal.cpp` | M4第1步：构造不预占编号、命令/索引边界、真实保存/同步、复用M1–M3的双Core真实存储闭环；没有提交推进或KV apply |
| `tests/test_raft_commit.cpp` | leader提交12组：多数派/当前任期、连续区间、退位重选、读取失败、红→绿及五Core的Figure8类覆盖轨迹；内存保存模型 |
| `tests/test_raft_follower_commit.cpp` | follower提交原9组+M5 R4读失败围栏：原请求范围、真实存储失败门禁、3个Core提交通知；无socket/生产Worker/KV apply |
| `tests/test_raft_core_scenarios.cpp` | M5共12组：3/5节点可控消息队列、重复/丢失/乱序、隔离重选/少数派、分批追平、真实存储重开和新Core；含跨事件不变量检查 |
| `tests/test_raft_rpc_codec.cpp` | M6第1步：帧长度头/类型/17MiB上限/半帧与粘包/未知类型/失败不改写出参；纯函数，无socket |
| `tests/test_raft_rpc_codec_body.cpp` | M6第2步：四类正文往返、固定长度严格、布尔只认0/1、4096/4097条目数两道闸、每个截断点、cmd_len谎报、正文经帧往返 |
| `tests/test_raft_rpc_validate.cpp` | M6第3步：投票/复制请求的语义规则表 + 真实存储证明（绕过校验会写入非法任期条目、重启被拒；走校验则日志不动） |
| `tests/test_raft_config.cpp` | M6第4步：ID/端口/PEERS 语法/成员关系/环境变量缺失；失败不改出参与错误取名 |
| `tests/test_raft_transport.cpp` | M6第5步：真实 loopback 的握手、四类帧、半包/粘包、坏帧断连、peer_down、慢 peer 隔离 |
| `tests/test_crc32_threads.cpp` | 8 线程并发首次调用，无预热；独立 CRC 黄金值与 TSan 红→绿 |
| `tests/test_raft_worker_init.cpp` / `test_raft_worker_live.cpp` | 启动恢复/目录锁 9 组，实际线程/TCP 9 组；含超时编号替换、迟到回复、高任期保存及失败停止 |
| `tests/raft_worker_node.cpp` / `raft_worker_process_test.py` | 实际 Worker 子进程与共享 PID/新鲜状态/磁盘观察器 |
| `tests/raft_server_startup_test.py` / `raft_server_cluster_test.py` | 真实 kvserver 启动/信号/坏配置/失败传播与三进程选主、重选、重启和追平 |
| `tests/raft_cluster.py` | 旧 --smoke/--leader/--kill-leader CLI，复用新观察器；每次独立日志，不复用历史 leader 行 |
| `tests/fail_inject.{c,py}` | LD_PRELOAD 故障注入（写/回滚/fsync/epoll_ctl/pwrite/pread 失败、短写、慢 fsync） |

两个测试工程上的约定：

- **每个修复都配一条"红过的"围栏**（先确认它在修复前失败，再修到绿）；
- **测试脚手架本身也要围栏**：`startup_probe_test.py` 专门验证"假二进制 / 端口被占 / 协议不符"
  不会被误判成启动成功 —— 否则网络测试可能全跑在"不是我们的服务器"上。

---

## 5. 已知限制

1. **数据集受内存限制**：keydir 全量常驻内存（1M 小 key ≈ 100MB 量级，估算值，未实测）。
2. **日志不回收**：追加日志只增不减，尚未实现 compaction（需要合并 + hint 文件重建索引）。
3. **写入已做批提交（group commit）**：见 §3.1 —— `always` 10 281 ops/s、`everysec` 98 210 ops/s；
   单条请求延迟仍是 4.6ms（一次 fsync 的本体）；收益随并发/流水线深度提高（批量越大越划算）。
4. **单 Reactor 是天花板（≈124k ops/s）**：p=64、`off` 模式下 SET **124 506** / GET **123 599** ops/s ——
   读写同速 ⇒ 瓶颈是两者共用的那条链路。实测线程 CPU：**Reactor 线程 ~100% 单核，worker 线程只有 55%**。
   再往上需要多 Reactor（`SO_REUSEPORT`，先解决键空间分片）或读旁路（GET 由 Reactor 直接读索引，
   代价是打破"存储只被 worker 线程访问"这个不变量，需要分片 / RCU 之类的设计）。
   另外：C=8 × p=256 = 2048 条在途会超过 `max_pending` 1024，触发过载保护回 `Error` ——
   深管道要配合上限调整才有意义（见 §2.5）。
5. **KV 读写仍为单机服务**：Raft 选举/复制运行层已接通，提交后 KV 执行与一致恢复待 M8；Raft 模式暂时明确拒绝 KV 请求。
6. **无认证/无 TLS**：仅适用于可信内网。
7. **`MAX_MESSAGE` 16MB**：单条消息上限，超过则视为协议错误并关闭连接。
8. **收包预算是"总量"而不是"公平分配"**：128MB 被半条帧吃满时，被关掉的是"正在长大的那条连接"，
   可能包含合法但很慢的客户端；半条帧超时（30s）负责把这种占用限制在时间上。
9. **半条帧超时粒度 1s**：靠 `epoll_wait` 的 1s 周期扫描实现，所以 800ms 的超时会在大约 1s 内生效。
10. **三种提交模式已实现**（`KV_SYNC_MODE=always|everysec|off`，默认 `always`）：
    `always` = 回复前这批必须落盘；`everysec` = 最多每秒落一次（契约"最多丢 1 秒"）；`off` = 只用于对照实验。
    非法取值拒绝启动。该设置只控制单机模式；Raft HardState/日志的同步不受它影响，KV apply 策略在 M8 确定。
11. **封版前资源整改仍未完成**：GET 完成结果与 Buffer 保留容量尚未被请求/未消费输入预算完整覆盖；Raft transport 的发送预算、连接/帧超时等仍有待办。

---

## 6. 目录结构

```text
src/
  base/      buffer.{h,cpp}  be32.h  limits.h
  storage/   record.{h,cpp}  storage.{h,cpp}
  net/       reactor.{h,cpp} connection.{h,cpp} protocol.{h,cpp}
  app/       storage_worker.{h,cpp}
  tools/     client.cpp
  raft/      state / logstore / core / rpc / codec / validate / config / transport / worker
  main.cpp
tests/       （见上表）
CMakeLists.txt
```

## 7. 后续计划

1. ~~**group commit**~~ ✅ 已完成（`storage-v1.2`）：一次 fsync 提交一批，写入 170 → 10 281 ops/s
   （`always`）/ 98 210 ops/s（`everysec`），单条延迟不变（见 §2.10 / §3.1）。
2. **Raft**：M1–M7 主线已通过相应验收；下一步 M8 KV apply/一致恢复，再做 M9 真实读写故障验收及封版整改。
3. 可靠性验证：网络分区/丢包/重排、客户端请求去重、线性一致性检查。
4. 快照与日志压缩；ReadIndex / Lease Read 优化只读。
5. 多 Reactor（提高读上限）与存储引擎接口化 + compaction，必要时对比 LSM 后端。
