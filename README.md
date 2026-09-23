# KV — 单节点持久化 KV 存储（C++17 / epoll）

一个从零手写的持久化键值存储服务：epoll 事件循环 + 后台存储线程 + 二进制协议 + Bitcask 风格日志存储。
支持 `SET` / `GET` / `DEL`，每条写都 `fsync`，进程被 `kill -9` 后重启数据仍在。

> 当前状态：**单节点持久化 KV 已完成**（tag `storage-v1`）。
> 下一步计划是在它之上实现 Raft，做成 3 副本的 RaftKV。

```bash
./build/kvserver /tmp/db.data &          # 启动（默认 ./kvdb.data）
./build/kvclient set name sixfoxes       # → OK      （退出码 0）
./build/kvclient get name                # → sixfoxes（退出码 0）
./build/kvclient get nokey               # → NOT_FOUND（退出码 1）
kill -9 %1 && ./build/kvserver /tmp/db.data &   # 重启后数据仍在
```

---

## 1. 架构

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
要排在慢 `SET` 后面，实测 **658ms**。压测同样能看到：`SET` 吞吐在 1→64 连接之间恒为 ~215 ops/s。

### 2.5 发送队列有上限（三层防护）

对端不读时 `out_` 曾经无界增长（可被单连接打到 OOM）。现在：

1. **高水位 2MB**：暂停读（`EPOLLIN` 摘掉）→ 压力转成 TCP 背压；
2. **低水位 512KB**：恢复读（迟滞，避免在阈值附近反复开关）；
3. **硬上限 17MB**：连"已接收请求"产生的回复都堆爆时，放弃这条连接（必须 > 单条消息最大值 16MB）。

实测：40 条 2MB 回复且客户端不读 → 服务器在 17MB 处 `send queue overflow`、连接关闭、
进程存活、峰值 `VmHWM` **32MB**（若无限排队则是 80MB+）。

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

---

## 3. 实测数据

环境：WSL2 / Linux，64 字节 value，客户端为多进程（避免 Python GIL 污染测量，
第一版用线程时 GET 在 C=16 从 20k 掉到 4k，实际是客户端瓶颈）。
复现：`python3 tests/bench.py 3`

**SET（每条都 fsync，单 worker 串行）**

| 连接数 | QPS | 平均 | P50 | P99 |
|---:|---:|---:|---:|---:|
| 1 | 215.1 | 4.64ms | 4.60ms | 5.63ms |
| 4 | 221.1 | 18.03ms | 18.27ms | 19.38ms |
| 16 | 221.7 | 71.29ms | 72.10ms | 77.42ms |
| 64 | 218.4 | 279.61ms | 291.01ms | 307.22ms |

→ 吞吐恒为 ~215 ops/s（= 1 / fsync 延迟 4.65ms），延迟随并发线性增长 = 纯排队。
**这就是下一步做 group commit 的直接理由。**

**GET（不落盘；走同一条串行路径读页缓存）**

| 连接数 | QPS | 平均 | P50 | P99 |
|---:|---:|---:|---:|---:|
| 1 | 6845.4 | 0.14ms | 0.14ms | 0.23ms |
| 4 | 24241.5 | 0.16ms | 0.16ms | 0.27ms |
| 16 | 34944.4 | 0.46ms | 0.44ms | 0.66ms |
| 64 | 37964.5 | 1.67ms | 1.62ms | 2.11ms |

→ 单 worker 的上限约 **38k ops/s**（≈26µs/请求的线程往返）。再往上需要多 worker
或让读旁路 worker（`pread` 无副作用，多线程读同一 fd 是安全的）。

**混合 80/20（C=16）**：1082.8 ops/s，P50 13.89ms。
→ 20% 写决定总吞吐：215 / 0.2 ≈ 1075，实测吻合。

**其他已验证行为**

| 项目 | 结果 |
|---|---|
| 背压 | 客户端 `SO_RCVBUF=4096` 时 8MB 回复完整送达（3.05s，中途无 EOF） |
| 队列水位 | 17MB 硬上限生效，峰值 VmHWM 32.3MB |
| fd 耗尽 | `RLIMIT_NOFILE=32` 后观察窗口 CPU 0.00–0.14s（不空转） |
| 启动失败 | 端口被占 → 退出码 1（且数据库未就绪前不 listen） |
| 崩溃注入 | 27/27：`kill -9` 可恢复、残尾可截断、位翻转 fail closed |
| 故障注入 | 6/6：fsync 失败 → poisoned；写失败+回滚成功 → 普通失败；回滚失败 → poisoned |
| 端到端 | `kill -9` 后重启数据仍在；已删除的 key 仍为 `NotFound`；重启后的写入同样持久化 |
| 并发 | 8 连接 × 各 100 轮 SET+GET 无错（1600 次操作 3.63s，含 fsync） |

---

## 4. 测试与工具

```bash
bash tests/run_all.sh            # 11 项快速回归（约 40s）
bash tests/run_all.sh --slow     # 追加：崩溃注入 + 故障注入 + 慢 fsync 注入
bash tests/run_all.sh --asan     # 追加：ASan/UBSan（服务器 + 端到端场景）
bash tests/run_tsan.sh           # TSan：worker 单测 x5 + 服务器端到端（Reactor ⇄ worker）
python3 tests/bench.py 3         # 压测
```

| 文件 | 作用 |
|---|---|
| `tests/test_storage.cpp` / `test_recovery.cpp` | 存储单元与恢复（含格式 v2 的黄金值） |
| `tests/test_protocol.cpp` | 协议编解码（黄金字节串 + 往返 + 非法输入） |
| `tests/test_storage_worker.cpp` | 后台线程：FIFO、身份透传、eventfd 清空、析构 drain、跨重启 |
| `tests/kvproto.py` | 协议的 Python 实现（测试共用） |
| `tests/kv_protocol_test.py` | 端到端：语义 / 流水线 / 1MB / 协议错误 / 并发 / kill -9 重启 |
| `tests/kv_client_test.py` | kvclient CLI 契约（退出码、二进制 value、与 Python 客户端互通） |
| `tests/net_*_test.py` | 网络层：背压 / 半关闭 / 半关闭+大回复 / EMFILE / 启动失败 / 队列水位 |
| `tests/crash_inject_test.py` `crash_writer.cpp` | 崩溃注入（kill -9 / 截断 / 位翻转） |
| `tests/fail_inject.{c,py}` | LD_PRELOAD 故障注入（写/回滚/fsync 失败、慢 fsync） |
| `tests/bench.py` | 压测（SET / GET / 混合，并发扫描，QPS + P50/P99） |

---

## 5. 已知限制

1. **数据集受内存限制**：keydir 全量常驻内存（1M 小 key ≈ 100MB 量级，估算值，未实测）。
2. **日志不回收**：追加日志只增不减，尚未实现 compaction（需要合并 + hint 文件重建索引）。
3. **写吞吐被串行 fsync 卡住**：~215 ops/s（本机 fsync 4.65ms）；改进方向是 group commit。
4. **读也被单 worker 串行**：GET 上限 ~38k ops/s；改进方向是多 worker 或读旁路。
5. **单点**：没有副本、没有故障转移（Raft 是下一步）。
6. **无认证/无 TLS**：仅适用于可信内网。
7. **`MAX_MESSAGE` 16MB**：单条消息上限，超过则视为协议错误并关闭连接。

---

## 6. 目录结构

```text
src/
  base/      buffer.{h,cpp}  be32.h  limits.h
  storage/   record.{h,cpp}  storage.{h,cpp}
  net/       reactor.{h,cpp} connection.{h,cpp} protocol.{h,cpp}
  app/       storage_worker.{h,cpp}
  tools/     client.cpp
  main.cpp
tests/       （见上表）
CMakeLists.txt
```

## 7. 后续计划

1. **Raft**：leader 选举、日志复制、持久化 `currentTerm/votedFor/log`、apply 到本存储。
2. 可靠性验证：网络分区/丢包/重排、客户端请求去重、线性一致性检查。
3. 快照与日志压缩；ReadIndex / Lease Read 优化只读。
4. group commit（合并 fsync）与多 worker（提高写/读上限）。
5. 存储引擎接口化 + compaction，必要时对比 LSM 后端。
