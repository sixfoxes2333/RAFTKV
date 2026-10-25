# RaftCore：先看完整调用顺序，再看单个函数

范围：当前Core接口。外层现在由测试扮演，生产RaftWorker还没实现；图中的消息不是已经接通的TCP。

## 1. 谁负责什么

- 每个节点有自己的Core、StateStore、LogStore，同名成员不是共享内存。
- Core决定角色、投票、复制动作和提交位置，不负责发包或直接执行KV。
- StateStore保存本机term/vote；LogStore保存本机Raft日志。
- 测试/未来Worker完成保存、传递消息，并接住Core交出的结果。

**一轮Core调用产生的必要保存，必须完成后才能放行依赖它的消息或处理下一轮事件。必要保存或运行期日志查询失败时，外层停止实例，不猜磁盘是否已经改变，也不靠回滚内存继续。正常拒绝、非法提案参数等不能一概混成这种运行期故障。**

## 2. 一条命令，从节点1走到节点2

假定节点1已当选Leader，两边原来都提交到10：

```text
节点1：Leader                                  节点2：Follower

build_proposal(command) → 第11条
    ↓
LogStore.append → 保存成功
    ↓
on_log_persisted → 自己的match到11
    ↓
advance_commit → 只有自己一份，暂不新增提交
    ↓
build_append_entries(2)
    ── 原请求：带第11条，leader_commit仍10 ──────→ handle_append_entries
                                                    ↓ 判断匹配成功
                                                apply_append_entries
                                                先存必要term/vote，再存日志、同步尾部
                                                    ↓ 保存成功
                                                advance_follower_commit(原请求)
                                                原通知是10，所以此时仍提交到10
                                                    ↓
handle_append_entries_response(2,原sent,回复) ←── 保存成功回复
    ↓ 节点2的match到11
advance_commit → 当前任期第11条已有2/3份
    ↓ 本机commit到11，交出本机可执行区间11..11
build_append_entries(2)
    ── 下一份请求/空心跳：leader_commit=11 ──────→ 核对成功、完成必要保存及同步
                                                    ↓
                                                advance_follower_commit(这一份原请求)
                                                节点2自己的commit到11
                                                交出节点2本机区间11..11
```

新日志请求若已经带上新提交位置，Follower可在同轮保存后跟进，不一定额外等心跳。
`CommitOut`只交给本机外层，不跨机器传；跨机器的是请求里的`leader_commit`。
允许执行不等于KV已执行，更不等于现在已经向客户端回复OK；那部分在M8接入。

当前可读的正常演练：`tests/test_raft_follower_commit.cpp`的`test_real_commit_notice_roundtrip`。

## 3. 选举、计时器和恢复放在哪里

| 外层遇到的事 | 调用Core | 外层必须接着完成什么 |
|---|---|---|
| 选举超时 | `start_election` | 先保存`ElectionOutPut.hard_state`，再发投票请求或放行单节点领导行为 |
| 收到投票请求 | `handle_request_vote` | 保存输出里的HardState（如有），再发投票回复；拒绝也可能需要保存新term |
| 收到投票回复 | `handle_vote_response` | 保存新HardState（如有），再按角色继续；看到高term要退位 |
| 心跳/复制机会 | `build_append_entries` | 保存这一份原请求上下文，向目标peer发送；每peer在途纪律由外层管理 |
| 收到复制请求 | `handle_append_entries` | 按上图完成日志执行、Follower提交后，才释放成功回复 |
| 收到复制回复 | `handle_append_entries_response` | 回复对应原sent；仍为Leader时检查`advance_commit` |
| 需要重置选举超时 | 输出里的`reset_election_timer` | 由外层重置计时器，Core不sleep也不开线程 |
| Raft节点恢复 | 先`StateStore.load`、`LogStore.open`，再`attach_log`和`init` | 用实际恢复的term/vote和日志尾部重建Core；这不是KV/applied恢复 |

## 4. 几种结果不要混用

- 方法返回`true`：本次正常处理完；它可能是正常拒绝或没有新进展。
- `AppendEntriesOut.accept`：Core认为该请求可接受，但还没说明保存已完成。
- 执行函数返回`true`且`response.success=true`：本次Raft日志保存/同步成功，才能调用Follower提交入口。
- `CommitOut`非零区间：本机现在允许执行的新范围，不是执行完成。
- `good()`：Core已初始化且未停止，不证明一轮保存动作已完成。

## 5. M5当前检查表

- [x] 复用M4正常链路，写清节点、角色、调用者和保存顺序。
- [x] 查询/保存失败边界：R4先红，由用户修正查询返回值处理后转绿；日志未改、测试外层停止，不释放依赖结果。
- [x] 统一队列覆盖丢失、延迟、重复、乱序、隔离重选与64+64+2追平。
- [x] 真实Raft存储重开、新Core恢复投票/日志，再参与选举与提交。
- [x] 本轮普通36项、配置ASan/UBSan复测通过；中途背压启动探测失败保留，不宣称首次全绿。

完整范围见[验收记录](raft-core-acceptance.md)。R4的Core错误传播已修，但真实Worker门禁仍待M7；RPC内容校验、极端索引等其他已知边界未因此关闭。M5的节点重启是同进程对象销毁/重建，不是kill -9集群；不含TCP、生产Worker或KV apply。
