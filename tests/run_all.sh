#!/usr/bin/env bash
# run_all.sh —— 一键跑当前所有测试
#
# 【用法】
#   tests/run_all.sh           跑：存储单测 + 恢复测试 + 6 项网络/类型契约
#   tests/run_all.sh --asan    额外跑 ASan/UBSan 构建（较慢）
#   tests/run_all.sh --slow    额外跑崩溃注入（kill -9 / 残尾 / 位翻转，约 25s）
#
# 【当前预期（2026-09-16，M2e 完成）】
#   test_storage              PASS
#   test_recovery             PASS（keydir 重建 / torn-tail 截断 / 头校验和拒绝损坏 / 拒绝 v1 文件）
#   net_halfclose_test        PASS
#   net_halfclose_flush_test  PASS
#   net_backpressure_test     PASS
#   net_emfile_test           PASS
#   net_startup_fail_test     PASS
#   test_noncopyable          PASS
#   整体退出码 0
#
# 【历史】
#   网络层 Step 0-9 已封版并标记 net-v1
#   存储 M1/M2a/M2b：记录格式、文件头、put
#   存储 M2c：append_record + get + CRC 拒绝脏数据
#   存储 M2d：DEL 墓碑 + keydir 删除
#   存储 M2e：recover + torn-tail 截断 + 记录格式 v2（头校验和，区分损坏与残尾）
set -u

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

#每轮开始清一次测试日志：
#  start_server 现在是【追加】写日志（同一次运行里多个实例的输出都保留），
#  所以跨轮次必须清掉旧的，否则日志断言会匹配到上一轮的残留。
rm -f /tmp/kv_*.log

echo "== 构建 =="
cmake -B build > /dev/null || exit 2
cmake --build build -j > /dev/null 2>&1 || { echo "构建失败"; exit 2; }
echo "构建完成"

fail=0

STEP=0   #序号自增：以后插入测试不用再改后面的编号

echo
echo "== $((++STEP)) 存储单元测试（记录/文件头/put/get/del）=="
if ./build/test_storage > /tmp/kv_test_storage.log 2>&1; then
    echo "PASS（日志 /tmp/kv_test_storage.log）"
else
    echo "FAIL："
    grep -n "FAIL" /tmp/kv_test_storage.log | head -10
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) 存储恢复（keydir 重建 / torn-tail 截断 / CRC 损坏拒绝）=="
if ./build/test_recovery > /tmp/kv_test_recovery.log 2>&1; then
    echo "PASS（日志 /tmp/kv_test_recovery.log）"
else
    echo "FAIL："
    grep -n "FAIL" /tmp/kv_test_recovery.log | head -20
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) CRC 首次调用并发（8线程同时建表、独立黄金值）=="
if timeout 30 ./build/test_crc32_threads; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) 半关闭回归（小消息）=="
if python3 tests/net_halfclose_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) 半关闭 + 大回复围栏（先发完再关 + 不空转）=="
if python3 tests/net_halfclose_flush_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) 背压回归 =="
if python3 tests/net_backpressure_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) 发送队列水位线（硬上限 / 暂停读后恢复 / 内存有界）=="
if python3 tests/net_outqueue_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) fd 用尽（EMFILE）不许空转 =="
if python3 tests/net_emfile_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) 启动失败必须响亮地退出（端口被占）=="
if python3 tests/net_startup_fail_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) 单节点 KV 端到端（协议 / 流水线 / 大 value / 并发 / kill -9 重启）=="
if python3 tests/kv_protocol_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) kvclient 命令行（退出码 / 二进制 value / 与 Python 客户端互通）=="
if python3 tests/kv_client_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) 流水线围栏（服务端必须关 Nagle：depth=16 要显著快于 depth=1）=="
if python3 tests/net_nodelay_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) 数据文件锁（第二个实例不许动正在用的库）=="
if python3 tests/db_lock_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) 回复顺序（第 i 个回复必须对应第 i 个请求；过载要显式拒绝）=="
if python3 tests/reply_order_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo
echo "== $((++STEP)) 队列字节预算（大 value 不许把内存打爆；过载必须显式拒绝）=="
if python3 tests/queue_bytes_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) 暂停读（背压要真的挡住客户端；连接不许被上限杀掉）=="
if python3 tests/net_pause_read_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) epoll 注册失败（必须响亮退出，不许活着但不服务）=="
if python3 tests/net_epoll_add_fail_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) 客户端帧长校验（假服务器声明巨大长度：不许崩、不许挂）=="
if python3 tests/kvclient_fake_server_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) 客户端字段上限（超长 key 本地就该拒绝）=="
if python3 tests/kvclient_limits_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) 收包缓冲全局预算（半条帧不许把内存打爆）=="
if python3 tests/in_buffer_budget_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) 半条帧超时（卡住不回进展的连接要断开；慢但有序的不许误杀）=="
if python3 tests/net_frame_timeout_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) 启动探测（脚手架自己：假二进制/端口被占/协议不符都要报错）=="
if python3 tests/startup_probe_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) 批提交（always 提速 / 批 fsync 失败整批不可信 / everysec 节流 / off）=="
if python3 tests/group_commit_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo "== $((++STEP)) Raft term/vote 持久化（大端格式 / 损坏必须拒绝 / 改名替换后只留正式文件）=="
if ./build/test_raft_state; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) Raft term/vote 写路径失败（write/fsync 失败必须报错，错误信息不许丢）=="
if python3 tests/raft_state_fault_test.py; then
    :
else
    fail=$((fail + 1))
fi


echo
echo "== $((++STEP)) Raft 日志存储（追加/读取/截断/恢复 + 损坏必须拒绝 + 残尾修复）=="
if ./build/test_logstore; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) Raft 日志存储写路径失败（pwrite/fsync/ftruncate 失败必须报错）=="
if python3 tests/logstore_fault_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) Raft Core 初始化/发起选举（含分配失败）=="
if ./build/test_raft_core && ./build/test_raft_core_alloc; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) Raft 选举演练（3 节点内存投递：选主/分票/退位/重复票）=="
if ./build/test_raft_election; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) Raft 日志复制打包/接收（前一条核对、冲突截断、分批上限）=="
if ./build/test_raft_append; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) Raft 复制落地（真实 LogStore/StateStore：写日志、存任期、失败停机）=="
if ./build/test_raft_append_persist; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) Raft 复制回复（成功推进、失败回退、迟到回复、冲突追平）=="
if ./build/test_raft_append_response; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) Raft 本地提案（组装不预占、大小边界、保存同步及复制闭环）=="
if ./build/test_raft_proposal; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) Raft leader提交（多数派、当前任期、连续区间、读取失败）=="
if ./build/test_raft_commit; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) Raft follower提交（匹配范围上限、保存失败、完整提交通知）=="
if ./build/test_raft_follower_commit; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) Raft Core综合场景（消息队列、隔离换届、旧回复、真实存储重启）=="
if ./build/test_raft_core_scenarios; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) Raft RPC 帧编解码（长度头 / 类型 / 17MiB 上限 / 半帧与粘包）=="
if ./build/test_raft_rpc_codec; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) Raft RPC 正文编解码（四类消息 / 条目数上限 / 截断点 / 布尔只认0和1）=="
if ./build/test_raft_rpc_codec_body; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) Raft 入站校验（term/ID/日志尾一致/位置溢出/条目任期范围与单调）=="
if ./build/test_raft_rpc_validate; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) Raft 配置解析（ID/端口/PEERS 语法/成员关系/环境变量缺失）=="
if ./build/test_raft_config; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) Raft transport（真实 loopback：握手/四类帧/半包/粘包/坏帧/慢 peer）=="
if ./build/test_raft_transport; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) RaftWorker 启动（真实状态/日志恢复、缺失/损坏拒绝、目录锁）=="
if ./build/test_raft_worker_init; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) RaftWorker 后台线程（真实 TCP/选举/快照/失败停止确认）=="
if timeout 90 ./build/test_raft_worker_live; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) RaftWorker 三进程（心跳/kill -9 重选/重启/130条追平）=="
if timeout 90 python3 tests/raft_worker_process_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) kvserver Raft 入口（状态/信号/预留fd/失败传播/坏配置）=="
if timeout 90 python3 tests/raft_server_startup_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) kvserver 三进程（选主/重选/重启/追平与KV入口门控）=="
if timeout 90 python3 tests/raft_server_cluster_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== $((++STEP)) 拷贝/移动契约（编译期 static_assert）=="
if ./build/test_noncopyable; then
    :
else
    fail=$((fail + 1))
fi

if [ "${1:-}" = "--slow" ]; then
    echo
    echo "== 附加（慢）：崩溃注入 + 写失败故障注入 + 慢 fsync 注入 =="
    if python3 tests/crash_inject_test.py; then
        :
    else
        fail=$((fail + 1))
    fi

    if python3 tests/fail_inject_test.py; then
        :
    else
        fail=$((fail + 1))
    fi

    # 慢 fsync：验证事件循环没被 fsync 堵住（快路径 <100ms），并记录单 worker 的串行代价
    if python3 tests/slow_fsync_test.py; then
        :
    else
        fail=$((fail + 1))
    fi
fi

if [ "${1:-}" = "--asan" ]; then
    echo
    echo "== 附加：ASan/UBSan =="
    if bash tests/run_asan.sh; then
        :
    else
        fail=$((fail + 1))
    fi
fi

echo
if [ "$fail" -eq 0 ]; then
    echo "=== 全部通过 ==="
else
    echo "=== $fail 组失败 ==="
fi
exit "$fail"
