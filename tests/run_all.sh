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
