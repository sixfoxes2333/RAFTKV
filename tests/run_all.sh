#!/usr/bin/env bash
# run_all.sh —— 一键跑当前所有测试
#
# 【用法】
#   tests/run_all.sh           跑：存储单测 + 恢复测试 + 6 项网络/类型契约
#   tests/run_all.sh --asan    额外跑 ASan/UBSan 构建（较慢）
#
# 【当前预期（2026-09-16，M2d 完成、M2e 未做）】
#   test_storage              PASS
#   test_recovery             FAIL  ← 待修：重开后 keydir 为空 / 残损尾未截断 / CRC 损坏未拒绝
#   net_halfclose_test        PASS
#   net_halfclose_flush_test  PASS
#   net_backpressure_test     PASS
#   net_emfile_test           PASS
#   net_startup_fail_test     PASS
#   test_noncopyable          PASS
#   整体退出码非 0
#
# 【历史】
#   网络层 Step 0-9 已封版并标记 net-v1
#   存储 M1/M2a/M2b：记录格式、文件头、put
#   存储 M2c：append_record + get + CRC 拒绝脏数据
#   存储 M2d：DEL 墓碑 + keydir 删除
#   存储 M2e：待做 recover + torn-tail 截断（本表 test_recovery 转绿）
set -u

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

echo "== 构建 =="
cmake -B build > /dev/null || exit 2
cmake --build build -j > /dev/null 2>&1 || { echo "构建失败"; exit 2; }
echo "构建完成"

fail=0

echo
echo "== 1/8 存储单元测试（记录/文件头/put/get/del）=="
if ./build/test_storage > /tmp/kv_test_storage.log 2>&1; then
    echo "PASS（日志 /tmp/kv_test_storage.log）"
else
    echo "FAIL："
    grep -n "FAIL" /tmp/kv_test_storage.log | head -10
    fail=$((fail + 1))
fi

echo
echo "== 2/8 存储恢复（keydir 重建 / torn-tail 截断 / CRC 损坏拒绝）=="
if ./build/test_recovery > /tmp/kv_test_recovery.log 2>&1; then
    echo "PASS（日志 /tmp/kv_test_recovery.log）"
else
    echo "FAIL："
    grep -n "FAIL" /tmp/kv_test_recovery.log | head -20
    fail=$((fail + 1))
fi

echo
echo "== 3/8 半关闭回归（小消息）=="
if python3 tests/net_halfclose_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== 4/8 半关闭 + 大回复围栏（先发完再关 + 不空转）=="
if python3 tests/net_halfclose_flush_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== 5/8 背压回归 =="
if python3 tests/net_backpressure_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== 6/8 fd 用尽（EMFILE）不许空转 =="
if python3 tests/net_emfile_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== 7/8 启动失败必须响亮地退出（端口被占）=="
if python3 tests/net_startup_fail_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== 8/8 拷贝/移动契约（编译期 static_assert）=="
if ./build/test_noncopyable; then
    :
else
    fail=$((fail + 1))
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
