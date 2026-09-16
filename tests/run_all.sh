#!/usr/bin/env bash
# run_all.sh —— 一键跑当前所有测试
#
# 【用法】
#   tests/run_all.sh           跑：构建 + 存储 + 半关闭 + 半关闭大回复 + 背压 + EMFILE + 启动失败 + 拷贝契约
#   tests/run_all.sh --asan    额外跑 ASan/UBSan 构建（较慢）
#
# 【当前预期（2026-09-16，Step 7 完成）】
#   test_storage              PASS
#   net_halfclose_test        PASS
#   net_halfclose_flush_test  PASS
#   net_backpressure_test     PASS
#   net_emfile_test           PASS
#   net_startup_fail_test     PASS
#   test_noncopyable          PASS（编译期 static_assert：Connection/Reactor 不可拷贝、不可移动）
#   整体退出码 0
#
# 【历史】
#   Step 0       背压红（n == 1 判据 + use-after-free）
#   Step 1 / 2   修完 → 背压转绿（ASan 也转绿）
#   Step 3       新增"半关闭+大回复"语义围栏（先发完再关 + 空闲不许空转）
#   Step 4       读循环到 EAGAIN（epoll_wait 4129 → 16 次）
#   Step 5       accept 循环 + accept4 + EMFILE 占位 fd（空转 2.03s → 0.00s）
#   Step 6       系统调用返回值检查（启动失败非 0 退出、epoll_ctl 失败不静默）
#   Step 7       禁拷贝/禁移动契约 + 编译期断言测试
set -u

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

echo "== 构建 =="
cmake -B build > /dev/null || exit 2
cmake --build build -j > /dev/null 2>&1 || { echo "构建失败"; exit 2; }
echo "构建完成"

fail=0

echo
echo "== 1/7 存储单元测试（编解码 / 文件头 / put）=="
if ./build/test_storage > /tmp/kv_test_storage.log 2>&1; then
    echo "PASS（日志 /tmp/kv_test_storage.log）"
else
    echo "FAIL："
    grep -n "FAIL" /tmp/kv_test_storage.log | head -10
    fail=$((fail + 1))
fi

echo
echo "== 2/7 半关闭回归（小消息）=="
if python3 tests/net_halfclose_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== 3/7 半关闭 + 大回复围栏（先发完再关 + 不空转）=="
if python3 tests/net_halfclose_flush_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== 4/7 背压回归 =="
if python3 tests/net_backpressure_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== 5/7 fd 用尽（EMFILE）不许空转 =="
if python3 tests/net_emfile_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== 6/7 启动失败必须响亮地退出（端口被占）=="
if python3 tests/net_startup_fail_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== 7/7 拷贝/移动契约（编译期 static_assert）=="
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
