#!/usr/bin/env bash
# run_all.sh —— 一键跑当前所有测试
#
# 【用法】
#   tests/run_all.sh           跑：构建 + 存储单元测试 + 半关闭 + 半关闭大回复 + 背压
#   tests/run_all.sh --asan    额外跑 ASan/UBSan 构建（较慢）
#
# 【当前预期（2026-09-15，Step 2 完成）】
#   test_storage              PASS
#   net_halfclose_test        PASS
#   net_halfclose_flush_test  PASS   ← Step 3 的语义围栏（现在就该是绿的）
#   net_backpressure_test     PASS
#   整体退出码 0
#
# 【历史】
#   Step 0 时背压是红的（n == 1 判据 + use-after-free），Step 1 / Step 2 修完后转绿。
set -u

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

echo "== 构建 =="
cmake -B build > /dev/null || exit 2
cmake --build build -j > /dev/null 2>&1 || { echo "构建失败"; exit 2; }
echo "构建完成"

fail=0

echo
echo "== 1/4 存储单元测试（编解码 / 文件头 / put）=="
if ./build/test_storage > /tmp/kv_test_storage.log 2>&1; then
    echo "PASS（日志 /tmp/kv_test_storage.log）"
else
    echo "FAIL："
    grep -n "FAIL" /tmp/kv_test_storage.log | head -10
    fail=$((fail + 1))
fi

echo
echo "== 2/4 半关闭回归（小消息）=="
if python3 tests/net_halfclose_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== 3/4 半关闭 + 大回复围栏（先发完再关 + 不空转）=="
if python3 tests/net_halfclose_flush_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== 4/4 背压回归 =="
if python3 tests/net_backpressure_test.py; then
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
