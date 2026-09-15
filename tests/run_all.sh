#!/usr/bin/env bash
# run_all.sh —— 一键跑当前所有测试
#
# 【用法】
#   tests/run_all.sh           跑：构建 + 存储单元测试 + 半关闭 + 背压
#   tests/run_all.sh --asan    额外跑 ASan/UBSan 构建（较慢）
#
# 【当前预期（2026-09-15，Step 0：先固化失败用例）】
#   test_storage        PASS
#   net_halfclose_test  PASS
#   net_backpressure_test FAIL  ← 待修（n == 1 + UAF）
#   整体退出码非 0
set -u

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

echo "== 构建 =="
cmake -B build > /dev/null || exit 2
cmake --build build -j > /dev/null 2>&1 || { echo "构建失败"; exit 2; }
echo "构建完成"

fail=0

echo
echo "== 1/3 存储单元测试（编解码 / 文件头 / put）=="
if ./build/test_storage > /tmp/kv_test_storage.log 2>&1; then
    echo "PASS（日志 /tmp/kv_test_storage.log）"
else
    echo "FAIL："
    grep -n "FAIL" /tmp/kv_test_storage.log | head -10
    fail=$((fail + 1))
fi

echo
echo "== 2/3 半关闭回归 =="
if python3 tests/net_halfclose_test.py; then
    :
else
    fail=$((fail + 1))
fi

echo
echo "== 3/3 背压回归 =="
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
