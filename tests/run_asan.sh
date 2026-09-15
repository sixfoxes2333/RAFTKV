#!/usr/bin/env bash
# run_asan.sh —— 用 AddressSanitizer + UndefinedBehaviorSanitizer 构建并跑测试
#
# 【目的】
#   捕捉内存类错误（use-after-free / 越界 / 双重释放）和未定义行为。
#   网络层已知的 P0 之一（回调 delete c 后继续访问）就是 ASan 抓到的。
#
# 【设计取舍】
#   · 构建目录放在仓库外（默认 /tmp/kv-asan），不污染 git 工作区
#   · 关闭 LeakSanitizer（detect_leaks=0）：程序是被测试脚本 terminate 掉的，
#     退出时的"泄漏"报告会淹没真正的错误；先保证内存错误可见
#   · 判定标准：进程输出里出现 "AddressSanitizer" 或 "runtime error" 即失败
#
# 【当前状态（2026-09-15）】
#   红的 —— 背压场景下稳定触发 heap-use-after-free（handle_write 里 delete 后
#   process_frames 继续用）。这正是要修的第二个 P0。
#
# 【用法】
#   bash tests/run_asan.sh
#   环境变量：KV_ASAN_BUILD 指定构建目录（默认 /tmp/kv-asan）
set -u

REPO="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="${KV_ASAN_BUILD:-/tmp/kv-asan}"
export ASAN_OPTIONS="detect_leaks=0:abort_on_error=0"
export UBSAN_OPTIONS="print_stacktrace=1"

echo "[ASan] 仓库: $REPO"
echo "[ASan] 构建目录: $BUILD（在仓库外，不进 git）"

# ---- 1. 配置并构建（-O0 + -g：报错栈可读；sanitizer 只加在这份构建里）----
cmake -S "$REPO" -B "$BUILD" \
      -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer -g -O0" \
      > /dev/null || { echo "[ASan] cmake 配置失败"; exit 2; }
cmake --build "$BUILD" -j > /dev/null 2>&1 || { echo "[ASan] 构建失败"; exit 2; }
echo "[ASan] 构建完成"

fail=0

# ---- 2. 存储层单元测试：ASan 下跑一遍 ----
echo "[ASan] 运行 test_storage"
if "$BUILD/test_storage" > /tmp/kv_asan_storage.log 2>&1; then
    echo "       test_storage 退出码 0"
else
    echo "       test_storage 非零退出，日志尾部："
    tail -20 /tmp/kv_asan_storage.log
    fail=1
fi
if grep -qE "AddressSanitizer|runtime error" /tmp/kv_asan_storage.log; then
    echo "       !! test_storage 触发了 sanitizer 报告"
    grep -nE "AddressSanitizer|runtime error" /tmp/kv_asan_storage.log | head -5
    fail=1
fi

# ---- 3. 背压场景：已知的 UAF 触发条件（写满 → delete c → 继续访问）----
# 关键：必须把 KV_SERVER 指向 ASan 构建，否则测的是普通二进制（那是假绿）
export KV_SERVER="$BUILD/kvserver"
echo "[ASan] 运行背压场景（当前预期触发 use-after-free），服务器=$KV_SERVER"
python3 "$REPO/tests/net_backpressure_test.py" \
        > /tmp/kv_asan_backpressure.log 2>&1 || true   # 客户端收到的字节数不作判定
SERVER_LOG=/tmp/kv_backpressure_server.log
if grep -qE "AddressSanitizer|runtime error" "$SERVER_LOG" 2>/dev/null; then
    echo "       !! 服务器触发了 sanitizer 报告："
    grep -nE "AddressSanitizer|runtime error" "$SERVER_LOG" | head -5
    echo "       （详细栈见 $SERVER_LOG）"
    fail=1
else
    echo "       服务器日志里没有 sanitizer 报告"
fi

echo
if [ "$fail" -eq 0 ]; then
    echo "[ASan] PASS（没有内存/UB 错误）"
else
    echo "[ASan] FAIL（见上面的报告）"
fi
exit "$fail"
