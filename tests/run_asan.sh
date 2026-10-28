#!/usr/bin/env bash
# run_asan.sh —— 用 AddressSanitizer + UndefinedBehaviorSanitizer 构建并跑测试
#
# 【目的】
#   捕捉内存类错误（use-after-free / 越界 / 双重释放 / 泄漏）和未定义行为。
#   网络层曾经的两个 P0 就是 ASan/审查发现的：
#     · 回调里 delete c 之后继续访问（heap-use-after-free）
#     · write 失败判据写成 n == 1（功能 bug，由背压测试发现）
#
# 【设计取舍】
#   · 构建目录放在仓库外（默认 /tmp/kv-asan），不污染 git 工作区
#   · 关闭 LeakSanitizer（detect_leaks=0）：网络场景的进程是被测试脚本 terminate 的，
#     退出时的"泄漏"报告会淹没真正的错误；先保证内存错误可见
#   · 判定标准（两层，缺一不可）：
#       1) 每个测试可执行文件必须退出码 0
#       2) 日志里不能出现 AddressSanitizer / runtime error / LeakSanitizer
#   · 网络场景额外要求"确实跑起来了"：客户端必须有启动标记、服务器必须 accept 过连接。
#     否则服务器没起来 / 端口被占 时也会报 PASS —— 那是假通过
#
# 【用法】
#   bash tests/run_asan.sh
#   环境变量：KV_ASAN_BUILD 指定构建目录（默认 /tmp/kv-asan）
set -u

REPO="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="${KV_ASAN_BUILD:-/tmp/kv-asan}"
export ASAN_OPTIONS="detect_leaks=0:abort_on_error=0"
export UBSAN_OPTIONS="print_stacktrace=1"

SAN_PATTERN="AddressSanitizer|runtime error|LeakSanitizer"

#每轮开始清一次日志（start_server 现在追加写日志，跨轮次必须清旧的）
rm -f /tmp/kv_*.log

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

# ---- 2. 所有测试可执行文件：查退出码 + 查 sanitizer 输出 ----
run_bin()
{
    local name="$1"
    local log="/tmp/kv_asan_${name}.log"

    if "$BUILD/$name" > "$log" 2>&1; then
        echo "       $name 退出码 0"
    else
        echo "       $name 非零退出，日志尾部："
        tail -20 "$log"
        fail=1
    fi

    if grep -qE "$SAN_PATTERN" "$log"; then
        echo "       !! $name 触发了 sanitizer 报告："
        grep -nE "$SAN_PATTERN" "$log" | head -5
        fail=1
    fi
}

echo "[ASan] 运行存储单元测试"
run_bin test_storage
echo "[ASan] 运行存储恢复测试"
run_bin test_recovery
echo "[ASan] 运行类型契约测试（编译期断言，顺带确认可执行）"
run_bin test_noncopyable

echo "[ASan] 运行 Raft term/vote 持久化测试"
run_bin test_raft_state
echo "[ASan] 运行 Raft 日志存储测试"
run_bin test_logstore
echo "[ASan] 运行 Raft Core 初始化/发起选举测试"
run_bin test_raft_core
echo "[ASan] 运行 Raft 日志复制打包/接收测试"
run_bin test_raft_append
echo "[ASan] 运行 Raft 复制落地测试（真实 LogStore/StateStore）"
run_bin test_raft_append_persist
echo "[ASan] 运行 Raft 复制回复/追平测试（内存日志/保存模型）"
run_bin test_raft_append_response
echo "[ASan] 运行 Raft 本地提案测试（构造/真实保存同步/复制闭环）"
run_bin test_raft_proposal
echo "[ASan] 运行 leader提交测试（当前任期多数派与新增区间）"
run_bin test_raft_commit
echo "[ASan] 运行 follower提交/真实存储提交通知测试"
run_bin test_raft_follower_commit
echo "[ASan] 运行 Core综合场景（可控消息队列/真实存储重启；非TCP集群）"
run_bin test_raft_core_scenarios
echo "[ASan] 运行 Raft RPC 帧编解码测试"
run_bin test_raft_rpc_codec
echo "[ASan] 运行 Raft RPC 正文编解码测试"
run_bin test_raft_rpc_codec_body
echo "[ASan] 运行 Raft 入站校验测试"
run_bin test_raft_rpc_validate
echo "[ASan] 运行 Raft 配置解析测试"
run_bin test_raft_config
echo "[ASan] 运行 Raft transport 测试（真实 loopback socket）"
run_bin test_raft_transport

# ---- 3. 网络场景：背压（写满 → EAGAIN → 短写续写），必须真的跑起来 ----
export KV_SERVER="$BUILD/kvserver"
CLIENT_LOG=/tmp/kv_asan_backpressure.log
SERVER_LOG=/tmp/kv_backpressure_server.log
rm -f "$CLIENT_LOG" "$SERVER_LOG"

echo "[ASan] 运行背压场景（服务器=$KV_SERVER）"
python3 "$REPO/tests/net_backpressure_test.py" > "$CLIENT_LOG" 2>&1 || true
# 客户端收到的字节数这里不作判定（那是功能测试的职责），但"跑没跑起来"必须判定

started=0
accepted=0
transferred=0
grep -q "启动服务器" "$CLIENT_LOG" 2>/dev/null && started=1
grep -q "new conn fd" "$SERVER_LOG" 2>/dev/null && accepted=1
grep -q "收到 " "$CLIENT_LOG" 2>/dev/null && transferred=1

if [ "$started" -eq 1 ] && [ "$accepted" -eq 1 ] && [ "$transferred" -eq 1 ]; then
    echo "       场景已真实执行（客户端启动服务器 / 服务器 accept / 数据收发都有）"
    echo "       客户端结论：$(grep -E '\[背压测试\] (PASS|FAIL)' "$CLIENT_LOG" | tail -1)"
else
    #假通过防护：缺任一标记说明场景没真正跑，此时"没有 sanitizer 报告"毫无意义
    echo "       !! 场景没有真正执行 —— 本次结果不能作为通过依据"
    echo "          客户端启动标记 = $started，服务器 accept 标记 = $accepted，数据收发标记 = $transferred"
    echo "          客户端日志尾部："
    tail -5 "$CLIENT_LOG" 2>/dev/null
    echo "          服务器日志尾部："
    tail -5 "$SERVER_LOG" 2>/dev/null
    fail=1
fi

if grep -qE "$SAN_PATTERN" "$SERVER_LOG" 2>/dev/null; then
    echo "       !! 服务器触发了 sanitizer 报告："
    grep -nE "$SAN_PATTERN" "$SERVER_LOG" | head -5
    echo "       （详细栈见 $SERVER_LOG）"
    fail=1
else
    echo "       服务器日志里没有 sanitizer 报告"
fi

# ---- 4. 端到端场景：协议 + 后台存储线程 + kill -9 重启（多线程路径也要过 sanitizer）----
E2E_CLIENT_LOG=/tmp/kv_asan_e2e.log
E2E_SERVER_LOG=/tmp/kv_e2e.log
rm -f "$E2E_CLIENT_LOG" "$E2E_SERVER_LOG"

echo "[ASan] 运行单节点端到端场景（服务器=$KV_SERVER）"
python3 "$REPO/tests/kv_protocol_test.py" > "$E2E_CLIENT_LOG" 2>&1 || true

if grep -q "\[KV 协议测试\] PASS" "$E2E_CLIENT_LOG" 2>/dev/null; then
    echo "       客户端结论：$(grep -E '\[KV 协议测试\] (PASS|FAIL)' "$E2E_CLIENT_LOG" | tail -1)"
else
    echo "       !! 端到端场景没有通过 —— 本次结果不能作为通过依据"
    tail -8 "$E2E_CLIENT_LOG" 2>/dev/null
    fail=1
fi

if grep -qE "$SAN_PATTERN" "$E2E_SERVER_LOG" 2>/dev/null; then
    echo "       !! 端到端服务器触发了 sanitizer 报告："
    grep -nE "$SAN_PATTERN" "$E2E_SERVER_LOG" | head -5
    fail=1
else
    echo "       端到端服务器日志里没有 sanitizer 报告"
fi

echo
if [ "$fail" -eq 0 ]; then
    echo "[ASan] PASS（没有内存/UB 错误）"
else
    echo "[ASan] FAIL（见上面的报告）"
fi
exit "$fail"
