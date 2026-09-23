#!/usr/bin/env bash
# TSan 数据竞争检测：目前针对 StorageWorker（项目里第一处多线程代码）
#
# 用法：bash tests/run_tsan.sh
#
# 为什么单独一个脚本：
#   1) 普通构建不带 -fsanitize=thread，竞争检测只在需要时手动跑
#   2) WSL 下 TSan 会因为 ASLR 随机化而随机报
#        "FATAL: ThreadSanitizer: unexpected memory mapping"
#      这是环境问题，不是代码问题 → 用 setarch -R 关闭 ASLR 后稳定通过
set -uo pipefail

cd "$(dirname "$0")/.."

OUT=/tmp/kv-tsan
RUNS=5
mkdir -p "$OUT"

echo "== 构建（-fsanitize=thread）=="
g++ -std=c++17 -Wall -Wextra -fsanitize=thread -g -O1 -Isrc -pthread \
    tests/test_storage_worker.cpp \
    src/app/storage_worker.cpp \
    src/net/protocol.cpp \
    src/storage/storage.cpp \
    src/storage/record.cpp \
    -o "$OUT/test_storage_worker_tsan" || exit 1

echo "== 运行 ${RUNS} 次（setarch -R 关闭 ASLR）=="
failed=0
for i in $(seq 1 "$RUNS"); do
    log="$OUT/run_$i.log"
    setarch "$(uname -m)" -R "$OUT/test_storage_worker_tsan" > "$log" 2>&1
    rc=$?

    if grep -q "WARNING: ThreadSanitizer" "$log"; then
        echo "  run${i}: FAIL —— 检测到数据竞争（详见 $log）"
        grep -A 20 "WARNING: ThreadSanitizer" "$log" | head -30
        failed=1
    elif [ $rc -ne 0 ]; then
        echo "  run${i}: FAIL —— 退出码 $rc（详见 $log）"
        tail -5 "$log"
        failed=1
    else
        echo "  run${i}: PASS（无数据竞争）"
    fi
done

echo
echo "== 服务器端到端（Reactor 线程 ⇄ worker 线程）=="
g++ -std=c++17 -Wall -Wextra -fsanitize=thread -g -O1 -Isrc -pthread \
    src/main.cpp \
    src/net/reactor.cpp src/net/connection.cpp src/base/buffer.cpp \
    src/storage/storage.cpp src/storage/record.cpp \
    src/net/protocol.cpp src/app/storage_worker.cpp \
    -o "$OUT/kvserver_tsan" || exit 1

pkill -f kvserver_tsan 2>/dev/null; sleep 0.3
rm -f /tmp/kv_e2e.log /tmp/kv_e2e.data
# 服务器进程同样要关 ASLR（WSL 下 TSan 的 shadow memory 与地址随机化冲突）
cat > "$OUT/kvserver_tsan_wrap.sh" <<EOF
#!/usr/bin/env bash
exec setarch \$(uname -m) -R "$OUT/kvserver_tsan" "\$@"
EOF
chmod +x "$OUT/kvserver_tsan_wrap.sh"

KV_SERVER="$OUT/kvserver_tsan_wrap.sh" python3 tests/kv_protocol_test.py > "$OUT/e2e_client.log" 2>&1 || true

if grep -q "\[KV 协议测试\] PASS" "$OUT/e2e_client.log"; then
    echo "  客户端结论：$(grep -E '\[KV 协议测试\] (PASS|FAIL)' "$OUT/e2e_client.log" | tail -1)"
else
    echo "  FAIL —— 端到端没有通过（详见 $OUT/e2e_client.log）"
    tail -8 "$OUT/e2e_client.log"
    failed=1
fi

if grep -q "WARNING: ThreadSanitizer" /tmp/kv_e2e.log 2>/dev/null; then
    echo "  FAIL —— 服务器报告了数据竞争："
    grep -A 20 "WARNING: ThreadSanitizer" /tmp/kv_e2e.log | head -30
    failed=1
else
    echo "  服务器（Reactor 线程 + worker 线程）未报告数据竞争"
fi

if [ $failed -eq 0 ]; then
    echo "== TSan: ${RUNS}/${RUNS} 通过，未发现数据竞争 =="
else
    echo "== TSan: 失败 =="
fi

exit $failed
