#!/usr/bin/env python3
"""bench.py —— 单节点 KV 压测（M7）

【测什么】
    · SET / GET / 80-20 混合 三种负载
    · 并发扫描：1 / 4 / 16 / 64 个客户端
    · 输出 QPS、平均延迟、P50、P99

【为什么用多进程客户端，而不是线程】
    第一版用 Python 线程跑，GET 在 C=16 时从 20k 掉到 4k ops/s —— 看着像服务器退化，
    其实是 GIL：16 个线程抢一把解释器锁。换成多进程后同一场景能跑到 33k ops/s。
    → 压测客户端本身不能成为瓶颈，否则量到的是客户端，不是服务器（数字会骗人）。

【为什么这么测（预期结论）】
    每条 SET 都 fsync，而所有存储操作由一个后台 worker 串行执行，所以：
      · SET 吞吐不会随并发增长（被串行 fsync 卡在 ~fsync 延迟的倒数）
      · 并发上升只会让 SET 的延迟线性变长（排队）——这正是 group commit 的动机
      · GET 不落盘，可以真正并行吃到多核 + 页缓存

【用法】
    python3 tests/bench.py            # 每组 3 秒
    python3 tests/bench.py 5          # 每组 5 秒
    KV_SERVER=... KV_PORT=... 可覆盖
"""

import multiprocessing as mp
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kvproto import (OP_SET, OP_GET, ST_OK, ST_NOTFOUND,  # noqa: E402
                     connect, request, remove_db, start_server, stop_server)

DB = "/tmp/kv_bench.data"
LOG = "/tmp/kv_bench.log"

VALUE = b"v" * 64               # 64 字节 value
PRELOAD_KEYS = 1000             # GET 的键空间
HOT_KEYS = 50                   # 混合负载的热键集
WARMUP_OPS = 200
CONCURRENCY = [1, 4, 16, 64]
DEFAULT_DURATION = 3.0

fails = []


def section(name):
    print(f"\n== {name} ==")


def client_loop(cid, op, conc, end_time, keys, value, q):
    """一个客户端进程：闭环（发一条等一条），把统计结果放回队列。

    延迟统计在进程内精确计算（每个连接看到的 P50/P99），避免把几十万个样本搬回父进程。
    """
    try:
        s = connect(timeout=30)
        lat = []
        i = cid
        n_keys = len(keys)

        while time.time() < end_time:
            k = keys[i % n_keys]
            t0 = time.perf_counter()
            if op == OP_SET:
                st, _ = request(s, OP_SET, k, value, timeout=30)
            else:
                st, _ = request(s, OP_GET, k, timeout=30)
            dt = time.perf_counter() - t0

            if st not in (ST_OK, ST_NOTFOUND):
                q.put((cid, len(lat), 0.0, 0.0, 0.0, f"status={st}"))
                s.close()
                return

            lat.append(dt)
            i += conc

        s.close()
        if not lat:
            q.put((cid, 0, 0.0, 0.0, 0.0, "no samples"))
            return
        lat.sort()
        mean = sum(lat) / len(lat)
        p50 = lat[len(lat) // 2]
        p99 = lat[min(len(lat) - 1, int(len(lat) * 0.99))]
        q.put((cid, len(lat), mean, p50, p99, None))
    except Exception as e:  # noqa: BLE001
        q.put((cid, 0, 0.0, 0.0, 0.0, repr(e)))


def run_level(op, conc, duration, keys, value=b"", label=""):
    """跑一组并发，打印 QPS / 平均 / P50 / P99（延迟取各客户端统计的平均值）"""
    q = mp.Queue()
    end_time = time.time() + duration + 0.3      # 留出建连时间
    ps = [mp.Process(target=client_loop, args=(c, op, conc, end_time, keys, value, q))
          for c in range(conc)]

    t0 = time.perf_counter()
    for p in ps:
        p.start()

    stats = [q.get() for _ in ps]
    for p in ps:
        p.join(timeout=30)
    elapsed = time.perf_counter() - t0

    errors = [e for *_, e in stats if e]
    counts = [c for _, c, *_ in stats]
    total = sum(counts)

    if total == 0 or errors:
        print(f"  {label}C={conc:<3d} 无有效数据（错误：{errors[:2]}）")
        fails.append(f"{label}C={conc}")
        return None

    qps = total / elapsed
    n_ok = len([1 for _, c, *_ in stats if c > 0])
    avg_ms = sum(m for _, _, m, _, _, _ in stats) / conc * 1000
    p50_ms = sum(p for _, _, _, p, _, _ in stats) / conc * 1000
    p99_ms = sum(p for _, _, _, _, p, _ in stats) / conc * 1000

    print(f"  {label}C={conc:<3d} {qps:9.1f} ops/s | 平均 {avg_ms:7.2f}ms | "
          f"P50 {p50_ms:7.2f}ms | P99 {p99_ms:7.2f}ms | 样本 {total} | 活跃连接 {n_ok}")
    return qps


def mix_client(cid, conc, end_time, keys, hot, value, q):
    """80% GET + 20% SET（热键集），闭环"""
    try:
        s = connect(timeout=30)
        lat = []
        i = cid
        while time.time() < end_time:
            if i % 5 == 0:                      # 20% 写
                k = hot[i % len(hot)]
                t0 = time.perf_counter()
                st, _ = request(s, OP_SET, k, value, timeout=30)
            else:
                k = keys[i % len(keys)]
                t0 = time.perf_counter()
                st, _ = request(s, OP_GET, k, timeout=30)
            dt = time.perf_counter() - t0

            if st not in (ST_OK, ST_NOTFOUND):
                q.put((cid, len(lat), 0.0, 0.0, 0.0, f"status={st}"))
                s.close()
                return

            lat.append(dt)
            i += conc

        s.close()
        if not lat:
            q.put((cid, 0, 0.0, 0.0, 0.0, "no samples"))
            return
        lat.sort()
        q.put((cid, len(lat), sum(lat) / len(lat), lat[len(lat) // 2],
               lat[min(len(lat) - 1, int(len(lat) * 0.99))], None))
    except Exception as e:  # noqa: BLE001
        q.put((cid, 0, 0.0, 0.0, 0.0, repr(e)))


def run_mix(conc, duration, keys, hot):
    q = mp.Queue()
    end_time = time.time() + duration + 0.3
    ps = [mp.Process(target=mix_client, args=(c, conc, end_time, keys, hot, VALUE, q))
          for c in range(conc)]

    t0 = time.perf_counter()
    for p in ps:
        p.start()
    stats = [q.get() for _ in ps]
    for p in ps:
        p.join(timeout=30)
    elapsed = time.perf_counter() - t0

    errors = [e for *_, e in stats if e]
    total = sum(c for _, c, *_ in stats)
    if total == 0 or errors:
        print(f"  C={conc} 无有效数据（{errors[:2]}）")
        fails.append("mix")
        return None

    print(f"  C={conc:<3d} {total / elapsed:9.1f} ops/s | 平均 "
          f"{sum(m for _, _, m, _, _, _ in stats) / conc * 1000:7.2f}ms | P50 "
          f"{sum(p for _, _, _, p, _, _ in stats) / conc * 1000:7.2f}ms | P99 "
          f"{sum(p for _, _, _, _, p, _ in stats) / conc * 1000:7.2f}ms | 样本 {total}")
    return total / elapsed


def main() -> int:
    duration = float(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DURATION

    remove_db(DB)
    if os.path.exists(LOG):
        os.remove(LOG)

    proc = start_server(DB, LOG)
    print(f"[压测] 服务器 {os.environ.get('KV_SERVER', './build/kvserver')}，"
          f"每组 {duration}s，value {len(VALUE)}B，客户端=多进程")

    try:
        # 预置键空间
        s = connect(timeout=30)
        for i in range(PRELOAD_KEYS):
            request(s, OP_SET, f"key{i}".encode(), VALUE, timeout=30)
        s.close()

        keys = [f"key{i}".encode() for i in range(PRELOAD_KEYS)]
        hot = [f"hot{i}".encode() for i in range(HOT_KEYS)]

        section("SET（每次调用 fsync，单 worker 串行）")
        for c in CONCURRENCY:
            run_level(OP_SET, c, duration, keys, VALUE)

        section("GET（不落盘；可并行吃多核 + 页缓存）")
        for c in CONCURRENCY:
            run_level(OP_GET, c, duration, keys)

        section("混合 80/20（80% GET + 20% SET，热键集 50 个，C=16）")
        run_mix(16, duration, keys, hot)

    finally:
        stop_server(proc, hard=True)

    print()
    if fails:
        print(f"[压测] FAIL —— {fails}")
        return 1
    print("[压测] 完成")
    return 0


if __name__ == "__main__":
    sys.exit(main())
