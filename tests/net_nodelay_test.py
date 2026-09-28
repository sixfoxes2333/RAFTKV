#!/usr/bin/env python3
"""net_nodelay_test.py —— 流水线围栏：服务器不能把回复攒着（修复 #11）

【原理】
    同一个服务器、同一个客户端，只改"流水线深度"：
      depth=1 （闭环）：每次只等一条回复 → 不受 Nagle 影响
      depth=16        ：一批 16 条小回复 → 服务端 Nagle + 客户端延迟 ACK
                        会把回复扣住约 40ms，整批才发出去
    所以：如果服务器没设 TCP_NODELAY，depth=16 反而比 depth=1 略慢一个数量级。

【实测（修复前，WSL2）】
    GET depth=1   → 6 259 ops/s，P50 132µs
    GET depth=16  →   316 ops/s，P50 43.9ms     ← 比值 0.05
    修复后同一台机器 depth=16 应显著高于 depth=1（预期 5~20 倍）

【判据】
    ratio = qps(depth=16) / qps(depth=1) >= 2.0
      · 修复前：0.05 → FAIL（这就是红）
      · 修复后：应远大于 2
    用比值而不是绝对值：换机器时两者一起变，比值稳定。

【用法】python3 tests/net_nodelay_test.py
"""

import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kvproto import PORT, remove_db, start_server, stop_server  # noqa: E402

BENCH = os.environ.get("KV_BENCH", "./build/kvbench")
DB = "/tmp/kv_nodelay.data"
LOG = "/tmp/kv_nodelay.log"

KEYS = 200             # 键空间（预置用 SET，每条都 fsync，所以别太大）
DURATION = 1.5         # 每组测量时长
RATIO_MIN = 2.0        # 流水线必须比闭环快至少这么多倍

fails = []


def check(label, cond, detail=""):
    print(f"  {'ok   ' if cond else 'FAIL '}: {label}" + (f"   {detail}" if detail and not cond else ""))
    if not cond:
        fails.append(label)


def run_bench(*args, timeout=120):
    """跑一次 kvbench，返回 (qps, p50_us, 原始输出)"""
    cmd = [BENCH, "--port", str(PORT), *args]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)

    if p.returncode != 0:
        print(f"  !! kvbench 退出码 {p.returncode}：{p.stderr.strip()[:200]}")
        return None, None, p.stdout

    qps = p50 = None
    for line in p.stdout.splitlines():
        if line.startswith("RESULT "):
            for field in line.split()[1:]:
                k, _, v = field.partition("=")
                if k == "qps":
                    qps = float(v)
                elif k == "p50_us":
                    p50 = float(v)

    return qps, p50, p.stdout


def main() -> int:
    remove_db(DB)
    if os.path.exists(LOG):
        os.remove(LOG)

    proc = start_server(DB, LOG)
    print(f"[流水线围栏] 服务器已启动，键空间 {KEYS}，每组 {DURATION}s")

    try:
        # 预置键空间（SET 会 fsync，200 条约 1s）
        qps, _, _ = run_bench("--op", "set", "--keyspace", str(KEYS),
                              "--pipeline", "16", "--conns", "1",
                              "--duration", "0.2", "--warmup", "0.1")
        if qps is None:
            print("  !! 预置数据失败")
            return 1

        print("\n[1] 闭环（depth=1）")
        qps1, p50_1, _ = run_bench("--op", "get", "--keyspace", str(KEYS),
                                   "--pipeline", "1", "--conns", "1",
                                   "--duration", str(DURATION), "--warmup", "0.3")
        if qps1 is None:
            return 1
        print(f"  {qps1:9.1f} ops/s，P50 {p50_1:.0f}us")

        print("\n[2] 流水线（depth=16）")
        qps16, p50_16, _ = run_bench("--op", "get", "--keyspace", str(KEYS),
                                     "--pipeline", "16", "--conns", "1",
                                     "--duration", str(DURATION), "--warmup", "0.3")
        if qps16 is None:
            return 1
        print(f"  {qps16:9.1f} ops/s，P50 {p50_16:.0f}us")

        ratio = qps16 / qps1 if qps1 > 0 else 0
        print(f"\n  比值 = {ratio:.3f}（要求 >= {RATIO_MIN}）")

        check("流水线吞吐显著高于闭环（服务器没有攒回复）", ratio >= RATIO_MIN,
              f"ratio={ratio:.3f}，闭环 {qps1:.1f} / 流水线 {qps16:.1f}")
        check("流水线 P50 在毫秒级以内（没有被 Nagle 扣住 ~40ms）", p50_16 < 5000,
              f"P50={p50_16:.0f}us")

    finally:
        stop_server(proc, hard=True)

    print()
    if fails:
        print(f"[流水线围栏] FAIL —— {len(fails)} 项失败：{fails}")
        print("  → 预期：修复 #11（accept 后 setsockopt(TCP_NODELAY)）后转绿")
        return 1
    print("[流水线围栏] PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
