#!/usr/bin/env python3
"""slow_fsync_test.py —— 慢 fsync 不阻塞事件循环（M6e）

【这个测试在验证什么】
    把 fsync 拖慢到 800ms（LD_PRELOAD 注入 KV_DELAY_FSYNC_MS），然后验证：

      [1] 事件循环没有被堵住：慢 fsync 进行中，另一个连接发"协议错误请求"
          （解析失败 → 回 Error，**不碰存储**）必须 < 100ms 返回。
          如果 Reactor 线程被 fsync 堵住，这条会退化成 ~800ms。
      [2] 慢 fsync 期间新连接仍能被 accept（< 100ms）
      [3] 已知代价（明确记录，不是失败）：单 worker 串行 → 另一个连接的 GET
          会排在慢 SET 后面，延迟 ≈ 剩余 fsync 时间。
          这正是"存储层无锁"付出的代价；将来做 group commit / 多 worker 时
          这个期望需要重新评估（那时这条断言会提醒你更新文档）。

【为什么这样设计】
    "证明事件循环没被堵住"不能靠"感觉响应很快"，要挑一条**完全不经过 worker** 的路径：
    协议解析失败 → 直接回 Error。它的延迟只反映 Reactor 线程的健康度。

【用法】python3 tests/slow_fsync_test.py
"""

import os
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kvproto import (PORT, OP_SET, OP_GET, ST_OK, ST_ERROR, ST_NOTFOUND,  # noqa: E402
                     connect, encode_request, frame, recv_frame, decode_response, request,
                     remove_db, start_server, stop_server)

DB = "/tmp/kv_slow_fsync.data"
LOG = "/tmp/kv_slow_fsync.log"
SHIM = "/tmp/kv_fail_inject.so"
SHIM_SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fail_inject.c")

FSYNC_DELAY_MS = 800          # 注入的慢 fsync 时长
FAST_PATH_BUDGET = 0.100      # 快路径延迟上限：100ms（远小于 800ms）
SETTLE = 0.15                 # 发送慢 SET 之后等一会儿，确保 fsync 已经开始

fails = []


def check(label, cond, detail=""):
    print(f"  {'ok   ' if cond else 'FAIL '}: {label}" + (f"   {detail}" if detail and not cond else ""))
    if not cond:
        fails.append(label)


def build_shim():
    if os.path.exists(SHIM) and os.path.getmtime(SHIM) > os.path.getmtime(SHIM_SRC):
        return
    r = subprocess.run(["gcc", "-shared", "-fPIC", "-O0", "-g", "-o", SHIM, SHIM_SRC, "-ldl"],
                       capture_output=True)
    if r.returncode != 0:
        print("编译 fail_inject.c 失败：", r.stderr.decode()[:400])
        sys.exit(2)


def main() -> int:
    build_shim()
    remove_db(DB)
    if os.path.exists(LOG):
        os.remove(LOG)

    env = {
        "LD_PRELOAD": SHIM,
        "KV_DELAY_FSYNC_MS": str(FSYNC_DELAY_MS),
    }
    proc = start_server(DB, LOG, env=env)
    print(f"[慢 fsync 测试] 服务器已启动，每次 fsync 延迟 {FSYNC_DELAY_MS}ms")

    def malformed_roundtrip(sock, timeout=10):
        """发一条非法 op 的帧，返回 (是否拿到 Error, 耗时秒)。这条路径不碰存储"""
        sock.sendall(frame(bytes([9]) + b"\x00\x00\x00\x01\x00\x00\x00\x00a"))
        t0 = time.perf_counter()
        body = recv_frame(sock, timeout)
        dt = time.perf_counter() - t0
        return (body is not None and body[0] == ST_ERROR), dt
    try:
        # ---- 预热：确认服务器已经进入事件循环（不是"端口在听但还没就绪"） ----
        warm = connect(timeout=30)
        st, _ = request(warm, OP_GET, b"warmup", timeout=30)
        check("预热请求得到响应（NotFound）", st == ST_NOTFOUND, f"status={st}")

        # ---- 基线：worker 空闲时的快路径耗时 ----
        base_ok, base_dt = malformed_roundtrip(warm)
        print(f"  基线（空闲）快路径 = {base_dt * 1000:.2f}ms")
        check("基线快路径正常", base_ok)
        warm.close()

        # ---------- 起一个会长时间占用 worker 的 SET ----------
        a = connect(timeout=30)
        set_result = {}

        def do_slow_set():
            t0 = time.perf_counter()
            st, _ = request(a, OP_SET, b"slow", b"v", timeout=30)
            set_result["status"] = st
            set_result["elapsed"] = time.perf_counter() - t0

        th = threading.Thread(target=do_slow_set)
        th.start()
        time.sleep(SETTLE)   # 此刻 worker 应该正在慢 fsync 里
        # ---------- [1] 快路径：协议错误请求 ----------
        print("[1] 慢 fsync 期间，协议错误请求走快路径")
        b = connect(timeout=10)
        ok_status, fast = malformed_roundtrip(b)
        print(f"  慢 fsync 期间快路径 = {fast * 1000:.1f}ms（上限 {FAST_PATH_BUDGET * 1000:.0f}ms，"
              f"基线 {base_dt * 1000:.2f}ms）")
        check("Error 响应正确", ok_status)
        check("快路径 < 100ms（Reactor 没被 fsync 堵住）", fast < FAST_PATH_BUDGET,
              f"实际 {fast * 1000:.1f}ms")
        b.close()

        # ---------- [2] 慢 fsync 期间新连接 ----------
        print("[2] 慢 fsync 期间新连接")
        t0 = time.perf_counter()
        c = connect(timeout=10)
        conn_dt = time.perf_counter() - t0
        print(f"  建连耗时 {conn_dt * 1000:.1f}ms")
        check("新连接建立 < 100ms", conn_dt < FAST_PATH_BUDGET, f"实际 {conn_dt * 1000:.1f}ms")

        # ---------- [3] GET 排在慢 SET 后面（已知代价） ----------
        print("[3] 存储操作串行（已知代价，记录数字）")
        t0 = time.perf_counter()
        st, val = request(c, OP_GET, b"slow", timeout=30)
        get_dt = time.perf_counter() - t0
        print(f"  GET 耗时 {get_dt * 1000:.1f}ms（它必须排在慢 SET 之后）")
        check("GET 最终成功读到值", st == ST_OK and val == b"v", f"status={st} value={val!r}")
        check("GET 确实排在慢 SET 之后（>= 300ms）—— 单 worker 的代价", get_dt > 0.3,
              f"实际 {get_dt * 1000:.1f}ms")
        c.close()

        th.join(timeout=30)
        a.close()

        # ---------- 注入有效性 ----------
        print("[4] 注入是否真的生效")
        check("慢 SET 最终返回 Ok", set_result.get("status") == ST_OK)
        check(f"SET 耗时 >= {FSYNC_DELAY_MS / 1000 / 2:.1f}s（说明 fsync 被拖慢了）",
              set_result.get("elapsed", 0) >= FSYNC_DELAY_MS / 2000,
              f"实际 {set_result.get('elapsed', 0) * 1000:.1f}ms")

        # 慢下来之后一切仍然正常
        d = connect(timeout=10)
        st, _ = request(d, OP_SET, b"after", b"ok", timeout=30)
        check("慢 fsync 结束后一切正常", st == ST_OK)
        d.close()

    finally:
        stop_server(proc, hard=True)

    print()
    if fails:
        print(f"[慢 fsync 测试] FAIL —— {len(fails)} 项失败：{fails}")
        return 1
    print("[慢 fsync 测试] PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
