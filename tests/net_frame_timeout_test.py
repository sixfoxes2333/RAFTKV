#!/usr/bin/env python3
"""net_frame_timeout_test.py —— "半条帧"不能无限期占着连接（#10 方案 B）

【要证明什么】
服务端要为"发了一半的帧"缓存字节（in_）。如果对端发到一半就【再也不动】，
这条连接和它占的内存会一直留着 —— 典型的 slow-loris：
  慢速/恶意客户端只要开很多连接、每条发半个帧，就能长期占住内存和 fd
（方案 A 的全局预算能封住内存总量，但被预算挤掉的可能是【正常连接】；
  真正对症的是"部分帧无进展就断开"。）

【判据】
  1) 只发了半个帧、然后完全不动 → 服务端必须在超时后**关掉这条连接**
     （测试用 KV_FRAME_TIMEOUT_MS=800 把超时调短，默认 30s）
  2) 反面：**慢但有进展**的客户端不许被误杀 —— 4MB 的请求分成 100KB 一块、
     每块间隔 100ms（每次都短于超时）必须正常完成并拿到回复
  3) 服务端仍然健康（正常 set/get 可用）

【修复方向】连接上记一个"最后一次收到新字节"的时间戳；在事件循环里（epoll_wait
  改成带 1s 超时）扫一遍：in_ 非空 且 距上次进展 > T → 记日志 + request_close。
  超时值从环境变量 KV_FRAME_TIMEOUT_MS 读（默认 30000），便于测试调短。

【用法】python3 tests/net_frame_timeout_test.py
"""

import os
import socket
import struct
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kvproto import (PORT, OP_SET, OP_GET, ST_OK,  # noqa: E402
                     connect, decode_response, encode_request, recv_frame,
                     remove_db, request, start_server, stop_server)

DB = "/tmp/kv_frametimeout.data"
LOG = "/tmp/kv_frametimeout.log"

TIMEOUT_MS = 800            # 测试用的短超时（通过环境变量传给服务器）
WAIT = 3.0                  # 客户端等这么久，看连接有没有被服务端关掉
SLOW_TOTAL = 4 * 1024 * 1024    # 反面用例：4MB 请求
SLOW_CHUNK = 100 * 1024         # 每次发 100KB
SLOW_GAP = 0.1                  # 每块之间停 100ms（< 超时，所以属于"有进展"）

fails = []


def check(label, cond, detail=""):
    print(f"  {'ok   ' if cond else 'FAIL '}: {label}" + (f"   {detail}" if detail and not cond else ""))
    if not cond:
        fails.append(label)


def main() -> int:
    remove_db(DB)
    env = {"KV_FRAME_TIMEOUT_MS": str(TIMEOUT_MS)}
    proc = start_server(DB, LOG, env=env)
    print(f"[半条帧超时] 服务器已启动（KV_FRAME_TIMEOUT_MS={TIMEOUT_MS}）")

    try:
        # ---------- 1) 发半个帧然后不动：必须被关掉 ----------
        print(f"\n[1] 发一个声明 1MB 的帧头 + 100 字节，然后完全不动，等 {WAIT:.0f}s")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(WAIT + 2.0)
        s.connect(("127.0.0.1", PORT))
        s.sendall(struct.pack(">I", 1024 * 1024) + b"z" * 100)

        closed = False
        err = None
        deadline = time.time() + WAIT
        s.settimeout(WAIT)
        try:
            data = s.recv(1)
            closed = (data == b"")
            if data:
                err = f"收到意外数据 {data!r}"
        except socket.timeout:
            closed = False
        except OSError as e:
            closed = True
            err = f"连接被重置：{type(e).__name__}"
        dt = WAIT - (deadline - time.time())
        print(f"  连接被服务端关闭 = {closed}" + (f"（{err}）" if err else "（静默）"))
        check("半条帧无进展 → 服务端必须关掉这条连接", closed,
              f"{WAIT:.1f}s 内连接一直开着（data=None），slow-loris 可以永久占住它")
        s.close()

        # ---------- 2) 反面：慢但有进展的客户端不许被误杀 ----------
        print(f"\n[2] 反面用例：{SLOW_TOTAL // 1024 // 1024}MB 的请求分 {SLOW_CHUNK // 1024}KB 一块、"
              f"每块间隔 {SLOW_GAP * 1000:.0f}ms（都短于超时）")
        req = encode_request(OP_SET, b"slowkey", b"v" * SLOW_TOTAL)
        s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s2.settimeout(30)
        s2.connect(("127.0.0.1", PORT))
        sent = 0
        while sent < len(req):
            n = s2.send(req[sent:sent + SLOW_CHUNK])
            sent += n
            time.sleep(SLOW_GAP)
        print(f"  已分块发完 {sent} 字节（用时约 {sent // SLOW_CHUNK * SLOW_GAP:.1f}s）")
        body = recv_frame(s2, 10.0)
        st = decode_response(body)[0] if body else None
        check("慢但有进展的请求必须正常完成（超时不能误杀）", st == ST_OK,
              f"服务端回复 status={st}")
        s2.close()

        # ---------- 3) 服务端仍然健康 ----------
        s3 = connect()
        st1, _ = request(s3, OP_SET, b"k", b"v", timeout=10.0)
        st2, val = request(s3, OP_GET, b"k", timeout=10.0)
        check("服务端仍然健康（正常 set/get 可用）", st1 == ST_OK and st2 == ST_OK and val == b"v",
              f"set={st1} get={st2} val={val!r}")
        s3.close()

    finally:
        stop_server(proc, hard=True)

    print()
    if fails:
        print(f"[半条帧超时] FAIL —— {len(fails)} 项失败：{fails}")
        print("  → 修复方向：连接记『最后一次收到新字节』时间戳；事件循环（epoll_wait 带 1s 超时）")
        print("           里扫『in_ 非空 且 超时』的连接 → 记日志 + request_close")
        print("           超时值读 KV_FRAME_TIMEOUT_MS（默认 30000）")
        return 1
    print("[半条帧超时] PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
