#!/usr/bin/env python3
"""net_outqueue_test.py —— 发送队列水位线（review #3：out_ 无界 → OOM）

【这个测试在验证什么】
    服务器已经把回复放进发送队列、但对端一直不读时，服务器必须：
      [1] 有硬上限：待发数据累计到上限就放弃这个连接，而不是无限增长
      [2] 有高/低水位：超过高水位暂停读（把压力转成 TCP 背压），降回低水位后恢复读
      [3] 全过程内存有界（读 /proc/<pid>/status 的 VmHWM 验证）

【为什么必须有它】
    "暂停读"这条路径只有在"对端不读"时才会走到。没有这个用例，
    水位线写错了（比如暂停之后永远不恢复）也测不出来 —— 表现为连接挂死，
    而不是报错。所以 [2] 专门验证"暂停之后还能恢复"。

【用法】
    python3 tests/net_outqueue_test.py
    环境变量：KV_SERVER / KV_PORT
"""

import os
import socket
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kvproto import (PORT, OP_SET, OP_GET, ST_OK,  # noqa: E402
                     encode_request, decode_response, recv_frame,
                     remove_db, start_server, stop_server)

DB = "/tmp/kv_outqueue.data"
LOG = "/tmp/kv_outqueue.log"

VALUE_SIZE = 2 * 1024 * 1024      # 2MB：三条回复就超过 2MB 高水位
N_REQUESTS = 40                   # 40 × 2MB = 80MB：远超 17MB 硬上限
MEM_BOUND_MB = 400                # 服务器 VmHWM 上限（宽松上界，只为证明"没有无界增长"）

fails = []


def check(label, cond, detail=""):
    print(f"  {'ok   ' if cond else 'FAIL '}: {label}" + (f"   {detail}" if detail and not cond else ""))
    if not cond:
        fails.append(label)


def vm_hwm_mb(pid: int):
    """读 /proc/<pid>/status 的高水位常驻内存（VmHWM），单位 MB"""
    try:
        with open(f"/proc/{pid}/status", "r") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        return None
    return None


def small_rcvbuf_socket(port: int) -> socket.socket:
    """收缓冲压到 4096 的连接：对端不读时，TCP 窗口很快关闭"""
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    s.settimeout(10.0)
    s.connect(("127.0.0.1", port))
    return s


def main() -> int:
    remove_db(DB)
    if os.path.exists(LOG):
        os.remove(LOG)

    proc = start_server(DB, LOG)
    try:
        # 预置数据
        setup = socket.create_connection(("127.0.0.1", PORT), timeout=30)
        value = os.urandom(VALUE_SIZE)
        setup.sendall(encode_request(OP_SET, b"big", value))
        body = recv_frame(setup, 30)
        if body is None or body[0] != ST_OK:
            print(f"[队列水位测试] 预置数据失败：{body!r}")
            return 1
        setup.close()

        hwm_before = vm_hwm_mb(proc.pid)

        # ---------- [1] 硬上限：对端不读 + 大量请求 ----------
        print("[1] 硬上限（对端不读，流水线大量大回复请求）")
        s = small_rcvbuf_socket(PORT)
        s.settimeout(30.0)
        # 关键：发完请求后【先什么都不读】，让待发数据在服务器侧累积到硬上限
        #（如果一边发一边读，out_ 永远长不起来 —— 那测的是另一条路径）
        try:
            for _ in range(N_REQUESTS):
                s.sendall(encode_request(OP_GET, b"big"))
        except socket.timeout:
            # 暂停读生效后，客户端 send 会被 TCP 窗口挡住 —— 这是允许的（背压生效）
            print("  （发送阶段被 TCP 背压挡住，属预期）")

        print(f"  已发出 {N_REQUESTS} 条请求，故意不读 5 秒，等服务器撞上硬上限…")
        time.sleep(5.0)

        # 现在开始读：应该读到"已经进了内核缓冲的那部分"，然后 EOF（服务器放弃了这条连接）
        closed = False
        got = 0
        deadline = time.time() + 20
        s.settimeout(5.0)
        while time.time() < deadline:
            try:
                chunk = s.recv(65536)
            except (socket.timeout, ConnectionResetError):
                break
            if not chunk:
                closed = True
                break
            got += len(chunk)
        s.close()
        print(f"  读到 {got} 字节后 {'EOF' if closed else '仍未关闭'}")

        check("服务器在硬上限处关闭连接（没有无界排队）", closed)
        check("服务器进程仍然存活", proc.poll() is None)

        # ---------- [2] 水位恢复：暂停读之后必须能恢复 ----------
        print("[2] 水位恢复（暂停读 → 客户端开始读 → 必须恢复服务）")
        s = small_rcvbuf_socket(PORT)
        s.settimeout(30.0)

        # 三条 2MB 回复 = 6MB > 高水位 2MB → 服务器暂停读；客户端此刻故意不读
        for _ in range(3):
            s.sendall(encode_request(OP_GET, b"big"))
        time.sleep(1.0)

        # 现在开始读：把三条回复全部读完（服务器随之降到低水位以下 → 恢复读）
        got_values = 0
        read_ok = True
        for _ in range(3):
            body = recv_frame(s, 30)
            if body is None:
                read_ok = False
                break
            st, val = decode_response(body)
            if st != ST_OK or val != value:
                read_ok = False
                break
            got_values += 1
        check("三条 2MB 回复完整且内容正确", read_ok and got_values == 3, f"收到 {got_values} 条")

        # 恢复之后，新请求必须还能被服务 —— 这条才是"水位没写死"的证据
        time.sleep(0.5)
        s.sendall(encode_request(OP_SET, b"after-resume", b"ok"))
        body = recv_frame(s, 20)
        ok_resume = body is not None and decode_response(body) == (ST_OK, b"")
        check("暂停读之后服务器恢复读（新请求仍被服务）", ok_resume)
        s.close()

        # ---------- [3] 内存有界 ----------
        print("[3] 内存上界")
        hwm_after = vm_hwm_mb(proc.pid)
        if hwm_after is None:
            print("  跳过（/proc 不可用）")
        else:
            print(f"  服务器 VmHWM：{hwm_before:.1f}MB → {hwm_after:.1f}MB（上限 {MEM_BOUND_MB}MB）")
            check(f"VmHWM 未超过 {MEM_BOUND_MB}MB", hwm_after < MEM_BOUND_MB)

    finally:
        stop_server(proc, hard=True)

    print()
    if fails:
        print(f"[队列水位测试] FAIL —— {len(fails)} 项失败：{fails}")
        return 1
    print("[队列水位测试] PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
