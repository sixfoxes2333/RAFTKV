#!/usr/bin/env python3
"""in_buffer_budget_test.py —— 收包缓冲必须有全局字节预算（#10 方案 A）

【要证明什么】
一条连接上"发了一半的帧"必须由服务端缓存起来才能拼出完整消息（in_ 缓冲）。
单条上限是 4 + MAX_MESSAGE ≈ 16MB —— 这是无法避免的（帧本身可以有 16MB）。
但**没有任何全局限制**：N 条连接同时各发半条 16MB 帧 → N × 16MB。
1000 条连接就是 16GB ⇒ 内存耗尽 / OOM / bad_alloc 崩掉（和 #3 同类，只是入口在收包侧）。

实测（本条测试的场景）：服务器跑在 RLIMIT_AS = 192MiB 下，
20 条连接各发 14MB 的"半条帧"（合计 280MB）：
  · 修复前：服务端把这些字节全缓存起来 → 撞到地址空间上限 → bad_alloc/OOM
  · 修复后：全局预算（128MB）到顶就把正在长大的连接关掉 → 服务端活着、内存有界

【判据】
  1) 服务端进程必须活着（不许被内存耗尽杀死）
  2) 服务端 VmHWM 有界（远低于 RLIMIT，说明确实在按预算拒绝）
  3) 服务端必须还能服务新连接（压力结束后正常读写）
  4) 至少有一条连接被服务端关掉（说明预算真的在起作用，而不是"能撑住"）

【修复方向】全局 in_ 字节预算（例 128MB）：读事件处理时若"总量 + 本次读取"超预算，
  记日志 + request_close 掉这条连接。（单连接上限天然是 4+MAX_MESSAGE，不用另外加。）

【用法】python3 in_buffer_budget_test.py
"""

import os
import socket
import struct
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kvproto import (PORT, OP_SET, OP_GET, ST_OK, OP_SET as _S,  # noqa: E402
                     connect, decode_response, encode_request, recv_frame,
                     remove_db, request, start_server, stop_server, wait_port)

DB = "/tmp/kv_inbudget.data"
LOG = "/tmp/kv_inbudget.log"
BINARY = os.environ.get("KV_SERVER", "build/kvserver")

AS_LIMIT_MB = 192          # 服务器地址空间上限
N_CONNS = 20               # 20 条连接
PARTIAL_MB = 14            # 每条连接发 14MB 的"半条帧"（声明 16MB，但不发完）
DECLARED = 16 * 1024 * 1024    # 帧头声明的长度（MAX_MESSAGE）
CHUNK = 64 * 1024

fails = []


def check(label, cond, detail=""):
    print(f"  {'ok   ' if cond else 'FAIL '}: {label}" + (f"   {detail}" if detail and not cond else ""))
    if not cond:
        fails.append(label)


def start_capped_server():
    log = open(LOG, "wb")
    cmd = f"ulimit -v {AS_LIMIT_MB * 1024}; exec {BINARY} {DB}"
    proc = subprocess.Popen(["bash", "-c", cmd], stdout=log, stderr=subprocess.STDOUT)
    if not wait_port(PORT, 8.0):
        text = open(LOG, errors="replace").read()
        proc.kill()
        proc.wait(timeout=5)
        raise RuntimeError(f"受限服务器没能启动（日志尾部：{text[-300:]}）")
    return proc


def vm_hwm_kb(pid):
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1])
    except OSError:
        return None
    return None


def main() -> int:
    if not os.path.exists(BINARY):
        print(f"!! 找不到 {BINARY}")
        return 2

    remove_db(DB)
    print(f"[收包缓冲预算] 服务器 RLIMIT_AS = {AS_LIMIT_MB}MiB；"
          f"{N_CONNS} 条连接各发 {PARTIAL_MB}MB 半条帧")
    try:
        proc = start_capped_server()
    except RuntimeError as e:
        print(f"!! {e}")
        return 2
    print(f"  服务器已启动（pid={proc.pid}）")

    # ---------- 每条连接：发一个"声明 16MB 的帧头" + 14MB 身体，然后不发完 ----------
    header = struct.pack(">I", DECLARED)
    body = b"x" * CHUNK
    sock_l = []
    closed_by_server = 0
    send_err = 0
    t0 = time.time()
    try:
        for i in range(N_CONNS):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(10)
            try:
                s.connect(("127.0.0.1", PORT))
                s.sendall(header)
                sent = 0
                while sent < PARTIAL_MB * 1024 * 1024:
                    s.sendall(body)
                    sent += len(body)
                sock_l.append(s)
            except OSError as e:
                send_err += 1
                print(f"  第 {i + 1} 条连接发送中断：{type(e).__name__}: {e}")
                try:
                    s.close()
                except OSError:
                    pass
                closed_by_server += 1
        dt = time.time() - t0
        print(f"  {N_CONNS} 条连接写入完成，用时 {dt:.1f}s；发送中断 {send_err} 次")
    except Exception as e:                       # noqa: BLE001
        print(f"  !! 发送阶段异常：{type(e).__name__}: {e}")

    # ---------- 等一会儿，看服务端会不会被撑死 ----------
    time.sleep(1.0)
    alive = proc.poll() is None

    # 连接是否被服务端关掉（读到 EOF / 写失败）
    for s in sock_l:
        try:
            s.settimeout(1.0)
            if s.recv(1) == b"":
                closed_by_server += 1
        except socket.timeout:
            pass                                  # 还开着
        except OSError:
            closed_by_server += 1
    for s in sock_l:
        try:
            s.close()
        except OSError:
            pass

    log_text = open(LOG, errors="replace").read()
    print(f"\n[结果] 服务端活着 = {alive}；被服务端关闭的连接数 = {closed_by_server}/{N_CONNS}")
    if not alive:
        print(f"  退出码 = {proc.returncode}；日志尾部：{log_text.strip().splitlines()[-2:]}")

    check("服务端没有被内存耗尽杀死", alive,
          f"bad_alloc/OOM 迹象：{'bad_alloc' in log_text or 'terminate' in log_text}")
    if alive:
        hwm = vm_hwm_kb(proc.pid)
        print(f"  VmHWM = {hwm}KB（{hwm / 1024:.1f}MiB），RLIMIT_AS = {AS_LIMIT_MB}MiB")
        check("VmHWM 有界（远低于限制，说明按预算拒绝）",
              hwm is not None and hwm < AS_LIMIT_MB * 1024 * 0.85, f"VmHWM={hwm}KB")
        check("至少有一条连接被服务端关掉（预算真的在起作用）", closed_by_server > 0,
              "连接全部还开着 —— 说明没有任何预算在限制收包缓冲")

        s2 = connect()
        st, val = request(s2, OP_SET, b"after", b"ok", timeout=10.0)
        st2, val2 = request(s2, OP_GET, b"after", timeout=10.0)
        check("压力结束后服务端仍能正常读写", st == ST_OK and st2 == ST_OK and val2 == b"ok",
              f"set st={st} get st={st2} val={val2!r}")
        s2.close()
    else:
        check("VmHWM 有界（远低于限制，说明按预算拒绝）", False, "进程已死，无从测量")
        check("至少有一条连接被服务端关掉（预算真的在起作用）", False, "进程已死")
        check("压力结束后服务端仍能正常读写", False, "进程已死")

    stop_server(proc, hard=True)
    print()
    if fails:
        print(f"[收包缓冲预算] FAIL —— {len(fails)} 项失败：{fails}")
        print("  → 修复方向：全局 in_ 字节预算（例 128MB）；读事件里超预算就记日志 + 关掉这条连接")
        return 1
    print("[收包缓冲预算] PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
