#!/usr/bin/env python3
"""queue_bytes_test.py —— 后台队列的内存上界必须按【字节】限流（修复 #3）

【要证明什么】
现在 worker 队列的限流是【条数】：`pending_.size() + completed_.size() >= max_pending_(1024)`
→ 一条 4MiB 的 SET 也只算 1 条 → 1024 × 4MiB ≈ 4GiB 的请求体可以合法地堆在队列里
→ 内存爆炸。而且现场是 `std::bad_alloc` **未捕获** → std::terminate → 进程直接死。

【实测（修复前）】把服务器限制在 192MiB 地址空间（RLIMIT_AS），
  客户端流水线发 4MiB 的 SET、不读回复 → 服务器被 std::bad_alloc 杀掉。

【判据】
  1) 服务器进程必须活着（不许被 OOM/未捕获异常杀死）
  2) 客户端必须收到明确拒绝（Error）或看到连接被关闭 —— 不能挂住
  3) 服务器必须还能服务新连接（没有被永久打死）
  4) 服务器峰值内存 VmHWM 有界（远远低于 RLIMIT，说明确实在按字节拒绝）
  5) 压力解除后恢复：新连接能正常读写

【修复方向】队列的字节预算（含在途任务）：
  全局上限（几十 MiB 级）+ 单连接上限（要 > 单条最大消息，否则最大消息永远发不进来）
  超限 → 明确拒绝（Error），不能静默丢弃，也不能让进程死掉

【用法】python3 tests/queue_bytes_test.py
"""

import os
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kvproto import (PORT, OP_SET, OP_GET, ST_OK, ST_NOTFOUND, ST_ERROR,  # noqa: E402
                     connect, decode_response, encode_request, recv_frame,
                     remove_db, request, stop_server, wait_port)

DB = "/tmp/kv_qbytes.data"
LOG = "/tmp/kv_qbytes.log"
BINARY = os.environ.get("KV_SERVER", "build/kvserver")

AS_LIMIT_MB = 192          # 服务器地址空间上限（模拟"内存不宽裕的机器"）
VALUE_MB = 4               # 每个请求的 value 大小
N_REQUESTS = 40            # 40 × 4MiB = 160MiB（远超 192MiB 里的可用空间）

STATUS_NAME = {ST_OK: "Ok", ST_NOTFOUND: "NotFound", ST_ERROR: "Error"}
fails = []


def check(label, cond, detail=""):
    print(f"  {'ok   ' if cond else 'FAIL '}: {label}" + (f"   {detail}" if detail and not cond else ""))
    if not cond:
        fails.append(label)


def start_capped_server():
    """在 RLIMIT_AS 限制下启动服务器（模拟"内存本来就不宽裕的机器"）"""
    log = open(LOG, "wb")
    kb = AS_LIMIT_MB * 1024
    cmd = f"ulimit -v {kb}; exec {BINARY} {DB}"
    proc = subprocess.Popen(["bash", "-c", cmd], stdout=log, stderr=subprocess.STDOUT)
    if not wait_port(PORT, 8.0):
        text = open(LOG, errors="replace").read()
        proc.kill()
        proc.wait(timeout=5)
        raise RuntimeError(f"受限服务器没能启动（日志尾部：{text[-300:]})")
    return proc


def vm_hwm_kb(pid):
    """读 /proc/<pid>/status 的 VmHWM（峰值常驻内存，KB）"""
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
    print(f"[队列字节预算] 服务器地址空间限制 = {AS_LIMIT_MB}MiB")
    try:
        proc = start_capped_server()
    except RuntimeError as e:
        print(f"!! {e}")
        return 2

    print(f"  服务器已启动（pid={proc.pid}，VmHWM={vm_hwm_kb(proc.pid)}KB）")
    payload = encode_request(OP_SET, b"big", b"X" * (VALUE_MB * 1024 * 1024))
    total_mb = N_REQUESTS * VALUE_MB

    print(f"\n[1] 流水线灌 {N_REQUESTS} 笔 {VALUE_MB}MiB 的 SET（共 {total_mb}MiB），期间不读回复")
    s = connect()
    send_broke = None
    try:
        s.sendall(payload * N_REQUESTS)
    except OSError as e:
        send_broke = f"{type(e).__name__}: {e}"
    print(f"  发送结束" + (f"（中断：{send_broke}）" if send_broke else ""))

    # ---- 收回复：期望"若干 Ok → 一个 Error → EOF"；也可能是直接 EOF ----
    seq = []            # 前几个回复的名字（只用于打印）
    n_ok = n_err = n_other = 0
    tail = None
    while True:
        try:
            body = recv_frame(s, 3.0)
        except (socket.timeout, OSError) as e:
            tail = "TIMEOUT" if isinstance(e, socket.timeout) else "CLOSED_ERR"
            break
        if body is None:
            tail = "EOF"
            break
        st, _ = decode_response(body)
        if st == ST_OK:
            n_ok += 1
        elif st == ST_ERROR:
            n_err += 1
        else:
            n_other += 1
        if len(seq) < 6:
            seq.append(STATUS_NAME.get(st, f"?{st}"))
    print(f"  收到 Ok={n_ok}、Error={n_err}、其他={n_other}；前几个={seq}；结尾={tail}")
    s.close()

    # ---- 判据 1：服务器还活着吗 ----
    time.sleep(0.5)
    alive = proc.poll() is None
    log_text = open(LOG, errors="replace").read()
    print(f"\n[2] 服务器进程：{'活着' if alive else f'已退出（码 {proc.returncode}）'}")
    if not alive:
        tail = log_text.strip().splitlines()[-3:]
        print("  日志尾部：" + " | ".join(tail))
    check("服务器进程没有被内存耗尽杀死", alive,
          f"日志中出现 bad_alloc/OOM 迹象：{'bad_alloc' in log_text or 'terminate' in log_text}")

    # ---- 判据 2：客户端拿到明确答复（不许挂住） ----
    check("客户端拿到明确答复（Error 或连接关闭），没有挂住",
          tail != "TIMEOUT", f"结尾={tail}")

    # ---- 判据 3/4：还能服务 + 内存有界 ----
    if alive:
        hwm = vm_hwm_kb(proc.pid)
        print(f"  VmHWM = {hwm}KB（{hwm / 1024:.1f}MiB），RLIMIT_AS = {AS_LIMIT_MB}MiB")
        check("峰值内存有界（< 限制的 85%，说明确实在按字节拒绝）",
              hwm is not None and hwm < AS_LIMIT_MB * 1024 * 0.85,
              f"VmHWM={hwm}KB")

        # 新连接必须能用（压力还没解除时也会被拒绝，但要有明确答复）
        s2 = connect()
        st1, _ = request(s2, OP_GET, b"nonexistent", timeout=5.0)
        check("服务器仍能接受新连接并给出答复", st1 is not None,
              "新连接没有任何答复")

        # 压力解除：等 worker 把队列排空（每笔 4MiB 写盘），然后必须恢复正常
        deadline = time.time() + 60
        recovered = False
        while time.time() < deadline:
            st, _ = request(s2, OP_GET, b"nonexistent", timeout=5.0)
            if st in (ST_OK, ST_NOTFOUND):
                recovered = True
                break
            time.sleep(1.0)
        check("队列排空后恢复正常服务", recovered, "60s 内一直拒绝")
        s2.close()
    else:
        check("峰值内存有界（< 限制的 85%，说明确实在按字节拒绝）", False, "进程已死，无从测量")
        check("服务器仍能接受新连接并给出答复", False, "进程已死")
        check("队列排空后恢复正常服务", False, "进程已死")

    stop_server(proc, hard=True)
    print()
    if fails:
        print(f"[队列字节预算] FAIL —— {len(fails)} 项失败：{fails}")
        print("  → 修复方向：队列按【字节】限流（含在途任务），全局上限 + 单连接上限；")
        print("           超限要明确拒绝（Error），进程绝不能因 bad_alloc 死掉")
        return 1
    print("[队列字节预算] PASS")
    return 0




if __name__ == "__main__":
    sys.exit(main())
