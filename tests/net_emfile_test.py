#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
net_emfile_test.py —— fd 用尽（EMFILE）时不许空转的回归测试（Step 5）

【这个测试在验证什么】
    把服务器的 fd 上限压低（RLIMIT_NOFILE = 32），然后从客户端猛开连接，
    逼服务器 accept 时拿到 EMFILE（打开文件数已达上限）。此时：
      · accept 队列里还排着连接 → listenfd 在【水平触发】下会一直报"可读"
      · 如果 handle_accept 遇到 EMFILE 就单纯 return（什么都不做），
        下一轮 epoll_wait 立刻又返回 → 空转 → 一个核 100% CPU，而且永远接不进新连接
    正确做法（muduo / nginx 同款）：预留一个占位 fd，EMFILE 时
      close(占位 fd) → accept 取出队首那个连接 → close 掉它 → 重新打开占位 fd
    这样队列被排空，空转停止；已有连接也应当继续工作。

【为什么它是真・红→绿（不是围栏）】
    当前代码在 EMFILE 分支里直接 return，什么都不做 → 必然空转。
    所以本用例在 Step 5 之前应该是 FAIL（CPU 逼近 100%），修好之后 PASS。

【判据】
    1) EMFILE 事件发生之后的观察窗口内，服务器 CPU 时间 < 0.5s（不是 2.0s）
    2) 已经建立成功的连接仍然可用：发一条消息能收到完整回复
    3) 服务器进程还活着

【用法】
    python3 tests/net_emfile_test.py
    环境变量：KV_SERVER / KV_PORT
"""
import os
import resource
import socket
import struct
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kvproto import (OP_SET, OP_GET, ST_OK,  # noqa: E402
                     encode_request, decode_response, recv_frame)

SERVER = os.environ.get("KV_SERVER", "build/kvserver")
PORT = int(os.environ.get("KV_PORT", "9006"))
SERVER_LOG = "/tmp/kv_emfile_server.log"

FD_LIMIT = 32                 # 服务器进程能打开的文件数上限
N_ATTEMPTS = 45               # 客户端尝试建立的连接数（远超上限）
CONNECT_TIMEOUT = 0.3         # 单个 connect 的超时（队列满了就会连接不上）
CPU_WINDOW = 2.0              # EMFILE 之后的观察窗口（秒）
CPU_BUDGET = 0.5              # 观察窗口内允许的 CPU 时间上限（秒）
CLK_TCK = os.sysconf("SC_CLK_TCK")


def frame(payload: bytes) -> bytes:
    """按协议打包一条消息：[4 字节大端长度][内容]"""
    return struct.pack(">I", len(payload)) + payload


def _limit_fds():
    """在子进程里把 fd 上限压到 32（只有服务器进程受影响）"""
    resource.setrlimit(resource.RLIMIT_NOFILE, (FD_LIMIT, FD_LIMIT))


def start_server():
    log = open(SERVER_LOG, "wb")
    proc = subprocess.Popen([SERVER], stdout=log, stderr=subprocess.STDOUT,
                            cwd=os.getcwd(), preexec_fn=_limit_fds)
    deadline = time.time() + 5.0
    while time.time() < deadline:
        if proc.poll() is not None:
            break
        try:
            s = socket.create_connection(("127.0.0.1", PORT), timeout=0.2)
            s.close()
            return proc
        except OSError:
            time.sleep(0.05)

    log.flush()
    print("!! 服务器启动失败，日志如下：")
    try:
        print(open(SERVER_LOG, encoding="utf-8", errors="replace").read())
    except OSError:
        pass
    print("!! 提示：端口可能被占用，先确认没有别的 kvserver 在跑")
    sys.exit(2)


def stop_server(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=3)


def cpu_seconds(pid: int):
    """读 /proc/<pid>/stat 的 utime+stime（累计 CPU 秒数）"""
    try:
        with open(f"/proc/{pid}/stat", "r") as f:
            data = f.read()
    except OSError:
        return None
    rest = data[data.rindex(")") + 2:].split()
    return (int(rest[11]) + int(rest[12])) / CLK_TCK


def kv_once(sock: socket.socket, key: bytes, value: bytes, timeout: float) -> bool:
    """在一条已建立的连接上做一次 SET + GET，验证服务器仍能正常读写"""
    try:
        sock.settimeout(timeout)
        sock.sendall(encode_request(OP_SET, key, value))
        body = recv_frame(sock, timeout)
        if body is None or decode_response(body) != (ST_OK, b""):
            return False
        sock.sendall(encode_request(OP_GET, key))
        body = recv_frame(sock, timeout)
        if body is None:
            return False
        return decode_response(body) == (ST_OK, value)
    except (OSError, ValueError):
        return False


def main() -> int:
    print(f"[EMFILE 测试] 启动服务器 {SERVER}（RLIMIT_NOFILE={FD_LIMIT}）")
    proc = start_server()
    socks = []
    try:
        # ---- 1. 猛开连接，把服务器的 fd 用光（并让 accept 队列里留有待处理连接）----
        established = 0
        for i in range(N_ATTEMPTS):
            s = socket.socket()
            s.settimeout(CONNECT_TIMEOUT)
            try:
                s.connect(("127.0.0.1", PORT))
                socks.append(s)           # 连接成功（可能稍后被服务器在 EMFILE 处理中丢弃）
                established += 1
            except OSError:
                s.close()                 # 队列满 → SYN 被丢 → 连接超时，正常现象
        print(f"  已尝试 {N_ATTEMPTS} 个连接，其中 connect 成功 {established} 个"
              f"（服务器 fd 上限 {FD_LIMIT}，其余排队或超时）")

        # ---- 2. 观察窗口：EMFILE 已经发生，服务器不许空转 ----
        c0 = cpu_seconds(proc.pid)
        time.sleep(CPU_WINDOW)
        c1 = cpu_seconds(proc.pid)
        alive = proc.poll() is None
        cpu = None if (c0 is None or c1 is None) else (c1 - c0)
        if cpu is None:
            print("  观察窗口 CPU 时间 = 读取失败（/proc 不可用？）")
        else:
            print(f"  EMFILE 后 {CPU_WINDOW}s 内服务器 CPU 时间 = {cpu:.2f}s"
                  f"（上限 {CPU_BUDGET}s）")
        print(f"  服务器进程存活 = {alive}")

        # ---- 3. 已有连接还得能用（服务没被 fd 耗尽搞死）----
        served = 0
        for i, s in enumerate(socks):
            if kv_once(s, b"emfile-" + str(i).encode(), b"v", 3.0):
                served += 1
        print(f"  {established} 个已建立连接中，仍能正常读写的连接有 {served} 个")

        ok_cpu = (cpu is not None) and (cpu < CPU_BUDGET)
        ok_serving = (served > 0)

        if ok_cpu and ok_serving and alive:
            print("[EMFILE 测试] PASS")
            return 0

        print("[EMFILE 测试] FAIL")
        if not ok_cpu:
            print("  原因：EMFILE 之后服务器在空转（水平触发下 listenfd 一直可读，"
                  "而 accept 又一直失败）—— 需要在 EMFILE 分支里把队首连接取出来丢掉")
        if not ok_serving:
            print("  原因：没有任何已建立连接能收到回显（服务已不可用）")
        if not alive:
            print("  原因：服务器进程退出了")
        print(f"  服务器日志：{SERVER_LOG}")
        return 1
    finally:
        for s in socks:
            try:
                s.close()
            except OSError:
                pass
        stop_server(proc)


if __name__ == "__main__":
    sys.exit(main())
