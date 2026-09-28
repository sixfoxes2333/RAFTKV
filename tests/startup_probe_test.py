#!/usr/bin/env python3
"""startup_probe_test.py —— 测试脚手架自己的围栏（修复 #7）

【要证明什么】
kvproto.start_server() 原来只做一件事：探端口（能 connect 上就算"启动成功"）。
于是这些情况都会被判成"服务器起来了"：
  · 用 /bin/false 之类的假二进制启动（进程秒退），而端口上碰巧有【别人的监听】
  · 端口被别人的服务占着 → 我们的服务器 bind 失败退出 → 但端口仍然可连
  · 端口后面是个"活着但不会说我们协议"的东西
后果：**所有网络测试都会在一个根本不是我们的服务器上跑**，结论全部作废（假绿最恶劣）。

【判据（修复后 start_server 必须）】
  1) 假二进制（秒退）+ 端口被占 → 抛 RuntimeError
  2) 真二进制 + 端口被占（bind 失败自己退出）→ 抛 RuntimeError
  3) 进程活着但端口后面不是我们的协议 → 抛 RuntimeError（协议握手）
  4) 正常启动照旧成功（不能把好路径搞坏）

【实现注意】四条用例都在同一端口（服务器的端口是硬编码的）上，
所以 fake listener 只能建一次、复用 —— 反复 close/bind 会撞 TIME_WAIT。

【用法】python3 tests/startup_probe_test.py
"""

import os
import socket
import struct
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kvproto import PORT, remove_db, start_server, stop_server  # noqa: E402

SERVER = os.environ.get("KV_SERVER", "build/kvserver")
DB = "/tmp/kv_probe.data"
LOG = "/tmp/kv_probe.log"
FAKE_BIN = "/tmp/kv_probe_fake_binary.sh"     # 一个"活着但不是 KV 服务器"的假二进制

fails = []


def check(label, cond, detail=""):
    print(f"  {'ok   ' if cond else 'FAIL '}: {label}" + (f"   {detail}" if detail and not cond else ""))
    if not cond:
        fails.append(label)


class FakeListener:
    """占着端口；reply=None 时什么都不回，reply=字节 时先回这段再挂着"""

    def __init__(self):
        self.srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(("127.0.0.1", PORT))
        self.srv.listen(16)
        self.reply = None
        self.stop_flag = False
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self):
        while not self.stop_flag:
            try:
                conn, _ = self.srv.accept()
            except OSError:
                return
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn):
        try:
            conn.recv(1 << 16)
            if self.reply:
                conn.sendall(self.reply)
                time.sleep(0.2)
                conn.recv(1 << 16)
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def close(self):
        self.stop_flag = True
        try:
            self.srv.close()
        except OSError:
            pass


def expect_raise(binary):
    """调用 start_server，期望抛 RuntimeError"""
    try:
        proc = start_server(DB, LOG, binary=binary)
    except RuntimeError as e:
        return True, str(e)[:100]
    else:
        stop_server(proc, hard=True)
        return False, "start_server 没抛异常（把假启动当成成功了）"


def main() -> int:
    if not os.path.exists(SERVER):
        print(f"!! 找不到 {SERVER}")
        return 2
    if os.path.exists(FAKE_BIN):
        os.remove(FAKE_BIN)

    remove_db(DB)
    proc = None
    fake = None

    # ---------- 0) 先跑正常启动：这时端口是干净的（fake listener 还没建） ----------
    print("\n[0] 正常启动（端口干净）—— 先跑这条，避免 fake listener 的 TIME_WAIT 干扰")
    try:
        proc = start_server(DB, LOG)
        check("正常 start_server 成功返回", proc is not None and proc.poll() is None)
    except RuntimeError as e:
        check("正常 start_server 成功返回", False, f"反而抛了异常：{e}")
    finally:
        stop_server(proc, hard=True)
        proc = None
        time.sleep(0.3)

    try:
        # 建 fake listener：占住 9006（SO_REUSEADDR 让我们能在刚关掉服务器的端口上重绑）
        last = None
        for _ in range(20):
            try:
                fake = FakeListener()
                break
            except OSError as e:
                last = e
                time.sleep(0.2)
        if fake is None:
            print(f"!! fake listener 绑不上 {PORT}：{last}")
            return 2

        # ---------- 1) 假二进制（秒退）+ 端口被占 ----------
        print("\n[1] 端口被别人的监听占着 + 用 /bin/false 当二进制（进程秒退）")
        raised, why = expect_raise("/bin/false")
        check("必须抛异常（进程秒退不能算启动成功）", raised, why)

        # ---------- 2) 真二进制 + 端口被占 ----------
        print("\n[2] 端口被别人的监听占着 + 用真二进制（bind 失败，它会自己退出）")
        raised, why = expect_raise(SERVER)
        check("必须抛异常（端口能连 ≠ 我们的服务器在服务）", raised, why)

        # ---------- 3) 进程活着，但端口后面不是我们的协议 ----------
        print("\n[3] 进程活着（假二进制 sleep）+ 端口后面回垃圾 → 协议握手必须失败")
        with open(FAKE_BIN, "w") as f:
            f.write("#!/bin/sh\n# 活着、但不提供任何服务（模拟\"端口是别人的，我们的进程还活着\"）\nsleep 30\n")
        os.chmod(FAKE_BIN, 0o755)
        fake.reply = struct.pack(">I", 4) + b"\xff\xff\xff\xff"     # 帧头说 4 字节，内容是垃圾
        raised, why = expect_raise(FAKE_BIN)
        check("必须抛异常（协议握手失败）", raised, why)
    finally:
        stop_server(proc, hard=True)
        if fake is not None:
            fake.close()
        if os.path.exists(FAKE_BIN):
            os.remove(FAKE_BIN)

    print()
    if fails:
        print(f"[启动探测] FAIL —— {len(fails)} 项失败：{fails}")
        print("  → 修复方向：start_server 在 wait_port 之后还要")
        print("           ① 确认我们启动的进程还活着 ② 用一条只读请求握手（协议通才算起来）")
        return 1
    print("[启动探测] PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
