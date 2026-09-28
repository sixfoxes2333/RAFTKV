#!/usr/bin/env python3
"""net_epoll_add_fail_test.py —— 注册失败必须"响亮地退出"（修复 #5）

【要证明什么】
Reactor::add_connection() 是 void，内部 add_event() 失败时只打印一行就 return，
调用者（main 里注册 监听 fd 和 worker eventfd 的那两处）完全不知道。

启动阶段注册失败的后果：
  · 监听 fd 注册失败 → unique_ptr 析构 → listenfd 被关掉 → 端口没了，
    进程进入 epoll_wait **空转**：活着、但永远不服务，而且永远不会自己退出
    （监控看起来"进程在"，实际是个僵尸服务器）
  · eventfd 注册失败 → 能接连接、能收请求，但完成通知永远收不到
    → **回复永远发不出去**，客户端挂死，服务器自己不报错

【怎么注入】LD_PRELOAD 的 fail_inject 加了 KV_FAIL_EPOLL_ADD=N：
  第 N 次 epoll_ctl(EPOLL_CTL_ADD) 返回 -1/ENOMEM。
  启动阶段的 ADD 顺序：第 1 次 = 监听 fd，第 2 次 = worker 的 eventfd。

【判据（两种情况都一样）】
  1) 进程必须在几秒内自己退出（不许挂在那里当僵尸服务器）
  2) 退出码必须非 0（"我没能服务"必须能让脚本/监控看见）
  3) 日志里必须有明确的失败信息（epoll_ctl ADD ... failed）
  端口是否可连只打印，不作为判据（修好之后端口本来就不该在）。

【用法】python3 tests/net_epoll_add_fail_test.py
"""

import os
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kvproto import PORT, remove_db  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
SHIM_SRC = os.path.join(HERE, "fail_inject.c")
SHIM = "/tmp/kv_fail_inject.so"
BINARY = os.environ.get("KV_SERVER", "build/kvserver")
DB = "/tmp/kv_epolladd.data"
LOG = "/tmp/kv_epolladd.log"

EXIT_WAIT = 5.0     # 期望它在这么久内自己退出

CASES = [(1, "监听 fd"), (2, "worker eventfd")]

fails = []


def check(label, cond, detail=""):
    print(f"  {'ok   ' if cond else 'FAIL '}: {label}" + (f"   {detail}" if detail and not cond else ""))
    if not cond:
        fails.append(label)


def build_shim():
    if os.path.exists(SHIM) and os.path.getmtime(SHIM) >= os.path.getmtime(SHIM_SRC):
        return True
    r = subprocess.run(["gcc", "-shared", "-fPIC", "-O0", "-g", "-o", SHIM, SHIM_SRC, "-ldl"],
                       capture_output=True)
    if r.returncode != 0:
        print("编译 fail_inject.c 失败：", r.stderr.decode()[:400])
        return False
    return True


def port_open():
    try:
        s = socket.create_connection(("127.0.0.1", PORT), 0.3)
        s.close()
        return True
    except OSError:
        return False


def run_case(nth, what):
    print(f"\n[注入第 {nth} 次 EPOLL_CTL_ADD 失败 → {what}]")
    remove_db(DB)
    log = open(LOG, "wb")

    env = os.environ.copy()
    env["LD_PRELOAD"] = SHIM
    env["KV_FAIL_EPOLL_ADD"] = str(nth)

    proc = subprocess.Popen([BINARY, DB], stdout=log, stderr=subprocess.STDOUT,
                            env=env, cwd=REPO)

    deadline = time.time() + EXIT_WAIT
    while time.time() < deadline and proc.poll() is None:
        time.sleep(0.1)

    exited = proc.poll() is not None
    code = proc.returncode
    still_port = port_open() if not exited else port_open()

    if not exited:
        proc.kill()
        proc.wait(timeout=5)

    log.flush()
    text = open(LOG, errors="replace").read()
    print(f"  自己退出了吗 = {'是' if exited else '否（还挂在 epoll_wait 里）'}；退出码 = {code}；"
          f"端口可连 = {still_port}")
    if not exited:
        print(f"  → 日志尾部：{text.strip()[-160:]}")

    check(f"{what}：进程必须自己退出（不许活着但不服务）", exited,
          f"{EXIT_WAIT:.0f}s 内没退出，被测试杀掉")
    check(f"{what}：退出码必须非 0", exited and code not in (0, None), f"code={code}")
    check(f"{what}：日志里必须有 epoll_ctl ADD 失败信息",
          "epoll_ctl ADD" in text and "failed" in text,
          f"日志尾部：{text.strip()[-160:]}")


def main() -> int:
    if not os.path.exists(os.path.join(REPO, BINARY)):
        print(f"!! 找不到 {BINARY}")
        return 2
    if port_open():
        print(f"!! 端口 {PORT} 已被占用 —— 先停掉别的 kvserver")
        return 2
    if not build_shim():
        return 2

    for nth, what in CASES:
        run_case(nth, what)

    print()
    if fails:
        print(f"[注册失败] FAIL —— {len(fails)} 项失败：{fails}")
        print("  → 修复方向：add_connection 返回 bool（注册失败 = false），")
        print("           main 里注册 监听 fd / eventfd 两处必须检查返回值并打印错误 + 非 0 退出；")
        print("           运行期 accept 后注册失败则只放弃这条连接、服务器继续")
        return 1
    print("[注册失败] PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
