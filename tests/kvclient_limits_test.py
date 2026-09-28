#!/usr/bin/env python3
"""kvclient_limits_test.py —— 客户端要早点说清"字段超限"（#6 之后的可选加固）

【为什么做这个（以及为什么不做帧长检查）】
实测：命令行单个参数上限 = MAX_ARG_STRLEN = PAGE_SIZE×32 = **128KB**
      · 100000 字节的参数 → 能执行
      · 140000 字节的参数 → /bin/bash: Argument list too long（execve E2BIG）
而帧上限 MAX_MESSAGE = 16MB ⇒ CLI 能构造的最大帧只有约 262KB
⇒ "客户端发送方向查帧长上限"是**死代码**，不该加。

但同一方向上有一条**真够得到**的：MAX_KEY = 64KB < 128KB。
   实测（加固前）：100KB 的 key → 服务器按协议校验拒绝 → 客户端只打印笼统的 "ERROR"
   退出码是对的（2），但用户看不出到底是 key 太长还是别的问题。

【判据】
  1) 100KB key（> MAX_KEY）：**连服务器都不用开**，客户端就该本地拒绝：
     rc=2 且 stderr 能看出是"字段超限"（而不是 "connect: Connection refused"）
     —— 用"不起服务器"来证明它是【本地校验、早失败】，而不是靠服务器回 ERROR
  2) 60KB key（合法，但库里没有）：rc=1 NOT_FOUND —— 合法路径不能被搞坏
  3) 正常 set/get 往返：rc=0，取回的值正确

【用法】python3 tests/kvclient_limits_test.py
"""

import os
import socket
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kvproto import PORT, remove_db, start_server, stop_server  # noqa: E402

BIN = os.environ.get("KV_CLIENT", "./build/kvclient")
SERVER = os.environ.get("KV_SERVER", "build/kvserver")
DB = "/tmp/kv_limits.data"
LOG = "/tmp/kv_limits.log"

MAX_KEY = 64 * 1024     # 与 src/base/limits.h 的 kv::MAX_KEY 对应

fails = []


def check(label, cond, detail=""):
    print(f"  {'ok   ' if cond else 'FAIL '}: {label}" + (f"   {detail}" if detail and not cond else ""))
    if not cond:
        fails.append(label)


def run_client(*argv, port=PORT):
    env = os.environ.copy()
    env["KV_HOST"] = "127.0.0.1"
    env["KV_PORT"] = str(port)
    r = subprocess.run([BIN, *argv], capture_output=True, env=env, timeout=30)
    return r.returncode, r.stdout.decode(errors="replace"), r.stderr.decode(errors="replace")


def port_open():
    try:
        s = socket.create_connection(("127.0.0.1", PORT), 0.3)
        s.close()
        return True
    except OSError:
        return False


def main() -> int:
    if not os.path.exists(BIN):
        print(f"!! 找不到 {BIN}")
        return 2
    if port_open():
        print(f"!! 端口 {PORT} 被占用 —— 先停掉别的 kvserver（本测试要求先不开服务器）")
        return 2

    proc = None

    # ---------- 1) 超长 key：本地就该拒绝（不需要服务器） ----------
    print("[1] 100KB 的 key（MAX_KEY = 64KB），故意【不起服务器】")
    big_key = "k" * 100000
    rc, out, err = run_client("get", big_key)
    print(f"  rc={rc} stdout={out[:40]!r} stderr={err.strip()[:80]!r}")
    check("超长 key：退出码 = 2", rc == 2, f"rc={rc}")
    check("超长 key：客户端本地就报出字段超限（不是等着服务器回 ERROR、也不是连接错误）",
          ("too large" in err or "超" in err) and "Connection refused" not in err,
          f"stderr={err.strip()[:100]!r}")

    # ---------- 2) 合法 key：正常往返 ----------
    print("\n[2] 起服务器，合法 key 走正常路径")
    remove_db(DB)
    proc = start_server(DB, LOG)
    try:
        rc, out, err = run_client("set", "hello", "world")
        check("set 合法 key/value：rc=0", rc == 0 and "OK" in err, f"rc={rc} stderr={err.strip()[:60]!r}")

        rc, out, err = run_client("get", "hello")
        check("get 取回值正确：rc=0 且 stdout=world", rc == 0 and out == "world",
              f"rc={rc} stdout={out!r}")

        long_key = "j" * 60000          # 60KB < 64KB，合法
        rc, out, err = run_client("get", long_key)
        check("60KB 合法 key：rc=1 NOT_FOUND（合法路径没被加固搞坏）",
              rc == 1 and "NOT_FOUND" in err, f"rc={rc} stderr={err.strip()[:60]!r}")
    finally:
        stop_server(proc, hard=True)

    print()
    if fails:
        print(f"[客户端字段上限] FAIL —— {len(fails)} 项失败：{fails}")
        print("  → 加固方向：在 connect 之前校验 req.key/req.value 上限，")
        print("           命中就打印 key/value too large 并 return 2（本地早失败）")
        return 1
    print("[客户端字段上限] PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
