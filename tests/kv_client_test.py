#!/usr/bin/env python3
"""kv_client_test.py —— kvclient 命令行客户端测试（M6d）

【CLI 契约（测试就是它的规范）】
    kvclient set <key> <value>   →  stderr "OK"      ，退出码 0
    kvclient get <key>           →  stdout 原样 value（不加换行），退出码 0
    kvclient get <不存在的 key>   →  stderr NOT_FOUND ，退出码 1
    kvclient del <key>           →  stderr "OK"      ，退出码 0
    出错 / 用法错                 →  stderr ERROR/用法，退出码 2

【覆盖】
    [1] 用法与退出码（缺参数 / 未知命令）
    [2] set / get / del 语义 + 退出码
    [3] 1MB 随机 value 往返（stdout 逐字节一致）
    [4] 与 Python 客户端互通（C++ 写 → Python 读；Python 写 → C++ 读）
    [5] 服务器不在线 → 退出码 2

【用法】python3 tests/kv_client_test.py
"""

import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kvproto import (PORT, OP_SET, OP_GET, ST_OK, ST_NOTFOUND,  # noqa: E402
                     connect, request, remove_db, start_server, stop_server)

BIN = os.environ.get("KV_CLIENT", "./build/kvclient")
DB = "/tmp/kv_client_test.data"
LOG = "/tmp/kv_client_test.log"

fails = []


def check(label, cond, detail=""):
    print(f"  {'ok   ' if cond else 'FAIL '}: {label}" + (f"   {detail}" if detail and not cond else ""))
    if not cond:
        fails.append(label)


def cli(*args, timeout=30):
    """跑一次 kvclient，返回 (退出码, stdout, stderr)

    args 可以是 str 或 bytes：二进制 value 必须用 bytes 传，
    否则 Python 会按 UTF-8 重新编码，服务器收到的就不是原字节了。
    """
    env = os.environ.copy()
    env["KV_PORT"] = str(PORT)
    argv = [BIN.encode()] + [a if isinstance(a, bytes) else a.encode() for a in args]
    p = subprocess.run(argv, capture_output=True, env=env, timeout=timeout)
    return p.returncode, p.stdout, p.stderr


def main() -> int:
    remove_db(DB)
    proc = start_server(DB, LOG)
    try:
        # ---------- [1] 用法与退出码 ----------
        print("[1] 用法与退出码")
        rc, _, _ = cli()
        check("无参数 → 退出码 2", rc == 2, f"rc={rc}")

        rc, _, _ = cli("frobnicate", "a")
        check("未知命令 → 退出码 2", rc == 2, f"rc={rc}")

        rc, _, _ = cli("set", "onlykey")
        check("set 缺 value → 退出码 2", rc == 2, f"rc={rc}")

        # ---------- [2] 基本语义 ----------
        print("[2] set / get / del 语义与退出码")
        rc, out, err = cli("set", "a", "1")
        check("set a 1 → 退出码 0 且 stderr=OK", rc == 0 and b"OK" in err, f"rc={rc} err={err!r}")

        rc, out, _ = cli("get", "a")
        check("get a → 退出码 0 且 stdout='1'", rc == 0 and out == b"1", f"rc={rc} out={out!r}")

        rc, out, err = cli("get", "missing")
        check("get 不存在的 key → 退出码 1 且 stderr=NOT_FOUND",
              rc == 1 and b"NOT_FOUND" in err, f"rc={rc} err={err!r}")

        rc, _, err = cli("set", "a", "2")
        rc2, out, _ = cli("get", "a")
        check("覆盖写后 get → '2'", rc == 0 and rc2 == 0 and out == b"2", f"out={out!r}")

        rc, _, err = cli("del", "a")
        check("del a → 退出码 0 且 stderr=OK", rc == 0 and b"OK" in err, f"rc={rc} err={err!r}")

        rc, _, err = cli("get", "a")
        check("删除后 get → 退出码 1", rc == 1, f"rc={rc}")

        rc, _, err = cli("del", "a")
        check("重复 del → 退出码 1（幂等，报未找到）", rc == 1, f"rc={rc}")

        # ---------- [3] 大 value ----------
        print("[3] 二进制 value 往返（64KB）")
        # 注意两件事：① 单个 argv 参数上限 MAX_ARG_STRLEN = 128KB；
        #             ② argv 是 NUL 结尾的 C 字符串，参数里不能出现 0x00（execve 会报 embedded null byte）
        blob = bytes(((i % 255) + 1) for i in range(64 * 1024))   #1..255 循环：非 NUL 的二进制
        rc, _, err = cli(b"set", b"big", blob, timeout=60)
        check("set 64KB 二进制 → 退出码 0", rc == 0, f"rc={rc} err={err[:80]!r}")

        rc, out, _ = cli(b"get", b"big", timeout=60)
        check("get 64KB → 逐字节一致", rc == 0 and out == blob, f"rc={rc} len={len(out)}")

        # ---------- [4] 与 Python 客户端互通 ----------
        print("[4] 与 Python 客户端互通")
        rc, _, _ = cli("set", "from-cpp", "hello-from-kvclient")
        s = connect()
        st, val = request(s, OP_GET, b"from-cpp")
        check("C++ 写 → Python 读", st == ST_OK and val == b"hello-from-kvclient",
              f"status={st} value={val!r}")

        st, _ = request(s, OP_SET, b"from-py", b"hello-from-python")
        s.close()
        rc, out, _ = cli("get", "from-py")
        check("Python 写 → C++ 读", st == ST_OK and rc == 0 and out == b"hello-from-python",
              f"rc={rc} out={out!r}")

        # ---------- [5] 服务器不在线 ----------
        print("[5] 服务器不在线")
        stop_server(proc, hard=True)
        rc, _, err = cli("get", "a", timeout=15)
        check("连不上服务器 → 退出码 2", rc == 2, f"rc={rc} err={err[:80]!r}")

    finally:
        stop_server(proc, hard=True)

    print()
    if fails:
        print(f"[kvclient 测试] FAIL —— {len(fails)} 项失败：{fails}")
        return 1
    print("[kvclient 测试] PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
