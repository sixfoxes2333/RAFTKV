#!/usr/bin/env python3
"""db_lock_test.py —— 数据文件互斥围栏：第二个实例不许动正在使用的库（修复 #1）

【要证明什么】
    Storage 构造函数在 open 之后、recover 之前必须拿到**文件独占锁**。
    否则第二个实例（哪怕最后会因为端口占用而启动失败）会先执行 recover，
    而 recover 会截断"不完整尾部" —— 直接把第一个实例身后的文件剪掉。

【实测（修复前，WSL2）】
    实例 A 写入 5 条 → 文件 113 字节
    手工在尾部追加 5 字节垃圾（模拟一次写了一半）
    启动实例 B（同一个 db）→ 文件被截回 113 字节；B 随后才因 bind 失败退出
    → A 完全不知道自己的库被人动过

【测试流程（每一步都为了一个断言）】
    1) A 写入若干条，记录文件大小 S
    2) 在文件尾注入 5 字节垃圾（构造"不完整尾部"）→ S+5
    3) 启动 B（同库）：
         · 文件必须仍是 S+5（B 因为拿不到锁而拒绝打开）
         · B 的日志必须出现 "database is locked"（我们约定的标记）
         · B 非 0 退出
       注意：此步之后 **不让 A 继续写** —— 因为垃圾还留在文件里，
              A 的下一笔追加会落在垃圾之后，形成"中间损坏"（那是另一个场景）
    4) A 仍然能读（它的内存索引不受影响）
    5) kill -9 A → 锁必须随进程释放 → 新实例 C 能启动，
       并且**合法地**把注入的垃圾尾部截掉（文件回到 S），数据完好

【判据】见上面每一步；全部通过才算"数据文件互斥"成立。

【用法】python3 tests/db_lock_test.py
"""

import os
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kvproto import (PORT, OP_SET, OP_GET, ST_OK,  # noqa: E402
                     connect, request, remove_db, wait_port)

DB = "/tmp/kv_lock_test.data"
LOG_A = "/tmp/kv_lock_a.log"
LOG_B = "/tmp/kv_lock_b.log"
LOG_C = "/tmp/kv_lock_c.log"
BINARY = os.environ.get("KV_SERVER", "./build/kvserver")

# 修复后 Storage 必须打印这个标记（区分于 bind 之类的无关失败原因）
LOCK_MARKER = "database is locked"

fails = []


def check(label, cond, detail=""):
    print(f"  {'ok   ' if cond else 'FAIL '}: {label}" + (f"   {detail}" if detail and not cond else ""))
    if not cond:
        fails.append(label)


def port_free():
    """确认 9006 没人监听（避免把别人的服务器当成自己的）"""
    try:
        s = socket.create_connection(("127.0.0.1", PORT), 0.3)
        s.close()
        return False
    except OSError:
        return True


def spawn(db_path, log_path):
    log = open(log_path, "wb")
    return subprocess.Popen([BINARY, db_path], stdout=log, stderr=subprocess.STDOUT)


def main() -> int:
    if not port_free():
        print(f"  !! 端口 {PORT} 已被占用 —— 先停掉别的 kvserver 再跑这个测试")
        return 2

    remove_db(DB)
    for p in (LOG_A, LOG_B, LOG_C):
        if os.path.exists(p):
            os.remove(p)

    # ---------- 1) 起实例 A 并写入数据 ----------
    proc_a = spawn(DB, LOG_A)
    if not wait_port(PORT, 5.0):
        print("  !! 实例 A 启动失败：")
        print(open(LOG_A, errors="replace").read()[-300:])
        proc_a.kill()
        return 2

    print(f"[数据文件锁] 实例 A 已启动（db={DB}）")
    proc_c = None

    try:
        s = connect()
        for i in range(5):
            st, _ = request(s, OP_SET, f"k{i}".encode(), f"v{i}".encode())
            if st != ST_OK:
                print(f"  !! 预置写入失败 st={st}")
                return 2
        s.close()

        size_clean = os.path.getsize(DB)
        print(f"  已写入 5 条，文件 {size_clean} 字节")

        # ---------- 2) 注入"不完整尾部" ----------
        with open(DB, "ab") as f:
            f.write(b"\xde\xad\xbe\xef\x00")
        size_dirty = os.path.getsize(DB)
        check("构造：尾部注入 5 字节垃圾", size_dirty == size_clean + 5,
              f"{size_clean} → {size_dirty}")

        # ---------- 3) 第二个实例必须被挡住，且不许改文件 ----------
        print("\n[1] 启动第二个实例（同一数据库）")
        proc_b = spawn(DB, LOG_B)

        try:
            proc_b.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc_b.kill()
            proc_b.wait(timeout=5)

        log_b = open(LOG_B, errors="replace").read()
        size_after_b = os.path.getsize(DB)
        print(f"  实例 B 退出码 {proc_b.returncode}，文件 {size_dirty} → {size_after_b}")

        check("实例 B 没有修改数据文件", size_after_b == size_dirty,
              f"{size_dirty} → {size_after_b}（说明它执行了 recover 截断）")
        check(f"实例 B 的日志包含 '{LOCK_MARKER}'",
              LOCK_MARKER in log_b,
              f"日志尾部：{log_b.strip()[-160:]}")
        check("实例 B 非 0 退出", proc_b.returncode not in (0, None), f"rc={proc_b.returncode}")

        # ---------- 4) A 仍然能读（内存索引不受影响） ----------
        print("\n[2] 实例 A 仍然正常")
        s = connect()
        st, val = request(s, OP_GET, b"k0")
        check("实例 A 读得到自己的数据", st == ST_OK and val == b"v0", f"st={st} val={val!r}")
        s.close()

        # ---------- 5) kill -9 后锁释放，新实例合法地做恢复 ----------
        print("\n[3] kill -9 之后：锁释放，恢复可以正常进行")
        proc_a.kill()
        proc_a.wait(timeout=5)
        time.sleep(0.3)

        proc_c = spawn(DB, LOG_C)
        ready_c = wait_port(PORT, 5.0)
        check("A 被 kill -9 后，新实例能正常启动", ready_c,
              f"启动超时，日志：{open(LOG_C, errors='replace').read()[-160:]}")
        check("新实例把注入的垃圾尾部截掉（回到干净大小）",
              os.path.getsize(DB) == size_clean,
              f"当前 {os.path.getsize(DB)}，期望 {size_clean}")

        if ready_c:
            s = connect()
            st, val = request(s, OP_GET, b"k4")
            check("恢复后数据完好（k4 = v4）", st == ST_OK and val == b"v4", f"st={st} val={val!r}")
            s.close()

    finally:
        for p in (proc_a, proc_c):
            if p is not None and p.poll() is None:
                p.kill()
                p.wait(timeout=5)

    print()
    if fails:
        print(f"[数据文件锁] FAIL —— {len(fails)} 项失败：{fails}")
        print("  → 预期：Storage 在 open 之后、recover 之前 flock(fd, LOCK_EX|LOCK_NB)，")
        print("          失败时打印含 'database is locked' 的错误并让对象不可用")
        return 1
    print("[数据文件锁] PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
