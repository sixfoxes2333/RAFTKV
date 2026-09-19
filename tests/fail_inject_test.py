#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fail_inject_test.py —— 用 LD_PRELOAD 验证存储层的"失败语义"（M5）

【为什么要这一层测试】
   crash_inject_test.py 能验证"进程被杀/文件被破坏"之后还能恢复，
   但没法验证【写路径失败】之后的行为 —— 那需要让 write/ftruncate/fsync 真的失败。
   这里用 tests/fail_inject.c 编译出的 .so 通过 LD_PRELOAD 注入，不改一行生产代码。

【三个语义（就是审查 #4 要求的东西）】

  A. fsync 失败
     记录已经写进页缓存（write 成功），但持久性未知
     → 对象必须进入 poisoned：后续 put 全部失败、good() == false
     → 但磁盘上确实多了一条（write 成功了）—— 这就是"进程内与磁盘两个真相"，
        靠 poisoned 把它挡住：不允许在不确定状态下继续接受写入

  B. write 失败 + 回滚成功
     → 这属于【普通失败】：文件已回到写入前，对象仍然健康
     → put 返回 false，但后续 put 仍然成功、good() == true
     → 磁盘上不能留下残骸（文件大小 = 8 + 成功条数 × 记录长度）

  C. write 失败 + 回滚也失败
     → 文件里留了半条残骸，提交结果不确定 → poisoned
     → 后续 put 全部失败、good() == false

【用法】
   python3 tests/fail_inject_test.py
   （会自己把 tests/fail_inject.c 编译到 /tmp/kv_fail_inject.so）
"""
import os
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WRITER = os.path.join(REPO, "build", "crash_writer")
SHIM_SRC = os.path.join(REPO, "tests", "fail_inject.c")
SHIM = "/tmp/kv_fail_inject.so"
PATH = "/tmp/kv_fail_inject.log"
FILE_HEADER = 8

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'ok  ' if ok else 'FAIL'}] {name:<28} {detail}")


def clean():
    try:
        os.remove(PATH)
    except FileNotFoundError:
        pass


def run_poison(count, inject=None, timeout=60):
    """跑 crash_writer poison；inject 是环境变量字典（含 LD_PRELOAD）"""
    env = os.environ.copy()
    if inject:
        env.update(inject)

    r = subprocess.run([WRITER, "poison", PATH, str(count)],
                       capture_output=True, text=True, env=env, timeout=timeout)
    return r


def parse(out):
    """解析 PUT i r / GOOD g / SIZE s"""
    puts = []
    good = None
    size = None

    for line in out.splitlines():
        parts = line.split()
        if len(parts) == 3 and parts[0] == "PUT":
            puts.append((int(parts[1]), int(parts[2])))
        elif len(parts) == 2 and parts[0] == "GOOD":
            good = int(parts[1])
        elif len(parts) == 2 and parts[0] == "SIZE":
            size = int(parts[1])

    return puts, good, size


def rec_size():
    """先写一条，量出单条记录长度（键值都是 7 字节：k%06d / v%06d）"""
    clean()
    subprocess.run([WRITER, "write", PATH, "1"], capture_output=True, text=True)
    size = int(subprocess.run([WRITER, "size", PATH], capture_output=True, text=True).stdout)
    clean()
    return size - FILE_HEADER


def build_shim():
    r = subprocess.run(["gcc", "-shared", "-fPIC", "-O0", "-g", "-o", SHIM, SHIM_SRC, "-ldl"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        print("!! 编译注入库失败：")
        print(r.stderr[-800:])
        return False
    return True


def main() -> int:
    if not os.path.exists(WRITER):
        print(f"!! 找不到 {WRITER}，先构建：cmake --build build -j")
        return 2

    if not build_shim():
        return 2

    rec = rec_size()
    print(f"[故障注入] writer = {WRITER}")
    print(f"[故障注入] shim   = {SHIM}（单条记录 {rec}B，文件头 {FILE_HEADER}B）")

    preload = {"LD_PRELOAD": SHIM}

    # ---- 0) 先确认注入真的生效：让第 1 次 fsync 失败（写文件头那次） ----
    print("[0] 注入库自检：第 1 次 fsync 失败 → 新库的构造应当失败")
    clean()
    env = dict(preload)
    env["KV_FAIL_FSYNC_CALL"] = "1"
    r = subprocess.run([WRITER, "write", PATH, "1"], capture_output=True, text=True, env=env)
    record("shim-sanity", r.returncode == 2,
           f"write 退出码 {r.returncode}（期望 2 = storage not good）")

    # ---- A) fsync 失败 → poisoned ----
    # 调用序列（新文件）：ctor 写文件头 fsync=1，put k000000 的 fsync=2，put k000001 的 fsync=3
    print("[A] fsync 失败（第 3 次）→ 对象必须 poisoned")
    clean()
    env = dict(preload)
    env["KV_FAIL_FSYNC_CALL"] = "3"
    r = run_poison(3, env)
    puts, good, size = parse(r.stdout)

    expect_a = (puts == [(0, 1), (1, 0), (2, 0)]) and (good == 0)
    record("A-fsync-poisoned", expect_a,
           f"PUT 结果 = {puts}，GOOD = {good}，SIZE = {size}")
    # 磁盘上确实多了那条（write 成功、fsync 失败）→ 进程内只认一条，磁盘有两条
    if expect_a and size is not None:
        record("A-two-truths-contained", size == FILE_HEADER + 2 * rec,
               f"SIZE = {size}（期望 {FILE_HEADER + 2 * rec}：磁盘上有 2 条，进程内只认 1 条）")

    # ---- B) write 失败 + 回滚成功 → 普通失败，不 poison ----
    # 调用序列（新文件）：ctor 写文件头 write=1，put k000000 的 write=2
    print("[B] write 失败（第 2 次）+ 回滚成功 → 普通失败，仍可继续写")
    clean()
    env = dict(preload)
    env["KV_FAIL_WRITE_CALL"] = "2"
    r = run_poison(3, env)
    puts, good, size = parse(r.stdout)

    expect_b = (puts == [(0, 0), (1, 1), (2, 1)]) and (good == 1)
    record("B-rollback-ok", expect_b,
           f"PUT 结果 = {puts}，GOOD = {good}，SIZE = {size}")
    # 回滚成功 → 磁盘上只有 2 条（k000001、k000002），没有残骸
    if expect_b and size is not None:
        record("B-no-remains", size == FILE_HEADER + 2 * rec,
               f"SIZE = {size}（期望 {FILE_HEADER + 2 * rec}：回滚后没有残骸）")

    # ---- C) write 失败 + 回滚失败 → poisoned ----
    print("[C] write 失败（第 2 次）+ 回滚也失败（第 1 次 ftruncate）→ poisoned")
    clean()
    env = dict(preload)
    env["KV_FAIL_WRITE_CALL"] = "2"
    env["KV_FAIL_FTRUNCATE_CALL"] = "1"
    r = run_poison(3, env)
    puts, good, size = parse(r.stdout)

    expect_c = (puts == [(0, 0), (1, 0), (2, 0)]) and (good == 0)
    record("C-rollback-fail-poisoned", expect_c,
           f"PUT 结果 = {puts}，GOOD = {good}，SIZE = {size}")

    clean()

    failed = [r for r in results if not r[1]]
    print()
    print(f"[故障注入] 共 {len(results)} 项，失败 {len(failed)} 项")

    if failed:
        print("失败清单：")
        for name, _, detail in failed:
            print(f"  · {name}: {detail}")
        return 1

    print("[故障注入] PASS（fsync 失败 → poisoned / 回滚成功 → 普通失败 / 回滚失败 → poisoned）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
