#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""raft_state_fault_test.py —— term/vote 持久化在【写路径失败】时的行为

【为什么需要这一层】
  单元测试（test_raft_state）只能验证正常路径和磁盘内容损坏；
  它没法让 write/fsync 真的失败。这里用 tests/fail_inject.c 编译出的 .so
  通过 LD_PRELOAD 注入，不改一行生产代码。

【三条判据（每条都对应一个真实的崩溃窗口）】

  A. write 失败 → save() 必须返回 false，且 errno 原因不能丢
     · 正式文件必须还是【旧值】（这次保存根本没走到 rename）
     · error 字符串必须非空 —— 这条是回归围栏：
       曾经出现过 sync_fd(..., std::string error) 少写 & 的 bug，
       返回码是对的、错误信息却丢了，上线后等于没有排障线索

  B. fsync(临时文件) 失败 → save() 必须返回 false
     · 正式文件仍是旧值（内容没落盘就不许 rename）

  C. fsync(父目录) 失败 → save() 必须返回 false
     · 此时【新值可能已经生效】—— 这正是"失败 ≠ 磁盘没变"的例子
     · 所以判据是"必须报错"，而不是"必须读到旧值"
     · 上层（RaftWorker）必须按 fail-stop 处理，不能继续发送依赖本次状态的回复

【用法】
  python3 tests/raft_state_fault_test.py
"""
import os
import re
import shutil
import subprocess
import sys
import tempfile

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BINARY = os.path.join(REPO, "build", "test_raft_state")
SHIM_SRC = os.path.join(REPO, "tests", "fail_inject.c")
SHIM = "/tmp/kv_fail_inject.so"

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  —— {detail}" if detail else ""))


def clean_env():
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("KV_FAIL_") or key in ("LD_PRELOAD", "KV_DELAY_FSYNC_MS", "KV_COUNT_FSYNC"):
            env.pop(key, None)
    return env


def run(args, env):
    return subprocess.run(args, capture_output=True, text=True, env=env)


def main():
    if not os.path.exists(BINARY):
        record("build/test_raft_state 存在", False, f"{BINARY} 不存在（先构建）")
        return report()
    if not os.path.exists(SHIM):
        r = subprocess.run(["gcc", "-shared", "-fPIC", "-O0", "-g", "-o", SHIM, SHIM_SRC, "-ldl"],
                           capture_output=True, text=True)
        if r.returncode != 0:
            record("编译故障注入 shim", False, r.stderr.strip()[:200])
            return report()

    directory = tempfile.mkdtemp(prefix="kv_raft_state_fault_")
    try:
        base = clean_env()
        r = run([BINARY, "save", directory, "3", "2"], base)
        if r.returncode != 0:
            record("基线：能正常保存 3/2", False, r.stderr.strip()[:200])
            return report()

        cases = [
            ("write 失败：save 报错且正式文件仍是旧值", {"KV_FAIL_WRITE_CALL": "1"}, "write", "3 2"),
            ("fsync(临时文件) 失败：不许 rename，仍是旧值", {"KV_FAIL_FSYNC_CALL": "1"}, "fsync", "3 2"),
            ("fsync(父目录) 失败：必须报错（新值可能已生效）", {"KV_FAIL_FSYNC_CALL": "2"}, "fsync", None),
        ]
        for name, inject, keyword, expect in cases:
            env = clean_env()
            env.update(inject)
            env["LD_PRELOAD"] = SHIM
            r = run([BINARY, "save", directory, "4", "0"], env)
            ok = r.returncode == 1
            detail = f"rc={r.returncode}"
            if not ok:
                record(name, False, detail + "（应当返回 1）")
                continue

            # 错误信息不能丢：这是 & 漏写那类 bug 的围栏
            message = r.stderr.strip()
            if not message or keyword not in message:
                record(name, False, f"错误信息不合格: {message!r}（应包含 {keyword!r}）")
                continue

            back = run([BINARY, "load", directory], clean_env())
            if back.returncode != 0:
                record(name, False, f"失败后再读也失败: {back.stderr.strip()[:120]}")
                continue
            value = back.stdout.strip()
            if expect is not None and value != expect:
                record(name, False, f"读回 {value!r}，应为 {expect!r}")
                continue
            record(name, True, f"rc=1, 读到 {value!r}, error={message[:48]!r}")

        # 失败之后仍然可用：下一次正常保存必须成功（旧的 tmp 残留不该挡住它）
        r = run([BINARY, "save", directory, "5", "1"], clean_env())
        back = run([BINARY, "load", directory], clean_env()) if r.returncode == 0 else None
        ok = r.returncode == 0 and back is not None and back.stdout.strip() == "5 1"
        record("注入结束后恢复正常：保存 5/1 并读回", ok,
               f"rc={r.returncode}, load={back.stdout.strip() if back else '-'}")

        check_syscall_order()
    finally:
        shutil.rmtree(directory, ignore_errors=True)
    return report()


def check_syscall_order():
    """断言落盘顺序：open(tmp) → write → fsync(临时文件) → rename → fsync(父目录)

    为什么需要这条：漏掉 fsync(父目录) 的功能测试会全绿（ext4 默认约 5 秒提交一次日志），
    只有在真正的掉电窗口才暴露。能自动化验证的只有【系统调用顺序】。
    顺序错了（比如"先 rename 再 fsync 新文件"）就会出现"名字指向内容未落盘的 inode"，
    那是历史上很出名的"rename 后零字节文件"。
    """
    if shutil.which("strace") is None:
        print("SKIP  系统调用顺序检查（本机没装 strace）")
        return

    directory = tempfile.mkdtemp(prefix="kv_raft_order_")
    trace = os.path.join(directory, "trace.txt")
    try:
        subprocess.run(["strace", "-e", "trace=openat,write,fsync,rename", "-o", trace,
                        BINARY, "save", directory, "6", "6"],
                       capture_output=True, text=True, env=clean_env())
        with open(trace, "r", errors="replace") as handle:
            lines = handle.read().splitlines()
    finally:
        shutil.rmtree(directory, ignore_errors=True)

    seq = []
    for line in lines:
        match = re.match(r"^(openat|write|fsync|rename)\((.*)\)\s*=", line.strip())
        if match is None:
            continue
        op, rest = match.groups()
        if op == "openat":
            if "raft.state.tmp" in rest:
                seq.append((op, rest))
        elif op == "rename":
            if "raft.state" in rest:
                seq.append((op, rest))
        elif op == "write":
            # 只看写文件的 write：忽略 stdout/stderr（test_raft_state 会打印 "saved"）
            if rest.split(",")[0].strip() not in ("1", "2"):
                seq.append((op, rest))
        else:  # fsync
            seq.append((op, rest))

    names = [op for op, _ in seq]
    expected = ["openat", "write", "fsync", "rename", "fsync"]
    if names != expected:
        record("系统调用顺序 = open(tmp) → write → fsync → rename → fsync(dir)",
               False, f"实际顺序 {names}")
        return

    fds = [rest.split(")")[0].split(",")[0] for op, rest in seq if op == "fsync"]
    if len(fds) != 2 or fds[0] == fds[1]:
        record("两次 fsync 是不同对象（文件 vs 目录）", False, f"fsync fd = {fds}")
        return

    record("系统调用顺序 = open(tmp) → write → fsync(文件) → rename → fsync(目录)", True,
           f"fsync fd {fds[0]}(文件) → {fds[1]}(目录)")


def report():
    bad = [name for name, ok, _ in results if not ok]
    print()
    if bad:
        print(f"=== raft_state_fault_test: {len(bad)} 项失败 ===")
        return 1
    print("=== raft_state_fault_test: 全部通过 ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
