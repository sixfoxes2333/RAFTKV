#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""logstore_fault_test.py —— Raft 日志存储在【写路径失败】时的行为

【为什么需要这一层】
  单元测试（test_logstore）只能验证正常路径、磁盘内容损坏和恢复行为；
  它没法让 pwrite / fsync / ftruncate 真的失败。这里用 tests/fail_inject.c
  编译出的 .so 通过 LD_PRELOAD 注入，不改一行生产代码。

【判据（每条都对应一个真实的崩溃窗口）】

  A. create 的 fsync 失败 → 必须报错，且原因里能看出失败的是文件还是目录
  B. append 的 pwrite 失败 → 必须报错；重开后 last_index 只能是"已完整写入的前缀"
     （半条记录按残尾截掉；绝不允许出现空洞）
  C. append 的 fsync 失败 → 必须报错且对象不可用；记录可能已在页缓存里，
     所以重开后 last_index 允许是"写之前"或"写之后"两个值之一 ——
     这正是"失败 ≠ 磁盘没变"的例子，上层必须按 fail-stop 处理
  D. truncate 的 ftruncate 失败 → 必须报错；文件保持原样
  E. truncate 的 fsync 失败 → 必须报错；删除可能生效也可能没有，两种都要能重开
  F. 短写（每次只写 1 字节）→ 靠 write_full 循环拼出完整记录
  G. fsync 次数：打开已有日志 = 恢复同步文件 + 同步目录 = 2 次；
     追加 10 条只在整批之后加 1 次 —— 这是"批提交"的直接证据（缺一次都不行）
  H. 系统调用顺序：追加 = fsync(恢复) → fsync(目录) → pwrite… → fsync(整批)；
     截断 = … → ftruncate → fsync。顺序错了会出现"数据没落盘就报告成功"

【用法】
  python3 tests/logstore_fault_test.py
"""
import os
import re
import shutil
import subprocess
import sys
import tempfile

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BINARY = os.path.join(REPO, "build", "test_logstore")
SHIM_SRC = os.path.join(REPO, "tests", "fail_inject.c")
SHIM = "/tmp/kv_fail_inject.so"

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  —— {detail}" if detail else ""))


def clean_env():
    env = os.environ.copy()
    shim_keys = ("KV_FAIL_", "KV_SHORT_PWRITE", "KV_COUNT_FSYNC", "KV_DELAY_FSYNC_MS")
    for key in list(env):
        if key == "LD_PRELOAD" or key.startswith(shim_keys):
            env.pop(key, None)
    return env


def run(args, env):
    return subprocess.run([BINARY] + args, capture_output=True, text=True, env=env)


def injected(**values):
    env = clean_env()
    env["LD_PRELOAD"] = SHIM
    for key, value in values.items():
        env[key] = str(value)
    return env


def count_lines(path):
    try:
        with open(path, "r", errors="replace") as handle:
            return len([line for line in handle.read().splitlines() if line.strip()])
    except OSError:
        return -1


def last_index(directory):
    back = run(["last", directory], clean_env())
    if back.returncode != 0:
        return f"<rc={back.returncode}>"
    return back.stdout.strip()


def trace_ops(args, directory):
    """跑一次 strace，返回 [(op, fd)]，只保留落在 directory 里的文件对象。

    先记 openat 的 fd → 路径映射，这样后续 fsync/pwrite/ftruncate 才能按对象过滤：
    否则目录 fsync 和文件 fsync 会被混在一起，看不出"两次 fsync 是不同对象"。
    """
    trace = os.path.join(directory, "trace.txt")
    with open(trace, "r", errors="replace") as handle:
        lines = handle.read().splitlines()

    fd_path = {}
    ops = []
    for line in lines:
        match = re.match(r"^(openat|pwrite64|fsync|ftruncate)\((.*)\)\s+=\s+(.*)$", line.strip())
        if match is None:
            continue
        op, rest, ret = match.groups()
        first = ret.split()[0] if ret.strip() else ""
        if op == "openat":
            found = re.search(r'"([^"]+)"', rest)
            if found is None or not first.lstrip("-").isdigit() or int(first) < 0:
                continue
            path = found.group(1)
            if not path.startswith("/"):
                # openat(3, "raft.log", ...)：相对路径要拼到目录 fd 上
                base = rest.split(",")[0].strip()
                if base != "AT_FDCWD" and base.isdigit():
                    path = os.path.join(fd_path.get(int(base), ""), path)
            fd_path[int(first)] = path
            continue
        if not first.lstrip("-").isdigit():
            continue
        fd = int(rest.split(",")[0])
        if fd_path.get(fd, "").startswith(directory):
            ops.append((op, fd))
    return ops


def strace_run(args, directory):
    trace = os.path.join(directory, "trace.txt")
    subprocess.run(["strace", "-e", "trace=openat,pwrite64,fsync,ftruncate",
                    "-o", trace, BINARY] + args,
                   capture_output=True, text=True, env=clean_env())
    return trace_ops(args, directory)


def check_create_fsync_failures(root):
    for call, keyword, label in ((1, "fsync new raft.log", "文件"),
                                 (2, "fsync log directory", "目录")):
        directory = os.path.join(root, f"create-fsync-{call}")
        os.makedirs(directory)
        r = run(["create", directory], injected(KV_FAIL_FSYNC_CALL=call))
        message = r.stderr.strip()
        record(f"create 的 fsync({label}) 失败必须报错",
               r.returncode == 1 and keyword in message,
               f"rc={r.returncode}, stderr={message[:60]!r}")


def check_append_write_failures(root):
    directory = os.path.join(root, "append-pwrite-first")
    os.makedirs(directory)
    run(["create", directory], clean_env())
    r = run(["append", directory, "5"], injected(KV_FAIL_PWRITE_CALL=1))
    message = r.stderr.strip()
    record("append 第一条就写失败：必须报错且原因含 pwrite",
           r.returncode == 1 and "pwrite" in message,
           f"rc={r.returncode}, stderr={message[:60]!r}")
    record("写失败后重开：last_index 仍为 0（什么都没提交）",
           last_index(directory) == "0", f"last={last_index(directory)}")

    directory = os.path.join(root, "append-pwrite-midway")
    os.makedirs(directory)
    run(["create", directory], clean_env())
    run(["append", directory, "4"], clean_env())
    r = run(["append", directory, "2"], injected(KV_FAIL_PWRITE_CALL=2))
    value = last_index(directory)
    record("中途写失败：重开后只保留完整前缀（4 或 5，不许有空洞）",
           r.returncode == 1 and value in ("4", "5"),
           f"rc={r.returncode}, last={value}")


def check_append_fsync_failure(root):
    directory = os.path.join(root, "append-fsync")
    os.makedirs(directory)
    run(["create", directory], clean_env())
    run(["append", directory, "4"], clean_env())
    # 这个进程里：打开已有日志 = 第 1、2 次 fsync，整批追加 = 第 3 次
    r = run(["append", directory, "1"], injected(KV_FAIL_FSYNC_CALL=3))
    message = r.stderr.strip()
    value = last_index(directory)
    record("append 的 fsync 失败：必须报错并报出 append 同步",
           r.returncode == 1 and "fsync appended" in message,
           f"rc={r.returncode}, stderr={message[:60]!r}")
    record("fsync 失败后重开：last 只能是 4 或 5（失败 ≠ 磁盘没变）",
           value in ("4", "5"), f"last={value}")


def check_truncate_failures(root):
    directory = os.path.join(root, "trunc-ftruncate")
    os.makedirs(directory)
    run(["create", directory], clean_env())
    run(["append", directory, "5"], clean_env())
    r = run(["truncate", directory, "3"], injected(KV_FAIL_FTRUNCATE_CALL=1))
    message = r.stderr.strip()
    value = last_index(directory)
    record("truncate 的 ftruncate 失败：必须报错且文件原样（last=5）",
           r.returncode == 1 and "ftruncate" in message and value == "5",
           f"rc={r.returncode}, last={value}, stderr={message[:50]!r}")

    directory = os.path.join(root, "trunc-fsync")
    os.makedirs(directory)
    run(["create", directory], clean_env())
    run(["append", directory, "5"], clean_env())
    # 这个进程里：打开 = 第 1、2 次 fsync，截断自己的 fsync = 第 3 次
    r = run(["truncate", directory, "3"], injected(KV_FAIL_FSYNC_CALL=3))
    message = r.stderr.strip()
    value = last_index(directory)
    record("truncate 的 fsync 失败：必须报错（删除可能生效也可能没有）",
           r.returncode == 1 and "fsync truncated" in message and value in ("2", "5"),
           f"rc={r.returncode}, last={value}, stderr={message[:50]!r}")


def check_short_write(root):
    directory = os.path.join(root, "short-write")
    os.makedirs(directory)
    run(["create", directory], clean_env())
    r = run(["append", directory, "3"], injected(KV_SHORT_PWRITE=1))
    v = run(["verify", directory, "3"], clean_env()) if r.returncode == 0 else None
    record("每次只写 1 字节：循环拼装后内容完整",
           r.returncode == 0 and v is not None and v.returncode == 0,
           f"append rc={r.returncode}, verify rc={v.returncode if v else '-'}")


def check_fsync_counts(root):
    directory = os.path.join(root, "fsync-count")
    os.makedirs(directory)
    run(["create", directory], clean_env())

    open_count = os.path.join(root, "count-open.txt")
    r = run(["last", directory], injected(KV_COUNT_FSYNC=open_count))
    lines = count_lines(open_count)
    record("打开已有日志 = 2 次 fsync（恢复同步文件 + 同步目录）",
           r.returncode == 0 and lines == 2, f"lines={lines}")

    append_count = os.path.join(root, "count-append.txt")
    r = run(["append", directory, "10"], injected(KV_COUNT_FSYNC=append_count))
    lines = count_lines(append_count)
    value = last_index(directory)
    record("整批追加 10 条只加 1 次 fsync（打开 2 次 + 整批 1 次 = 3）",
           r.returncode == 0 and lines == 3 and value == "10",
           f"lines={lines}, last={value}")


def check_syscall_order(root):
    if shutil.which("strace") is None:
        print("SKIP  系统调用顺序检查（本机没装 strace）")
        return

    # 追加：整批写入后必须有一次 fsync，且顺序是 先 pwrite 再 fsync
    directory = os.path.join(root, "order-append")
    os.makedirs(directory)
    run(["create", directory], clean_env())
    ops = strace_run(["append", directory, "2"], directory)
    names = [op for op, _ in ops]
    detail = f"实际顺序 {names}"
    ok = names == ["fsync", "fsync", "pwrite64", "pwrite64", "fsync"]
    if ok:
        log_fd = ops[2][1]
        ok = ops[0][1] != ops[1][1] and ops[0][1] == log_fd and ops[4][1] == log_fd
        detail = (f"fsync(文件 fd={ops[0][1]}) → fsync(目录 fd={ops[1][1]}) → "
                  f"pwrite64×2 → fsync(fd={ops[4][1]})")
    record("追加顺序 = fsync(恢复) → fsync(目录) → pwrite → pwrite → fsync(整批)", ok, detail)

    # 截断：ftruncate 之后必须紧跟一次 fsync
    directory = os.path.join(root, "order-trunc")
    os.makedirs(directory)
    run(["create", directory], clean_env())
    run(["append", directory, "5"], clean_env())
    ops = strace_run(["truncate", directory, "3"], directory)
    names = [op for op, _ in ops]
    ok = names == ["fsync", "fsync", "ftruncate", "fsync"]
    detail = f"实际顺序 {names}"
    if ok:
        ok = ops[0][1] != ops[1][1] and ops[2][1] == ops[0][1] and ops[3][1] == ops[0][1]
        detail = (f"fsync(文件 fd={ops[0][1]}) → fsync(目录 fd={ops[1][1]}) → "
                  f"ftruncate(fd={ops[2][1]}) → fsync(fd={ops[3][1]})")
    record("截断顺序 = fsync(恢复) → fsync(目录) → ftruncate → fsync", ok, detail)


def main():
    if not os.path.exists(BINARY):
        record("build/test_logstore 存在", False, f"{BINARY} 不存在（先构建）")
        return report()
    # shim 源码比 .so 新（或 .so 不存在）就重新编译：否则会拿旧的钩子跑测试，
    # 出现"明明注入了却什么都没发生"的假通过。
    if (not os.path.exists(SHIM) or
            os.path.getmtime(SHIM_SRC) > os.path.getmtime(SHIM)):
        r = subprocess.run(["gcc", "-shared", "-fPIC", "-O0", "-g", "-o", SHIM, SHIM_SRC, "-ldl"],
                           capture_output=True, text=True)
        if r.returncode != 0:
            record("编译故障注入 shim", False, r.stderr.strip()[:200])
            return report()

    root = tempfile.mkdtemp(prefix="kv_logstore_fault_")
    try:
        directory = os.path.join(root, "baseline")
        os.makedirs(directory)
        created = run(["create", directory], clean_env())
        appended = run(["append", directory, "5"], clean_env())
        verified = run(["verify", directory, "5"], clean_env())
        record("基线：create + 追加 5 条 + 逐条校验",
               created.returncode == 0 and appended.returncode == 0 and verified.returncode == 0,
               f"create rc={created.returncode}, append rc={appended.returncode}, "
               f"verify rc={verified.returncode}")

        check_create_fsync_failures(root)
        check_append_write_failures(root)
        check_append_fsync_failure(root)
        check_truncate_failures(root)
        check_short_write(root)
        check_fsync_counts(root)
        check_syscall_order(root)
    finally:
        shutil.rmtree(root, ignore_errors=True)
    return report()


def report():
    bad = [name for name, ok, _ in results if not ok]
    print()
    if bad:
        print(f"=== logstore_fault_test: {len(bad)} 项失败 ===")
        return 1
    print("=== logstore_fault_test: 全部通过 ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
