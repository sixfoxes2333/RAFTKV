#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
crash_inject_test.py —— 存储崩溃注入测试（M5）

【为什么打存储层而不是服务器】
    kvserver 目前还是 echo（M6 才接存储），杀服务器根本碰不到落盘逻辑。
   所以这里用一个专门的写进程 build/crash_writer（见 tests/crash_writer.cpp）：
     脚本负责"在写的过程中杀掉它 / 破坏文件"，crash_writer 负责 write / verify。

【三个场景 + 各自的预期】

  1) kill -9：写第 N 条的过程中杀掉进程
     预期：重开必须能成功恢复（good() == true），且存在的记录是"前缀"
     不变量：target ≤ present ≤ N（target = 被杀前已确认完成的 put 数）
     为什么允许 present ≥ target：磁盘上可能已经完成、但还没来得及打印"put i"那一行
     为什么必须 ≥ target：put 成功返回 = 已经 fsync，进程随后被杀不影响已落盘的数据

  2) torn tail（残尾）：写完后从文件末尾截掉 k 字节
     预期：自动截掉最后一条残记录并成功启动 → present == N-1（少了最后一条）
     这是"断电时最后一条没写完"的模拟 —— 唯一真正能产生半条记录的场景

  3) 位翻转（磁盘损坏）：翻转文件中任意一个字节
     预期：必须【拒绝启动】（verify 退出码 2，good() == false）
     绝不能：返回成功但数据是错的；也绝不能把损坏点之后的数据静默删掉
     （这条正是外部审查发现的那个静默数据丢失，靠记录头校验和修掉）

【用法】
   python3 tests/crash_inject_test.py
   环境变量：KV_CRASH_ITER（kill 轮数，默认 12）、KV_CRASH_N（记录数，默认 200）
"""
import os
import random
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WRITER = os.path.join(REPO, "build", "crash_writer")
PATH = "/tmp/kv_crash_inject.log"

N_RECORDS = int(os.environ.get("KV_CRASH_N", "200"))
KILL_ITERATIONS = int(os.environ.get("KV_CRASH_ITER", "12"))
CRASH_LOG_TAIL = 400

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'ok  ' if ok else 'FAIL'}] {name:<22} {detail}")


def run(args, timeout=120):
    return subprocess.run([WRITER] + args, capture_output=True, text=True, timeout=timeout)


def clean():
    try:
        os.remove(PATH)
    except FileNotFoundError:
        pass


def present_of(out):
    for line in out.splitlines():
        if line.startswith("PRESENT "):
            return int(line.split()[1])
    return None


def write_all(n):
    """完整写 n 条，返回文件大小"""
    clean()
    r = run(["write", PATH, str(n)])
    if r.returncode != 0:
        print(f"!! 准备数据失败（write 退出码 {r.returncode}）：{r.stdout[-CRASH_LOG_TAIL:]}")
        sys.exit(2)
    return int(run(["size", PATH]).stdout.strip())


def scenario_kill():
    print(f"[1] kill -9：写第 N 条的过程中杀掉进程（{KILL_ITERATIONS} 轮）")

    for i in range(KILL_ITERATIONS):
        clean()
        # 杀在第 target 条之后，保证"确实落在写的过程中"
        target = random.randint(1, max(1, N_RECORDS - 2))

        proc = subprocess.Popen([WRITER, "write", PATH, str(N_RECORDS)],
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, bufsize=1)

        count = 0
        while count < target:
            line = proc.stdout.readline()
            if not line:            # 进程提前结束（写完了）
                break
            if line.startswith("put "):
                count += 1

        killed = proc.poll() is None
        if killed:
            proc.kill()
        proc.stdout.close()
        proc.wait(timeout=10)

        v = run(["verify", PATH, str(N_RECORDS)])
        p = present_of(v.stdout)

        ok = (v.returncode == 0) and (p is not None) and (count <= p <= N_RECORDS)
        detail = (f"轮{i}: {'被杀' if killed else '已写完'}，确认完成 {count} 条，"
                  f"恢复 present = {p}")
        record(f"kill#{i}", ok, detail)

        if not ok:
            print("       verify 输出：" + v.stdout.strip().replace("\n", " | ")[:CRASH_LOG_TAIL])


def scenario_torn_tail(rec_size):
    print("[2] torn tail：从文件末尾截掉 k 字节，必须能自动截掉残尾并启动")

    for k in [1, 5, 16, 17, 18, rec_size - 1, rec_size]:
        write_all(N_RECORDS)
        t = run(["truncate", PATH, str(k)])
        v = run(["verify", PATH, str(N_RECORDS)])
        p = present_of(v.stdout)

        #预期：能恢复（exit 0），且恰好丢掉最后一条记录
        ok = (t.returncode == 0) and (v.returncode == 0) and (p == N_RECORDS - 1)
        record(f"truncate-{k}B", ok, f"截掉 {k}B 后 present = {p}（期望 {N_RECORDS - 1}）")

        if not ok:
            print("       verify 输出：" + v.stdout.strip().replace("\n", " | ")[:CRASH_LOG_TAIL])


def scenario_bit_flip(rec_size):
    print("[3] 位翻转：翻转任意一个字节，必须 fail closed（拒绝启动）")

    size = write_all(N_RECORDS)
    cases = [
        ("magic", 0),
        ("version", 7),
        ("record1-header", 8 + 2),
        ("record1-key", 8 + 17),
        ("record1-value", 8 + 17 + 7),
        ("record-mid", 8 + rec_size * 2 + 5),
        ("last-byte", size - 1),
    ]

    for name, off in cases:
        write_all(N_RECORDS)
        f = run(["flip", PATH, str(off)])
        v = run(["verify", PATH, str(N_RECORDS)])

        #预期：拒绝启动（退出码 2）；绝不能返回 0（那意味着"成功但数据可能已损坏"）
        ok = (f.returncode == 0) and (v.returncode == 2)
        record(f"flip-{name}", ok, f"verify 退出码 {v.returncode}（期望 2 = 拒绝启动）")

        if not ok:
            print("       verify 输出：" + v.stdout.strip().replace("\n", " | ")[:CRASH_LOG_TAIL])


def scenario_empty_file():
    print("[4] 空文件：截到 0 字节 → 按『全新数据库』处理（已文档化的行为）")

    write_all(N_RECORDS)
    size = int(run(["size", PATH]).stdout.strip())
    run(["truncate", PATH, str(size)])

    if os.path.exists(PATH):
        os.remove(PATH)          # truncate 到 0 后文件仍存在，这里直接删掉重放同样的语义
    open(PATH, "wb").close()     # 造一个 0 字节文件

    v = run(["verify", PATH, str(N_RECORDS)])
    p = present_of(v.stdout)

    #预期：当成新库（写入 8B 文件头），present == 0
    ok = (v.returncode == 0) and (p == 0)
    record("empty-file", ok, f"present = {p}（期望 0）")


def main() -> int:
    if not os.path.exists(WRITER):
        print(f"!! 找不到 {WRITER}，先构建：cmake --build build -j")
        return 2

    print(f"[崩溃注入] writer = {WRITER}，记录数 = {N_RECORDS}，kill 轮数 = {KILL_ITERATIONS}")

    rec_size = (write_all(N_RECORDS) - 8) // N_RECORDS
    print(f"  单条记录 = {rec_size}B（17B 头 + 7B key + 7B value），文件头 8B")
    clean()

    scenario_kill()
    scenario_torn_tail(rec_size)
    scenario_bit_flip(rec_size)
    scenario_empty_file()
    clean()

    failed = [r for r in results if not r[1]]
    print()
    print(f"[崩溃注入] 共 {len(results)} 项，失败 {len(failed)} 项")

    if failed:
        print("失败清单：")
        for name, _, detail in failed:
            print(f"  · {name}: {detail}")
        return 1

    print("[崩溃注入] PASS（kill -9 可恢复 / 残尾可截断 / 位翻转 fail closed）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
