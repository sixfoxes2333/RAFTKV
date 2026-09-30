#!/usr/bin/env python3
"""group_commit_test.py —— 批提交（group commit）三模式的验收围栏

【要证明什么】
现在每条写各自 fsync 一次（SET 吞吐 = 1/fsync ≈ 180 ops/s，开流水线只增加排队）。
group commit 要做的：**一次 fsync 提交一批**，并提供三种模式：

  always（默认） 回复前这批必须已落盘    → 已确认的写，掉电也不丢
  everysec       最多每秒落一次盘        → 契约"最多丢 1 秒"（页缓存语义）
  off            不主动落盘              → 只用于对照实验

【判据】
always：① 批量提升：SET p=16 C=4 ≥ 2000 ops/s（修复前 ~180）
        ② 单条不变慢：SET C=1 p=1 的 P50 ≤ 6ms（不许引入等待）
        ③ 一批 fsync 失败 → 这一批【全部】Error + 之后不可用（注入 KV_FAIL_FSYNC_CALL=1）
        ④ kill -9 后：已收到 Ok 的写入必须全部还在
everysec：⑤ 落盘节流：连续写 ~3s，fsync 次数应为个位数（不是按批/按条）—— KV_COUNT_FSYNC 观察
          ⑥ 吞吐显著高于 always（≥2×）—— 证明 fsync 真的被推迟了
          ⑦ kill -9 后：库能打开、已确认的写入都在（kill -9 不会丢页缓存）
off：     ⑧ kill -9 后：库能打开、文件不损坏（残尾被截断，不要求全部数据）
配置：    ⑨ KV_SYNC_MODE 取值非法 → 必须【响亮退出】，不许静默降级成 always

注：③④⑤⑦⑧ 属于"修复前后都该绿"的围栏（防止新机制写坏语义）；
    ①②⑥⑨ 才是真红（修复前必然失败）。

【用法】python3 tests/group_commit_test.py
"""

import os
import select
import socket
import struct
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kvproto import (PORT, OP_SET, OP_GET, ST_OK, ST_ERROR,  # noqa: E402
                     connect, decode_response, encode_request, request,
                     remove_db, start_server, stop_server)

DB = "/tmp/kv_gc.data"
LOG = "/tmp/kv_gc.log"
FSYNC_LOG = "/tmp/kv_gc_fsync.log"
SHIM = "/tmp/kv_fail_inject.so"
SHIM_SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fail_inject.c")

VALUE = b"v" * 64
BATCH_MIN_QPS = 2000        # 判据①：批提交后的写入吞吐下限（保守）
SINGLE_P50_MAX_MS = 6.0     # 判据②：单条请求 P50 上限（当前 4.6ms 是 fsync 本体）
EVERY_SEC_RATIO = 2.0       # 判据⑥：everysec 吞吐至少是 always 的几倍

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
        print("编译 fail_inject.c 失败：", r.stderr.decode()[:300])
        return False
    return True


# ---------------- 负载：流水线（每连接 depth 条在途） ----------------

def load_set(conns, depth, duration, mode=None, tag=b"gc", value=VALUE):
    """流水线 SET 压测；返回 (acked, seconds, qps)"""
    acked = [0] * conns
    t_start = time.time()
    stop_at = t_start + duration
    threads = []

    def worker(cid):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", PORT))
        s.setblocking(False)
        seq = 0
        inflight = 0
        buf = b""
        try:
            while True:
                while inflight < depth and time.time() < stop_at:
                    s.sendall(encode_request(OP_SET, b"%s-%d-%d" % (tag, cid, seq), value))
                    seq += 1
                    inflight += 1
                if inflight == 0 and time.time() >= stop_at:
                    break
                r, _, _ = select.select([s], [], [], 0.2)
                if not r:
                    continue
                try:
                    chunk = s.recv(1 << 16)
                except BlockingIOError:
                    continue
                if not chunk:
                    break
                buf += chunk
                while len(buf) >= 9:                      # 回复固定 4(长度)+1(status)+4(vlen)
                    n = struct.unpack(">I", buf[:4])[0]
                    if len(buf) < 4 + n:
                        break
                    body = buf[4:4 + n]
                    buf = buf[4 + n:]
                    if body[0] == ST_OK:
                        acked[cid] += 1
                    inflight -= 1
        finally:
            s.close()

    import threading
    for i in range(conns):
        t = threading.Thread(target=worker, args=(i,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    dt = time.time() - t_start
    total = sum(acked)
    return total, dt, total / dt if dt > 0 else 0.0


def closed_loop_set(n, tag=b"c1"):
    """闭环（发一条等一条）测单条延迟；返回 (P50 ms, 平均 ms, acked)"""
    s = connect()
    lat = []
    acked = 0
    try:
        for i in range(n):
            req = encode_request(OP_SET, b"%s-%d" % (tag, i), VALUE)
            t0 = time.time()
            s.sendall(req)
            body = b""
            while len(body) < 4:
                body += s.recv(1 << 16)
            need = struct.unpack(">I", body[:4])[0]
            while len(body) < 4 + need:
                body += s.recv(1 << 16)
            st = body[4]
            lat.append((time.time() - t0) * 1000.0)
            if st == ST_OK:
                acked += 1
    finally:
        s.close()
    lat.sort()
    return lat[len(lat) // 2], sum(lat) / len(lat), acked


def recv_status(sock):
    """从连接里读一个完整的响应帧，返回 status 字节"""
    head = b""
    while len(head) < 4:
        chunk = sock.recv(1 << 16)
        if not chunk:
            return None
        head += chunk
    need = struct.unpack(">I", head[:4])[0]
    body = head[4:]
    while len(body) < need:
        chunk = sock.recv(1 << 16)
        if not chunk:
            return None
        body += chunk
    return body[0]


def count_fsyncs():
    if not os.path.exists(FSYNC_LOG):
        return []
    with open(FSYNC_LOG) as f:
        return [int(line.split()[0]) for line in f if line.strip()]


def kill9_and_restart(proc, env=None):
    proc.kill()
    proc.wait(timeout=5)
    time.sleep(0.2)
    return start_server(DB, LOG, env=env)


def verify_keys(keyval, limit=None):
    """逐个 GET 校验；返回 (ok 数, 缺失的 key 列表)"""
    s = connect()
    ok = 0
    missing = []
    try:
        for k, v in list(keyval.items())[:limit]:
            st, val = request(s, OP_GET, k, timeout=10.0)
            if st == ST_OK and val == v:
                ok += 1
            else:
                missing.append(k)
    finally:
        s.close()
    return ok, missing


def main() -> int:
    if not build_shim():
        return 2

    # ---------------- always 模式 ----------------
    print("\n[always] 批提交：一次 fsync 提交一批")
    remove_db(DB)
    env_always = {"KV_SYNC_MODE": "always"}
    proc = start_server(DB, LOG, env=env_always)
    always_qps = 0.0
    try:
        # ② 单条不变慢（先测：此时库里没多少东西）
        p50, avg, acked1 = closed_loop_set(30, tag=b"single")
        print(f"  闭环单条：P50={p50:.2f}ms 平均={avg:.2f}ms（{acked1}/30 成功）")
        check("② 单条请求 P50 ≤ %.1fms（批处理不许引入等待）" % SINGLE_P50_MAX_MS,
              acked1 == 30 and p50 <= SINGLE_P50_MAX_MS, f"P50={p50:.2f}ms acked={acked1}")

        # ① 批量提升
        acked, dt, always_qps = load_set(conns=4, depth=16, duration=2.5, tag=b"always")
        print(f"  流水线 p=16 C=4：{always_qps:.0f} ops/s（{acked} 条 / {dt:.1f}s）")
        check("① SET 流水线吞吐 ≥ %d ops/s（修复前 ~180）" % BATCH_MIN_QPS,
              always_qps >= BATCH_MIN_QPS, f"实测 {always_qps:.0f} ops/s")

        # ④ kill -9 后已确认的写仍在
        kv = {b"always-%d" % i: VALUE for i in range(200)}
        s = connect()
        confirmed = {}
        for k, v in kv.items():
            st, _ = request(s, OP_SET, k, v, timeout=10.0)
            if st == ST_OK:
                confirmed[k] = v
        s.close()
        proc = kill9_and_restart(proc, env_always)
        ok, missing = verify_keys(confirmed)
        print(f"  kill -9 重启后：{ok}/{len(confirmed)} 条已确认写入仍在")
        check("④ kill -9 后，已收到 Ok 的写入全部还在", ok == len(confirmed),
              f"丢了 {len(missing)} 条，例如 {missing[:3]}")
    finally:
        stop_server(proc, hard=True)

    # ③ fsync 失败：启动期要响亮退出；写入批次要保持"整批不可信"
    print("\n[always] fsync 失败：启动期响亮退出 + 批次保持「整批不可信」")
    remove_db(DB)
    startup_loud = False
    startup_why = "居然启动成功了，并且开始服务"
    try:
        p = start_server(DB, LOG, env={"KV_SYNC_MODE": "always", "LD_PRELOAD": SHIM,
                                       "KV_FAIL_FSYNC_CALL": "1"})
        stop_server(p, hard=True)
    except RuntimeError as e:
        startup_loud = True
        startup_why = str(e)[:80]
    check("③ 启动期（写文件头）的 fsync 失败 → 不许对外服务（非 0 退出）", startup_loud, startup_why)

    # 启动期用掉一次 fsync（文件头），所以第 2 次才是"第一批写"的 fsync
    remove_db(DB)
    proc = start_server(DB, LOG, env={"KV_SYNC_MODE": "always", "LD_PRELOAD": SHIM,
                                     "KV_FAIL_FSYNC_CALL": "2"})
    try:
        s = connect()
        for i in range(16):
            s.sendall(encode_request(OP_SET, b"fail-%d" % i, VALUE))

        statuses = []
        closed = False
        try:
            for _ in range(16):
                st = recv_status(s)
                if st is None:
                    closed = True
                    break
                statuses.append(st)
        except OSError:
            closed = True

        st_after = None
        try:
            s.sendall(encode_request(OP_SET, b"after-fail", VALUE))
            st_after = recv_status(s)
        except OSError:
            st_after = "连接被关"
        s.close()

        n_ok = statuses.count(ST_OK)
        n_err = statuses.count(ST_ERROR)
        print(f"  16 条 SET：{len(statuses)} 条回复（Ok={n_ok} Error={n_err}）"
              f"{'，连接被服务器关闭' if closed else ''}；之后再来一条 = {st_after}")

        # 关键不变量：fsync 失败的那一批里，绝不许有一条被回 Ok
        check("③ 批 fsync 失败 → 这一批【一条 Ok 都不许有】", n_ok == 0,
              f"出现了 {n_ok} 条 Ok：{statuses}")
        check("③ 失败必须被客户端感知（收到 Error 或连接被关）", n_err >= 1 or closed,
              f"既没 Error 也没关闭：{statuses}")
        check("③ 失败之后不再对外服务（进程退出或后续请求失败）", st_after != ST_OK,
              f"之后还成功了一次写：st_after={st_after}")
    finally:
        stop_server(proc, hard=True)

    # ---------------- everysec 模式 ----------------
    print("\n[everysec] 每秒最多落一次盘")
    remove_db(DB)
    for p in (FSYNC_LOG,):
        if os.path.exists(p):
            os.remove(p)
    env_sec = {"KV_SYNC_MODE": "everysec", "LD_PRELOAD": SHIM, "KV_COUNT_FSYNC": FSYNC_LOG}
    proc = start_server(DB, LOG, env=env_sec)
    every_qps = 0.0
    try:
        acked, dt, every_qps = load_set(conns=4, depth=16, duration=3.0, tag=b"every")
        t_end = time.time()
        time.sleep(1.2)                       # 让最后一个 1s 周期的 fsync 有机会发生
        fs = count_fsyncs()
        print(f"  连续写 {dt:.1f}s：{acked} 条 ack，{every_qps:.0f} ops/s；"
              f"期间 fsync 次数 = {len(fs)}")
        check("⑤ fsync 被节流（3 秒内个位数，不是按批/按条）", 1 <= len(fs) <= 8,
              f"fsync 次数 = {len(fs)}（{acked} 条写）")
        check("⑥ everysec 吞吐 ≥ %.1f× always（%.0f vs %.0f ops/s）" % (EVERY_SEC_RATIO, every_qps, always_qps),
              always_qps > 0 and every_qps >= EVERY_SEC_RATIO * always_qps,
              f"{every_qps:.0f} / {always_qps:.0f} = {every_qps / always_qps:.2f}x")

        # ⑦ kill -9 后已确认的写仍在（kill -9 不丢页缓存）
        kv = {b"every-%d" % i: VALUE for i in range(200)}
        s = connect()
        confirmed = {}
        for k, v in kv.items():
            st, _ = request(s, OP_SET, k, v, timeout=10.0)
            if st == ST_OK:
                confirmed[k] = v
        s.close()
        proc = kill9_and_restart(proc, env_sec)
        ok, missing = verify_keys(confirmed)
        print(f"  kill -9 重启后：{ok}/{len(confirmed)} 条仍在（库能打开）")
        check("⑦ everysec：kill -9 后库能打开且已确认的写入都在", ok == len(confirmed),
              f"丢了 {len(missing)} 条")
    finally:
        stop_server(proc, hard=True)

    # ---------------- off 模式 ----------------
    print("\n[off] 不主动落盘（对照组）")
    remove_db(DB)
    env_off = {"KV_SYNC_MODE": "off"}
    proc = start_server(DB, LOG, env=env_off)
    try:
        acked, dt, qps = load_set(conns=2, depth=8, duration=1.0, tag=b"off")
        kv = {b"off-%d" % i: VALUE for i in range(50)}
        s = connect()
        for k, v in kv.items():
            request(s, OP_SET, k, v, timeout=10.0)
        s.close()
        proc = kill9_and_restart(proc, env_off)
        ok, _ = verify_keys(kv)
        print(f"  off 模式：{qps:.0f} ops/s；kill -9 重启后 {ok}/{len(kv)} 条仍在")
        check("⑧ off：kill -9 后库能正常打开（不损坏；不要求数据全在）",
              proc.poll() is None, "重启后进程没活着（可能判成损坏或启动失败）")
    finally:
        stop_server(proc, hard=True)

    # ---------------- 配置校验 ----------------
    print("\n[配置] 非法的 KV_SYNC_MODE 必须响亮退出")
    remove_db(DB)
    raised = False
    why = ""
    try:
        p = start_server(DB, LOG, env={"KV_SYNC_MODE": "sometimes"})
        stop_server(p, hard=True)
    except RuntimeError as e:
        raised = True
        why = str(e)[:100]
    check("⑨ 非法 KV_SYNC_MODE → 启动失败（不许静默降级成 always）", raised, why or "居然启动成功了")

    print()
    if fails:
        print(f"[批提交] FAIL —— {len(fails)} 项失败：{fails}")
        print("  → 修复方向：Storage 拆『追加(不 fsync) + 统一 flush』；worker 把已就绪任务攒成一批、")
        print("           一次 fsync 后按队列顺序回结果；KV_SYNC_MODE=always|everysec|off（非法值报错退出）；")
        print("           everysec 用 cv_.wait_for(1s) 超时路径补 fsync")
        return 1
    print("[批提交] PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
