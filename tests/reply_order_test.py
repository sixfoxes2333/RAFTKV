#!/usr/bin/env python3
"""reply_order_test.py —— 回复顺序围栏（修复 #2，方案 A）

【要证明什么】
服务端有两条"回包路径"：
  · 正常请求：Reactor 投给后台 worker，回复从完成队列回来（晚）
  · 解析失败：Reactor 当场 send_error（早）
两条路径混一起 → 错误回复会插队到前面正常请求的前面。

【实测（修复前，一次 write 发出 5 条）】
  请求流：[SET x=1] [坏请求] [GET x] [坏请求] [GET x]
  实际收到：Error, Error, Ok, Ok("1"), Ok("1")
            ↑ 第 1、2 个回复错位 —— 客户端会把 Error 当成 SET 的结果
  （确定性：SET 要 fsync 约 4.6ms，而解析坏请求只要几微秒）

【判据 A（顺序）】
  第 i 个回复必须对应第 i 个请求：
    期望 [Ok, Error, Ok("1"), Error, Ok("1")]
  并且坏请求之后连接必须仍然可用（解析失败不是关连接的理由）

【判据 B（队列满 → Error + 关连接）】
  worker 队列灌满时，连"按序的错误回复"都排不进去 —— 顺序已经无法保证。
  此时必须：回一个 Error，然后**关闭连接**（客户端在 Error 之后只能读到 EOF）。
  绝不允许"Error 之后还继续收到 Ok"：那会让客户端把整条回复流错位配对，
  而且它没有任何办法察觉。

【修复方向（方案 A）】解析失败也走同一条完成队列（合成任务占位），
  投递失败（队列满/worker 不可用）才回错误 —— 但必须同时关连接。

【用法】python3 tests/reply_order_test.py
"""

import os
import socket
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kvproto import (PORT, OP_SET, OP_GET, ST_OK, ST_NOTFOUND, ST_ERROR,  # noqa: E402
                     connect, decode_response, encode_request, frame, request,
                     recv_frame, remove_db, start_server, stop_server)

DB_A = "/tmp/kv_order_a.data"
DB_B = "/tmp/kv_order_b.data"
LOG_A = "/tmp/kv_order_a.log"
LOG_B = "/tmp/kv_order_b.log"

BAD = frame(b"\x63")   # op = 99 → parse_request 必然失败

STATUS_NAME = {ST_OK: "Ok", ST_NOTFOUND: "NotFound", ST_ERROR: "Error"}

fails = []


def check(label, cond, detail=""):
    print(f"  {'ok   ' if cond else 'FAIL '}: {label}" + (f"   {detail}" if detail and not cond else ""))
    if not cond:
        fails.append(label)


def name_of(item):
    st, val = item
    if st == "EOF":
        return "EOF"
    if st == "TIMEOUT":
        return "TIMEOUT"
    n = STATUS_NAME.get(st, f"?{st}")
    return f"{n}({val.decode(errors='replace')})" if val else n


def part_a():
    """第 i 个回复必须对应第 i 个请求"""
    print("[A] 一帧里混发：SET → 坏请求 → GET → 坏请求 → GET")
    remove_db(DB_A)
    proc = start_server(DB_A, LOG_A)
    try:
        s = connect()
        s.sendall(b"".join([
            encode_request(OP_SET, b"x", b"1"),
            BAD,
            encode_request(OP_GET, b"x"),
            BAD,
            encode_request(OP_GET, b"x"),
        ]))

        got = []
        for _ in range(5):
            body = recv_frame(s, 5.0)
            if body is None:
                got.append(("EOF", b""))
                break
            got.append(decode_response(body))

        expect = [(ST_OK, b""), (ST_ERROR, b""), (ST_OK, b"1"), (ST_ERROR, b""), (ST_OK, b"1")]
        print(f"  期望：{['Ok', 'Error', 'Ok(1)', 'Error', 'Ok(1)']}")
        print(f"  实际：{[name_of(i) for i in got]}")

        check("5 个回复的顺序与请求一一对应", got == expect, "见上面『实际』")

        st, val = request(s, OP_GET, b"x")
        check("坏请求之后连接仍可用（不能因为解析失败就关连接）",
              st == ST_OK and val == b"1", f"st={STATUS_NAME.get(st, st)} val={val!r}")
        s.close()
    finally:
        stop_server(proc, hard=True)


def part_b():
    """队列灌满 → 一个 Error + 关连接；不许 Error 之后还继续收到 Ok"""
    print("\n[B] 灌满 worker 队列：2000 笔 SET（每笔 fsync 约 4.6ms，队列必然排不下）")
    remove_db(DB_B)
    proc = start_server(DB_B, LOG_B)
    try:
        s = connect()
        payload = encode_request(OP_SET, b"key", b"v")
        try:
            s.sendall(payload * 2000)
        except OSError as e:
            print(f"  （发送被中断：{type(e).__name__}: {e}）")

        seq = []            # 收到的回复序列（名字）
        n_err = 0
        after_err = 0       # Error 之后又收到的帧数（正常回复或第二个 Error）→ 违规
        while True:
            try:
                body = recv_frame(s, 3.0)
            except (socket.timeout, OSError) as e:
                seq.append("TIMEOUT" if isinstance(e, socket.timeout) else "CLOSED_ERR")
                break
            if body is None:
                seq.append("EOF")
                break

            st, _ = decode_response(body)
            if st == ST_ERROR:
                n_err += 1
                if n_err > 1:
                    after_err += 1
            elif n_err > 0:
                after_err += 1
            if len(seq) < 8:
                seq.append(STATUS_NAME.get(st, f"?{st}"))

        n_ok = sum(1 for x in seq if x == "Ok")
        tail = seq[-1] if seq else "(空)"
        print(f"  收到 {n_ok} 个 Ok、{n_err} 个 Error；结尾 = {tail}")
        print(f"  前几个回复：{seq[:8]}")

        check("队列灌满时必须回 Error（不是静默丢弃）", n_err >= 1, "一个 Error 都没收到")
        # 连接结束的形式可能是干净的 EOF，也可能是 RST（客户端还在发数据时服务器关闭就会这样）
        # 两者语义相同：客户端必须把这批在途请求当成"未知"；不许再收到 Ok
        check("Error 之后连接必须结束（EOF 或 RST），不许继续收到 Ok",
              tail in ("EOF", "CLOSED_ERR") and after_err == 0,
              f"结尾={tail}，Error 之后还收到 {after_err} 帧")
        check("只回一个 Error（第一个失败就该停止服务这条连接）", n_err == 1, f"n_err={n_err}")
        s.close()
        # 过载期间的契约：
        #   · 有在途请求的连接 → Error 之后必须 EOF（上面已断言）
        #   · 没有在途请求的新连接 → 只回 Error（过载显式拒绝），连接不必被关
        s2 = connect()
        st1, _ = request(s2, OP_GET, b"key")
        st2, _ = request(s2, OP_GET, b"key")
        check("过载期间新连接拿到明确答复（Error = 显式拒绝，不是静默丢弃/挂住）",
              st1 in (ST_OK, ST_NOTFOUND, ST_ERROR), f"st={STATUS_NAME.get(st1, st1)}")
        check("过载期间新连接不被关闭（它在途请求为 0，顺序没有被破坏）",
              st2 in (ST_OK, ST_NOTFOUND, ST_ERROR), f"st={STATUS_NAME.get(st2, st2)}")

        # 压力解除后必须恢复服务（队列里 1024 笔 SET × 4.6ms fsync 约 5s 排空）
        deadline = time.time() + 20
        recovered = False
        while time.time() < deadline:
            st, _ = request(s2, OP_GET, b"key")
            if st in (ST_OK, ST_NOTFOUND):
                recovered = True
                break
            time.sleep(0.5)
        check("队列排空后服务器恢复正常服务（不是永久降级）", recovered,
              "20s 内一直返回 Error")
        s2.close()
    finally:
        stop_server(proc, hard=True)   # 队列里可能还压着上千笔 SET，直接 SIGKILL


def main() -> int:
    part_a()
    part_b()

    print()
    if fails:
        print(f"[回复顺序] FAIL —— {len(fails)} 项失败：{fails}")
        print("  → 方案 A：解析失败也投一个『合成任务』占位，让它的错误回复与正常回复")
        print("           走同一条完成队列（顺序自然一致）；只有投递失败才当场回 Error，")
        print("           并且必须同时关闭连接（顺序已无法保证时不能让客户端继续等）")
        return 1
    print("[回复顺序] PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
