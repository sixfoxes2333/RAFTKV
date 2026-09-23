#!/usr/bin/env python3
"""kv_protocol_test.py —— 单节点 KV 端到端测试（M6c 验收）

覆盖：
  [1] 基本语义：SET / GET / DEL / NotFound / 覆盖写 / 删除后重写 / 空 value
  [2] 流水线：一次 write 塞多个请求，回复必须按顺序
  [3] 大 value：1MB 往返，内容逐字节一致
  [4] 协议错误：返回 Error 且连接仍可用；帧长超限 → 服务器关闭连接
  [5] 并发：8 个连接各自读写 100 轮
  [6] kill -9 后重启：数据仍在，已删除的 key 仍为 NotFound（端到端持久化）

PASS 条件：全部小节通过。
"""

import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kvproto import (                          # noqa: E402
    OP_SET, OP_GET, OP_DEL, ST_OK, ST_NOTFOUND, ST_ERROR,
    MAX_MESSAGE, connect, encode_request, frame, request, recv_frame,
    remove_db, start_server, stop_server,
)

DB = "/tmp/kv_e2e.data"
LOG = "/tmp/kv_e2e.log"
BIG = 1024 * 1024  # 1MB

fails = []


def check(label: str, cond: bool, detail: str = ""):
    if cond:
        print(f"  ok   : {label}")
    else:
        print(f"  FAIL : {label}   {detail}")
        fails.append(label)


def section(name: str):
    print(f"\n[{name}]")


def main():
    remove_db(DB)
    if os.path.exists(LOG):
        os.remove(LOG)

    proc = start_server(DB, LOG)
    try:
        # ---------- [1] 基本语义 ----------
        section("1 基本语义")
        s = connect()
        check("SET a=1 → Ok", request(s, OP_SET, b"a", b"1")[0] == ST_OK)
        st, val = request(s, OP_GET, b"a")
        check("GET a → Ok '1'", st == ST_OK and val == b"1", f"status={st} value={val!r}")
        check("GET missing → NotFound", request(s, OP_GET, b"missing")[0] == ST_NOTFOUND)

        st, _ = request(s, OP_SET, b"a", b"2")
        check("覆盖写 a=2 → Ok", st == ST_OK)
        check("覆盖后 GET a → '2'", request(s, OP_GET, b"a")[1] == b"2")

        check("DEL a → Ok", request(s, OP_DEL, b"a")[0] == ST_OK)
        check("删除后 GET a → NotFound", request(s, OP_GET, b"a")[0] == ST_NOTFOUND)
        check("重复 DEL a → NotFound", request(s, OP_DEL, b"a")[0] == ST_NOTFOUND)

        check("删除后重写 a=3 → Ok", request(s, OP_SET, b"a", b"3")[0] == ST_OK)
        check("重写后 GET a → '3'", request(s, OP_GET, b"a")[1] == b"3")

        st, val = request(s, OP_SET, b"empty", b"")
        check("SET 空 value → Ok", st == ST_OK)
        st, val = request(s, OP_GET, b"empty")
        check("GET 空 value → Ok ''", st == ST_OK and val == b"", f"status={st} value={val!r}")

        # 二进制 value（含 0x00 / 0xff）必须原样往返
        blob = bytes(range(256)) * 4
        request(s, OP_SET, b"blob", blob)
        check("二进制 value 往返一致", request(s, OP_GET, b"blob")[1] == blob)
        s.close()

        # ---------- [2] 流水线顺序 ----------
        section("2 流水线（一次写入多个请求）")
        s = connect()
        pipeline = [
            (OP_SET, b"p", b"1", ST_OK),
            (OP_GET, b"p", b"", ST_OK),
            (OP_SET, b"p", b"2", ST_OK),
            (OP_GET, b"p", b"", ST_OK),
            (OP_DEL, b"p", b"", ST_OK),
            (OP_GET, b"p", b"", ST_NOTFOUND),
        ]
        s.sendall(b"".join(encode_request(op, k, v) for op, k, v, _ in pipeline))

        expect_values = [b"", b"1", b"", b"2", b"", b""]
        for i, (op, k, v, want_status) in enumerate(pipeline):
            body = recv_frame(s)
            if body is None:
                check(f"流水线第 {i + 1} 条有回复", False, "连接被关闭")
                break
            status, value = body[0], body[5:]
            check(
                f"流水线第 {i + 1} 条顺序正确 (status={want_status})",
                status == want_status and value == expect_values[i],
                f"got status={status} value={value!r}",
            )
        s.close()

        # ---------- [3] 大 value ----------
        section("3 大 value（1MB）")
        s = connect(timeout=20.0)
        big = os.urandom(BIG)
        st, _ = request(s, OP_SET, b"big", big, timeout=20.0)
        check("SET 1MB → Ok", st == ST_OK)
        st, got = request(s, OP_GET, b"big", timeout=20.0)
        check("GET 1MB → Ok 且内容一致", st == ST_OK and got == big,
              f"status={st} len={len(got) if got else 0}")
        s.close()

        # ---------- [4] 协议错误 ----------
        section("4 协议错误")
        s = connect()

        s.sendall(frame(bytes([9]) + b"\x00\x00\x00\x01\x00\x00\x00\x00a"))  # 非法 op
        body = recv_frame(s)
        check("非法 op → Error", body is not None and body[0] == ST_ERROR)

        s.sendall(frame(bytes([OP_GET]) + b"\x00\x00\x00\x01\x00\x00\x00\x01aZ"))  # GET 带 value
        body = recv_frame(s)
        check("GET 带 value → Error", body is not None and body[0] == ST_ERROR)

        s.sendall(frame(bytes([OP_SET]) + b"\x00\x00\x00\x00\x00\x00\x00\x00"))  # 空 key
        body = recv_frame(s)
        check("空 key → Error", body is not None and body[0] == ST_ERROR)

        s.sendall(frame(bytes([OP_SET]) + b"\x00\x00\x00\x01\x00\x00\x00\x001x"))  # 长度少 1 字节
        body = recv_frame(s)
        check("声明长度与实际不符 → Error", body is not None and body[0] == ST_ERROR)

        check("出错的连接仍然可用（服务器没关它）", request(s, OP_GET, b"a")[0] == ST_OK)
        s.close()

        # 帧长超过 MAX_MESSAGE：服务器必须关连接（防恶意长度炸内存）
        s = connect()
        # 只发长度头，声明超限（不带 body）
        s.sendall((MAX_MESSAGE + 1).to_bytes(4, "big"))
        time.sleep(0.2)
        s.settimeout(5.0)
        check("帧长超限 → 连接被服务器关闭", s.recv(1) == b"")
        s.close()

        # ---------- [5] 并发 ----------
        section("5 并发（8 连接 × 100 轮）")
        errors = []


        def worker(tid: int):
            try:
                ws = connect(timeout=20.0)
                for i in range(100):
                    k = f"t{tid}k{i}".encode()
                    v = f"t{tid}v{i}".encode()
                    st, _ = request(ws, OP_SET, k, v, timeout=20.0)
                    if st != ST_OK:
                        errors.append(f"tid={tid} SET {k!r} status={st}")
                        break
                    st, got = request(ws, OP_GET, k, timeout=20.0)
                    if st != ST_OK or got != v:
                        errors.append(f"tid={tid} GET {k!r} status={st} value={got!r}")
                        break
                ws.close()
            except Exception as e:  # noqa: BLE001
                errors.append(f"tid={tid} 异常: {e!r}")

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(8)]
        t0 = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        elapsed = time.time() - t0
        check("并发 800 次 SET + 800 次 GET 无错误", not errors, "; ".join(errors[:3]))
        print(f"         耗时 {elapsed:.2f}s（1600 次操作，含 fsync）")

        # ---------- [6] kill -9 重启 ----------
        section("6 kill -9 后重启（端到端持久化）")
        s = connect()
        request(s, OP_SET, b"survivor", b"still-here")
        request(s, OP_SET, b"gone", b"bye")
        request(s, OP_DEL, b"gone")
        s.close()

        stop_server(proc, hard=True)   # kill -9：不跑任何收尾路径
        proc = start_server(DB, LOG)

        s = connect()
        st, val = request(s, OP_GET, b"survivor")
        check("重启后 GET survivor → 'still-here'", st == ST_OK and val == b"still-here",
              f"status={st} value={val!r}")
        check("重启后 GET gone → NotFound", request(s, OP_GET, b"gone")[0] == ST_NOTFOUND)
        check("重启后 GET a → '3'（更早的写也在）", request(s, OP_GET, b"a")[1] == b"3")

        request(s, OP_SET, b"written-after-restart", b"yes")
        s.close()

        # 再重启一次：重启后的写同样落盘
        stop_server(proc, hard=True)
        proc = start_server(DB, LOG)
        s = connect()
        st, val = request(s, OP_GET, b"written-after-restart")
        check("重启后的新写入也能跨重启", st == ST_OK and val == b"yes")
        s.close()

    finally:
        stop_server(proc, hard=True)

    print()
    if fails:
        print(f"[KV 协议测试] FAIL —— {len(fails)} 项失败：{fails[:5]}")
        return 1

    print("[KV 协议测试] PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
