#!/usr/bin/env python3
"""net_pause_read_test.py —— "暂停读"必须真的作用到内核掩码（修复 #4）

【要证明什么】
发送队列超过高水位（OUT_HIGH_WATER = 2MB）时，服务端应该**真的停止读**（内核掩码摘掉 EPOLLIN），
让客户端被 TCP 流量控制挡住 —— 这就是背压。
但现在 pause_read()/resume_read() 只改了 Connection::events_ 这个**缓存**，
从来没写回内核，所以事件循环照旧收到可读事件 → 服务端继续收请求 → 队列/预算限制先炸，
**连接被关掉**（而不是把压力转成 TCP 背压）。

【怎么测才算数（踩过的坑都记在这儿）】
  1) 客户端必须用非阻塞 send，并且**记住没发完的剩余字节**接着发。
     忽略返回值 = 丢掉半条帧 → 服务端把帧头后面的字节当长度（bad len）→ 它关连接是"合法的"。
  2) 客户端"不读回复"必须持续到发送队列真的堆起来（服务端回复慢，堆起来要时间）。
  3) ★ 客户端不能一上来就猛灌：服务端**读得比回复快**（实测 in-flight 512 条时只完成了 23 条回复），
     一次灌太多的话，"条数上限/字节预算"这些限制一定先触发 → 连接被拒绝关闭，
     水位线(2MB)根本没机会起作用。所以必须**先慢发一批**（让 in-flight 保持很小、
     而 out_ 越过 2MB），**再猛灌** —— 这时才是在测"暂停读"。

【判据】
  A. 连接没被杀；已完整发出的请求，最后都收到了正确回复
     （后半句同时验证 resume_read：压力退去后，堆在内核缓冲里的请求必须被重新读取、作答）
  B. 客户端必须被流量控制挡住（连续多次 send 返回 EAGAIN）
     —— 这是"服务端真的停止读"的唯一直接证据
  C. 压力结束后服务器仍能服务新连接

【修复方向】pause_read()/resume_read() 改完 events_ 必须立刻写回内核（mod_event），
  并把返回值当回事（失败就 request_close）—— 与 enable_write()/disable_write() 一致。
  （这两个函数在 connection.h 里是 inline，而那里只有 Reactor 前置声明，
   所以需要新增一个在 connection.cpp 里实现的 apply_events()。）

【用法】python3 tests/net_pause_read_test.py
"""

import errno
import os
import select
import socket
import struct
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kvproto import (PORT, OP_SET, OP_GET, ST_OK,  # noqa: E402
                     connect, decode_response, encode_request,
                     remove_db, request, start_server, stop_server)

DB = "/tmp/kv_pause.data"
LOG = "/tmp/kv_pause.log"

KEY_LEN = 64 * 1024            # 请求 64KB（请求大，客户端自己的发送缓冲很快被填满）
VALUE_LEN = 256 * 1024         # 回复 256KB（回复大，out_ 很快越过 2MB 高水位）
PACED = 30                     # 先慢发这么多条（in-flight 保持很小，让 out_ 堆起来）
PACE_SLEEP = 0.005
REQ_CAP = 1500                 # 慢发之后猛灌的上限
PUSH_DEADLINE = 15.0
SILENCE_LIMIT = 2.0            # 连续多久收不到回复就算读完
DRAIN_DEADLINE = 40.0
BLOCK_STREAK = 10              # 连续这么多次 EAGAIN 才算"真被流量控制挡住"

fails = []


def check(label, cond, detail=""):
    print(f"  {'ok   ' if cond else 'FAIL '}: {label}" + (f"   {detail}" if detail and not cond else ""))
    if not cond:
        fails.append(label)


def main() -> int:
    remove_db(DB)
    proc = start_server(DB, LOG)
    print(f"[暂停读] 服务器已启动（db={DB}）")

    try:
        # ---------- 预置：64KB 的 key → 256KB 的 value ----------
        seed = connect()
        key = b"k" * KEY_LEN
        st, _ = request(seed, OP_SET, key, b"v" * VALUE_LEN, timeout=10.0)
        seed.close()
        if st != ST_OK:
            print(f"!! 预置失败 st={st}")
            return 2

        # 客户端：接收缓冲设小（服务端侧更快堆积）+ 非阻塞 + 不读回复
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * 1024)
        s.connect(("127.0.0.1", PORT))
        s.setblocking(False)

        req = encode_request(OP_GET, key)   # 请求 64KB+15 / 回复 256KB+5
        print(f"  请求 {len(req) // 1024}KB / 回复 {VALUE_LEN // 1024}KB；"
              f"先慢发 {PACED} 条，再猛灌")

        sent = 0            # 完整发出去的请求条数
        pending = req       # 当前这条还没发完的剩余字节
        block_streak = 0
        killed = None

        def try_send_blocking_aware():
            """发一次；返回 False 表示要跳出（被挡住 / 出错）"""
            nonlocal sent, pending, block_streak, killed
            try:
                n = s.send(pending)
            except BlockingIOError:
                block_streak += 1
                if block_streak >= BLOCK_STREAK:
                    return False
                time.sleep(0.01)
                return True
            except OSError as e:
                killed = f"{type(e).__name__}: {e}"
                return False
            pending = pending[n:]
            if not pending:
                pending = req
                sent += 1
                block_streak = 0
            return True

        # ---------- 阶段 1a：慢发（让 out_ 越过水位线，同时 in-flight 很小）----------
        print(f"\n[1a] 慢发 {PACED} 条（每条之间 {PACE_SLEEP * 1000:.0f}ms），全程不读回复")
        t0 = time.time()
        while sent < PACED and time.time() - t0 < PUSH_DEADLINE:
            if not try_send_blocking_aware():
                break
            time.sleep(PACE_SLEEP)
        print(f"  慢发结束：已发出 {sent} 条（连接被关闭 = {killed or '否'}）")

        # ---------- 阶段 1b：猛灌（看客户端会不会被流量控制挡住）----------
        print(f"\n[1b] 猛灌到被挡住 / 被关闭（上限 {REQ_CAP} 条）")
        t1 = time.time()
        while killed is None and sent < REQ_CAP and time.time() - t1 < PUSH_DEADLINE:
            if not try_send_blocking_aware():
                break

        throttled = block_streak >= BLOCK_STREAK
        print(f"  完整发出 {sent} 条；连接被关闭 = {killed or '否'}；"
              f"被流量控制挡住 = {'是' if throttled else '否'}")

        # ---------- 阶段 2：不再发新请求，把已发出的回复读完 ----------
        print(f"\n[2] 开始读回复（客户端读 → 服务端应恢复读，把堆在缓冲里的请求也读掉）")
        got_ok = 0
        err = None
        buf = b""
        last_data = time.time()
        t2 = time.time()
        while time.time() - t2 < DRAIN_DEADLINE and time.time() - last_data < SILENCE_LIMIT:
            r, _, _ = select.select([s], [], [], 0.5)
            if not r:
                continue
            try:
                chunk = s.recv(1 << 16)
            except BlockingIOError:
                continue
            except OSError as e:
                err = f"{type(e).__name__}: {e}"
                break
            if not chunk:
                err = "对端关闭（EOF）"
                break
            buf += chunk
            last_data = time.time()
            while len(buf) >= 4:
                n = struct.unpack(">I", buf[:4])[0]
                if len(buf) < 4 + n:
                    break
                st, val = decode_response(buf[4:4 + n])
                buf = buf[4 + n:]
                if st == ST_OK and len(val) == VALUE_LEN:
                    got_ok += 1
                else:
                    err = f"回复异常 st={st} len={len(val)}"
                    break
        print(f"  收到 {got_ok} 条正确回复（已完整发出 {sent} 条）" + (f"，中断：{err}" if err else ""))

        # ---------- 判据 ----------
        print()
        check("A. 连接没有被上限/预算杀掉（压力转成了背压）", killed is None, f"连接被关闭：{killed}")
        check("A. 已完整发出的请求都拿到了回复（含 resume 后重新读取的那部分）",
              got_ok == sent and err is None,
              f"收到 {got_ok}/{sent}" + (f"（{err}）" if err else ""))
        check("B. 客户端被流量控制挡住（= 服务端真的停止读了）", throttled,
              f"一路发出 {sent} 条都没被挡住 —— 内核掩码里的 EPOLLIN 根本没摘掉")

        # ---------- 判据 C：服务器仍然健康 ----------
        s.close()
        s2 = connect()
        st, val = request(s2, OP_GET, key, timeout=10.0)
        check("C. 压力结束后服务器仍能正常服务", st == ST_OK and len(val) == VALUE_LEN, f"st={st}")
        s2.close()

    finally:
        stop_server(proc, hard=True)

    print()
    if fails:
        print(f"[暂停读] FAIL —— {len(fails)} 项失败：{fails}")
        print("  → 修复方向：pause_read()/resume_read() 改完 events_ 要立刻写回内核（mod_event），")
        print("           并把返回值当回事（失败就 request_close）")
        return 1
    print("[暂停读] PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
