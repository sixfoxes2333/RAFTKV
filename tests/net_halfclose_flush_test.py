#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
net_halfclose_flush_test.py —— "半关闭 + 大回复" 语义围栏（Step 3）

【这个测试在验证什么】
    客户端发一条 8MB 请求，随即 shutdown(SHUT_WR)（半关闭：我不再发，但还要收），
    然后故意不读一会儿，把服务器的发送缓冲填满（必然 EAGAIN）。
    服务器必须：
      1) 把已到达的请求全部读完、全部处理
      2) 把 8MB 响应全部发完
      3) 之后才关闭连接
    客户端必须"收满 8MB 之后"才看到 EOF。

【为什么这条重要 —— 它是"围栏"不是"红绿"】
    在当前这个 echo 服务器下，这个场景恰好能过，原因是侥幸：
      服务器 flush（out_ 里还有数据）时，把关注掩码整个换成了 EPOLLOUT，
      于是最危险的窗口里根本收不到 EPOLLIN，"读到 EOF → 立刻关连接"这条路被屏蔽了。
    Step 3 要把语义写明：**对端半关闭 ≠ 立刻关连接，先把待发数据发完**。
    所以本用例的角色是：
      · 现在就该是 PASS（作为基线）
      · Step 3 若实现成"收到 RDHUP/EOF 立刻 request_close"，它会立刻变红
      · 将来回复改成由存储层异步产生（EOF 可能先于回复到达）时，它同样能挡住回归

【第二个检查点：不许空转】
    "服务器有待发数据、对端暂时不读"的窗口里，服务器应当空闲阻塞在 epoll_wait 上。
    这里用 /proc/<pid>/stat 的 utime+stime 度量这段窗口的 CPU 时间：
      · 正确实现：≈ 0
      · 常见错误实现：处理半关闭时把"EOF 可读"继续留在关注掩码里
        → 水平触发下 epoll_wait 立即返回 → 死循环 → 一个核跑满 100%

【期望结果】
    PASS：回复长度与请求一致 + 内容一致 + 收满之后才 EOF + 空闲窗口 CPU < 0.5s

【用法】
    python3 tests/net_halfclose_flush_test.py
    环境变量：KV_SERVER / KV_PORT / KV_TEST_SNDBUF
"""
import os
import socket
import struct
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kvproto import OP_SET, OP_GET, ST_OK, encode_request, recv_frame  # noqa: E402

SERVER = os.environ.get("KV_SERVER", "build/kvserver")
PORT = int(os.environ.get("KV_PORT", "9006"))
SERVER_LOG = "/tmp/kv_halfclose_flush_server.log"

PAYLOAD_SIZE = 8 * 1024 * 1024      # 8MB：远大于"内核发送缓冲 + 接收缓冲"
IDLE_WINDOW = 2.0                   # 观察服务器是否空转的时长（秒）
CPU_BUDGET = 0.5                    # 空闲窗口内允许消耗的 CPU 时间上限（秒）
READ_TIMEOUT = 60.0                 # 读回复的总超时（秒）
SEND_TIMEOUT = 30.0                 # 发送阶段的超时（sendall 也可能永久阻塞）
CLK_TCK = os.sysconf("SC_CLK_TCK")  # 每秒的时钟滴答数（通常 100）


def frame(payload: bytes) -> bytes:
    """按协议打包一条消息：[4 字节大端长度][内容]"""
    return struct.pack(">I", len(payload)) + payload


def start_server():
    """启动自己的服务器实例（与其他测试同样的就绪探测逻辑）。"""
    log = open(SERVER_LOG, "wb")
    env = os.environ.copy()
    env.setdefault("KV_TEST_SNDBUF", "4096")   # 服务器支持时生效；不支持也无害
    proc = subprocess.Popen([SERVER], stdout=log, stderr=subprocess.STDOUT,
                            env=env, cwd=os.getcwd())
    deadline = time.time() + 5.0
    while time.time() < deadline:
        if proc.poll() is not None:
            break
        try:
            s = socket.create_connection(("127.0.0.1", PORT), timeout=0.2)
            s.close()
            return proc
        except OSError:
            time.sleep(0.05)

    log.flush()
    print("!! 服务器启动失败，日志如下：")
    try:
        print(open(SERVER_LOG, encoding="utf-8", errors="replace").read())
    except OSError:
        pass
    print("!! 提示：端口可能被占用，先确认没有别的 kvserver 在跑")
    sys.exit(2)


def stop_server(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=3)


def cpu_seconds(pid: int):
    """读 /proc/<pid>/stat 里的 utime+stime，返回累计 CPU 秒数（失败返回 None）。"""
    try:
        with open(f"/proc/{pid}/stat", "r") as f:
            data = f.read()
    except OSError:
        return None
    # comm（第 2 个字段）可能含空格和括号 → 从最后一个 ')' 之后再切分
    rest = data[data.rindex(")") + 2:].split()
    # 切分后：rest[0]=state(字段3) …… utime=字段14 → rest[11]，stime=字段15 → rest[12]
    utime, stime = int(rest[11]), int(rest[12])
    return (utime + stime) / CLK_TCK


def recv_exact(s: socket.socket, n: int, deadline: float) -> bytes:
    """精确读 n 字节；超时或提前 EOF 时返回已读到的部分（可能为空）。

    注意：必须给 socket 设 timeout，否则阻塞中的 recv 不会被上面的时钟检查打断 ——
    那会让整个测试脚本只能靠外部 timeout 杀掉（审查指出的问题）。
    """
    buf = bytearray()
    while len(buf) < n:
        remain = deadline - time.time()
        if remain <= 0:
            print("  !! 读取超时")
            return bytes(buf)
        s.settimeout(remain)          #关键：让阻塞的 recv 自己超时
        try:
            chunk = s.recv(min(65536, n - len(buf)))
        except socket.timeout:
            print("  !! 读取超时")
            return bytes(buf)
        if not chunk:
            return bytes(buf)
        buf += chunk
    return bytes(buf)


def main() -> int:
    payload = bytes(range(256)) * (PAYLOAD_SIZE // 256)   # 有规律的 8MB，便于定位差异

    print(f"[半关闭+大回复] 启动服务器 {SERVER}")
    proc = start_server()
    try:
        s = socket.socket()
        # 把收缓冲压小：保证"客户端不读"的那段窗口里，服务器一定会写完缓冲并撞上 EAGAIN
        # （否则默认缓冲可能把整条 8MB 吞下，服务器根本不需要排队等待，围栏就失去意义）
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        s.connect(("127.0.0.1", PORT))
        real_rcvbuf = s.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        print(f"  客户端 SO_RCVBUF 实际值 = {real_rcvbuf} 字节（内核会把设置值加倍）")

        # ---- 先把 8MB 的 value 存进服务器（另一条普通连接，避免干扰围栏） ----
        setup = socket.create_connection(("127.0.0.1", PORT), timeout=SEND_TIMEOUT)
        setup.sendall(encode_request(OP_SET, b"big", payload))
        pre = recv_frame(setup, SEND_TIMEOUT)
        setup.close()
        if pre is None or pre[0] != ST_OK:
            print(f"  !! 预置数据失败（SET big 未返回 Ok，收到 {pre!r}）")
            s.close()
            return 1

        print(f"  发送 GET big（value {len(payload)} 字节），随即 shutdown(SHUT_WR)（半关闭）")
        s.settimeout(SEND_TIMEOUT)   #发送阶段也要有超时，否则 sendall 可能永久阻塞
        try:
            s.sendall(encode_request(OP_GET, b"big"))
        except socket.timeout:
            print(f"  !! 发送超时（{SEND_TIMEOUT}s）")
            s.close()
            return 1
        s.shutdown(socket.SHUT_WR)

        # 让服务器读完请求并开始回写 —— 很快会撞上发送缓冲满（EAGAIN）
        time.sleep(0.8)

        # ---- 检查点 1：空闲窗口内服务器不应消耗 CPU ----
        c0 = cpu_seconds(proc.pid)
        time.sleep(IDLE_WINDOW)
        c1 = cpu_seconds(proc.pid)
        idle_cpu = None if (c0 is None or c1 is None) else (c1 - c0)
        if idle_cpu is None:
            print("  空闲窗口 CPU 时间 = 读取失败（/proc 不可用？）")
            ok_cpu = False
        else:
            print(f"  空闲窗口 {IDLE_WINDOW}s 内服务器 CPU 时间 = {idle_cpu:.2f}s"
                  f"（上限 {CPU_BUDGET}s）")
            ok_cpu = idle_cpu < CPU_BUDGET

        # ---- 读回复：先帧头，再内容，最后确认"收满之后才 EOF" ----
        deadline = time.time() + READ_TIMEOUT
        t0 = time.time()
        header = recv_exact(s, 4, deadline)
        got = b""
        reply_len = None
        ok_hdr = ok_len = ok_status = ok_content = eof_after_full = False

        if len(header) < 4:
            print(f"  只收到 {len(header)} 字节帧头就 EOF（服务器提前关了连接）")
        else:
            reply_len = struct.unpack(">I", header)[0]
            expect = 5 + len(payload)   #响应 payload = [1B status][4B vlen][value]
            ok_hdr = (reply_len == expect)
            print(f"  回复帧头声明长度 = {reply_len} 字节"
                  f"{'' if ok_hdr else f'（期望 {expect}）'}")

            got = recv_exact(s, reply_len if reply_len <= PAYLOAD_SIZE * 2 else expect, deadline)
            ok_len = (len(got) == expect)
            ok_status = (len(got) >= 5 and got[0] == ST_OK
                         and struct.unpack(">I", got[1:5])[0] == len(payload))
            ok_content = (got[5:] == payload)

            # 收满之后，服务器应当很快关闭连接 → 读到 EOF
            s.settimeout(10.0)
            try:
                eof_after_full = (s.recv(1) == b"")
            except socket.timeout:
                eof_after_full = False

        elapsed = time.time() - t0
        s.close()
        print(f"  收到 {len(got)}/{5 + len(payload)} 字节，用时 {elapsed:.2f}s，"
              f"收满后才 EOF = {eof_after_full}")

        if elapsed < 0.05:
            # 不是失败判据：8MB 远超"客户端收缓冲 + 服务器发送缓冲"的容量，
            # 所以在"客户端不读"的窗口里，必然有一部分数据只能排队在服务器自己的 out_ 里
            print("  （提示：收满得很快，但 8MB 仍超出两端缓冲容量 —— 说明确有数据曾排在服务器侧）")
        if ok_hdr and ok_len and ok_status and ok_content and eof_after_full and ok_cpu:
            print("[半关闭+大回复] PASS")
            return 0

        print("[半关闭+大回复] FAIL")
        if not ok_hdr:
            print("  原因：回复帧头声明的长度与请求不符")
        if not ok_len:
            print(f"  原因：回复长度不足（少 {5 + len(payload) - len(got)} 字节）"
                  " —— 待发数据被提前关闭丢掉了")
        if ok_len and not ok_status:
            print("  原因：状态或 vlen 字段不对")
        if ok_len and not ok_content:
            for i, (a, b) in enumerate(zip(got[5:], payload)):
                if a != b:
                    print(f"  原因：value 在第 {i} 字节不一致（收到 {a:#x}，期望 {b:#x}）")
                    break
        if not eof_after_full:
            print("  原因：收满回复后服务器没有关闭连接（或关闭得太慢）")
        if not ok_cpu:
            print("  原因：空闲窗口内在空转 —— 很可能是半关闭/EOF 状态仍留在 EPOLLIN 掩码里")
        print(f"  服务器日志：{SERVER_LOG}")
        return 1
    finally:
        stop_server(proc)


if __name__ == "__main__":
    sys.exit(main())
