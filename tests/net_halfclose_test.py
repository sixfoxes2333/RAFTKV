#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
net_halfclose_test.py —— 半关闭（half-close）回归测试

【这个测试在验证什么】
    客户端发完请求后调用 shutdown(SHUT_WR)（半关闭：我不再发数据，但还要收回复），
    服务器必须：
      1) 把已到达的请求全部读完、全部处理
      2) 把所有响应发完
      3) 之后才关闭连接
    客户端应当收到全部回复，然后才看到 EOF。

【为什么这条重要】
    · kvclient 就是这么工作的：发完参数 → shutdown(SHUT_WR) → 等回复
    · 一旦服务器把"对端半关闭"当成致命错误（EPOLLRDHUP 直接 close），
      还没发出去的响应就会丢 —— 这是半关闭语义的经典坑
    · 即使现在没引入 EPOLLRDHUP，这条也要作为长期回归用例留着

【期望结果】
    PASS：三条回复按顺序、内容正确，且在回复收完之后才 EOF
    当前状态（2026-09-15）：绿（小消息场景下路径正确，作为回归保护）

【用法】
    python3 tests/net_halfclose_test.py
    可用环境变量：KV_SERVER / KV_PORT（同背压测试）
"""
import os
import socket
import struct
import subprocess
import sys
import time

SERVER = os.environ.get("KV_SERVER", "build/kvserver")
PORT = int(os.environ.get("KV_PORT", "9006"))
SERVER_LOG = "/tmp/kv_halfclose_server.log"


def frame(payload: bytes) -> bytes:
    return struct.pack(">I", len(payload)) + payload


def start_server():
    """启动自己的服务器实例（与背压测试同样的就绪探测逻辑）。"""
    log = open(SERVER_LOG, "wb")
    proc = subprocess.Popen([SERVER], stdout=log, stderr=subprocess.STDOUT,
                            cwd=os.getcwd())
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
    sys.exit(2)


def stop_server(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=3)


def recv_frames(sock, want: int):
    """按长度头切出 want 条完整回复。客户端同样要处理半包，所以这里必须循环。"""
    buf = b""
    frames = []
    while len(frames) < want:
        while len(buf) < 4:                       # 先凑齐 4 字节长度头
            chunk = sock.recv(4096)
            if not chunk:
                return frames, False              # 提前 EOF
            buf += chunk
        n = struct.unpack(">I", buf[:4])[0]       # 大端长度
        while len(buf) < 4 + n:                   # 再凑齐内容
            chunk = sock.recv(4096)
            if not chunk:
                return frames, False
            buf += chunk
        frames.append(buf[4:4 + n])
        buf = buf[4 + n:]                         # 粘包：可能还有下一条，循环继续
    return frames, True


def main() -> int:
    msgs = [b"hello", b"world", b"half-close!"]

    print(f"[半关闭测试] 启动服务器 {SERVER}")
    proc = start_server()
    try:
        s = socket.create_connection(("127.0.0.1", PORT))
        for m in msgs:                            # 1) 发三条请求
            s.sendall(frame(m))

        s.shutdown(socket.SHUT_WR)                # 2) 半关闭：我不再发了
        print("  已发送 3 条请求并 shutdown(SHUT_WR)，等待回复…")

        got, complete = recv_frames(s, len(msgs))

        # 3) 回复收完后，服务器应该关闭连接：再读一次应当是 EOF
        tail = s.recv(1)
        s.close()

        ok = True
        if not complete or len(got) != len(msgs):
            print(f"  FAIL：期望 {len(msgs)} 条回复，实际收到 {len(got)} 条")
            ok = False
        for i, (g, w) in enumerate(zip(got, msgs)):
            if g != w:
                print(f"  FAIL：第 {i} 条回复不一致（收到 {g!r}，期望 {w!r}）")
                ok = False
        if tail != b"":
            print(f"  FAIL：回复收完后仍能读到数据 {tail!r}，服务器没有关闭连接")
            ok = False

        if ok:
            print(f"  收到 {len(got)} 条回复，内容全部一致，服务器随后关闭连接")
            print("[半关闭测试] PASS")
            return 0
        print(f"  服务器日志：{SERVER_LOG}")
        print("[半关闭测试] FAIL")
        return 1
    finally:
        stop_server(proc)


if __name__ == "__main__":
    sys.exit(main())
