#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
net_backpressure_test.py —— 背压回归测试（Step 0：先固化成"红"的用例）

【这个测试在验证什么】
    服务器发送缓冲写满时的完整行为链：
      写满 → write 返回 -1/EAGAIN → 挂起等待 EPOLLOUT → 客户端继续读
      → EPOLLOUT 触发 → 续写 → 最终整条消息完整送达

【为什么必须单独测这条路径】
    历史上这里藏着两个 bug（现在已确认可复现）：
      1) handle_write 里把 EAGAIN 判断写成 `n == 1` → 真实背压时直接关连接
      2) 关连接（delete c）后回调返回，process_frames 继续访问 c → Use After Free
    两个 bug 都只在"真的发生背压"时才触发。

    而之前那次验收是"假绿"：只把服务器的 SO_SNDBUF 调小，但客户端收缓冲是默认值
    （几十 KB 到 MB 级），loopback 上 ACK 立刻到达，发送缓冲一有数据就被搬走，
    EAGAIN 从来没发生过 —— 测的根本不是目标路径。

【如何制造确定的背压】
    网络里能容纳的"在途数据量" ≈ 客户端 SO_RCVBUF + 服务器 SO_SNDBUF
    只要消息体 > 这个容量，服务器必然写不完 → 必然 EAGAIN。
    所以本测试同时做三件事：
      · 客户端 SO_RCVBUF 压到 4096（内核会加倍，脚本里会打印实际值）
      · 服务器 SO_SNDBUF 压小（通过环境变量 KV_TEST_SNDBUF 传入，服务器支持则生效）
      · 消息体 8MB（远大于上面两者之和）
    另外客户端发送后故意 0.8 秒不读，确保缓冲区一定被填满。

【期望结果】
    PASS：客户端完整收到 8MB、内容逐字节一致、中途没有 EOF
    FAIL：收到一部分就 EOF（说明服务器提前关连接）

【当前状态（2026-09-15，Step 0 固化时）】
    红的 —— 实测只收到 1830912/8388608 字节就 EOF。这是预期，正是要修的 bug。

【用法】
    python3 tests/net_backpressure_test.py
    可用环境变量：
      KV_SERVER   服务器可执行文件路径（默认 build/kvserver）
      KV_PORT     端口（默认 9006）
"""
import os
import socket
import struct
import subprocess
import sys
import time

SERVER = os.environ.get("KV_SERVER", "build/kvserver")
PORT = int(os.environ.get("KV_PORT", "9006"))
SERVER_LOG = "/tmp/kv_backpressure_server.log"
PAYLOAD_SIZE = 8 * 1024 * 1024     # 8MB：远超 客户端收缓冲 + 服务器发缓冲
SEND_BLOCK_TIME = 0.8              # 客户端发送后故意不读的时长（秒）
READ_TIMEOUT = 60.0                # 读取总超时（秒）


def frame(payload: bytes) -> bytes:
    """按协议打包一条消息：[4 字节大端长度][内容]"""
    return struct.pack(">I", len(payload)) + payload


def start_server():
    """启动自己的服务器实例，等端口可连接后返回进程对象。

    如果端口已被占用（例如你手动开着的 kvserver），新进程会 bind 失败退出，
    这里会打印服务器日志并直接失败 —— 不要复用未知的服务器实例，
    否则测的可能是旧二进制。
    """
    log = open(SERVER_LOG, "wb")
    env = os.environ.copy()
    env.setdefault("KV_TEST_SNDBUF", "4096")   # 服务器支持时生效；不支持也无害
    proc = subprocess.Popen([SERVER], stdout=log, stderr=subprocess.STDOUT,
                            env=env, cwd=os.getcwd())
    deadline = time.time() + 5.0
    while time.time() < deadline:
        if proc.poll() is not None:            # 进程已退出 → 启动失败
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
    print("!! 提示：端口 9006 可能被占用，先确认没有别的 kvserver 在跑")
    sys.exit(2)


def stop_server(proc):
    """收尾：先 TERM，超时再 KILL，避免测试留下后台进程。"""
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=3)


def main() -> int:
    payload = bytes(range(256)) * (PAYLOAD_SIZE // 256)   # 有规律的 8MB，便于定位差异

    print(f"[背压测试] 启动服务器 {SERVER}（KV_TEST_SNDBUF=4096）")
    proc = start_server()
    try:
        # ---- 建连接：先把收缓冲压小，再 connect（窗口大小在握手时就要小） ----
        s = socket.socket()
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        s.connect(("127.0.0.1", PORT))
        real_rcvbuf = s.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        # 内核会把设置值加倍（4KB → 8KB），打印实际值供诊断
        print(f"  客户端 SO_RCVBUF 实际值 = {real_rcvbuf} 字节")
        print(f"  发送单条 {len(payload)} 字节的消息（协议：4 字节长度头 + 内容）")

        t0 = time.time()
        s.sendall(frame(payload))
        print(f"  发送完成，故意 {SEND_BLOCK_TIME}s 不读，制造背压…")
        time.sleep(SEND_BLOCK_TIME)

        # ---- 开始读：慢速分块，给服务器制造持续压力 ----
        # 注意：服务器回的是同样带长度头的帧 [4 字节大端长度][内容]，
        # 必须先精确吃掉 4 字节帧头，再读内容，否则等于拿帧头跟内容比对。
        got = bytearray()
        early_eof = False

        def recv_exact(n: int) -> bytes:
            """精确读 n 字节；提前关闭/超时时返回已读到的部分（可能为空）"""
            buf = bytearray()
            while len(buf) < n:
                if time.time() - t0 > READ_TIMEOUT:
                    print("  !! 读取超时")
                    return bytes(buf)
                chunk = s.recv(n - len(buf))
                if not chunk:
                    return bytes(buf)              # 对端提前关闭
                buf += chunk
            return bytes(buf)

        header = recv_exact(4)
        if len(header) < 4:
            early_eof = True                   # 连帧头都没读全：服务器提前关了连接
            print(f"  只收到 {len(header)} 字节帧头就 EOF")
        else:
            reply_len = struct.unpack(">I", header)[0]
            print(f"  回复帧头声明长度 = {reply_len} 字节")
            if reply_len != len(payload):
                print(f"  !! 回复长度和请求不一致（期望 {len(payload)}）")
            # 防御：长度头离谱时不去读它
            target = reply_len if 0 < reply_len <= 64 * 1024 * 1024 else len(payload)
            while len(got) < target:
                if time.time() - t0 > READ_TIMEOUT:
                    print("  !! 读取超时")
                    break
                chunk = s.recv(4096)
                if not chunk:
                    early_eof = True           # 对端提前关闭：当前 bug 的症状
                    break
                got += chunk
                time.sleep(0.001)              # 1ms/4KB ≈ 4MB/s，慢于服务器写入

        elapsed = time.time() - t0
        s.close()

        # ---- 判定 ----
        ok_len = (len(got) == len(payload))
        ok_content = (bytes(got) == payload)
        ok_no_eof = not early_eof

        print(f"  收到 {len(got)}/{len(payload)} 字节，用时 {elapsed:.2f}s，"
              f"提前 EOF = {early_eof}")

        if ok_len and ok_content and ok_no_eof:
            print("[背压测试] PASS")
            return 0

        print("[背压测试] FAIL")
        if not ok_len:
            print(f"  原因：长度不足（少 {len(payload) - len(got)} 字节）")
        if early_eof:
            print("  原因：服务器在响应发完之前就关闭了连接 —— "
                  "典型症状是 handle_write 的 EAGAIN 分支走进了错误处理")
        if ok_len and not ok_content:
            for i, (a, b) in enumerate(zip(bytes(got), payload)):
                if a != b:
                    print(f"  原因：内容在第 {i} 字节不一致（收到 {a:#x}，期望 {b:#x}）")
                    break
        print(f"  服务器日志：{SERVER_LOG}")
        return 1
    finally:
        stop_server(proc)


if __name__ == "__main__":
    sys.exit(main())
