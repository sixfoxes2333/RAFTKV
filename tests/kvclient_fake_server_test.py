#!/usr/bin/env python3
"""kvclient_fake_server_test.py —— 客户端必须校验帧长（修复 #6）

【要证明什么】
kvclient 的 recv_frame() 读到 4 字节长度头之后直接 `payload.resize(len)`，**没有上限检查**。
对端只要声明一个巨大的长度（假服务器 / 中间人 / 损坏的对端都能做到），客户端就会：
  · len = 0xFFFFFFFF（4GB）→ resize 时申请 + 清零 4GB：撑不住就 std::bad_alloc → std::terminate（SIGABRT），
    撑得住（实测这台机器就是这样）就一直阻塞在 read 上等那 4GB
  · len = MAX_MESSAGE+1 → 同样先把内存要下来，再阻塞等一个永远不会来的身体
  → 两种都是"客户端无声无息地挂住"（比崩溃更难排查），服务器端有这道检查，客户端漏了


【判据】
  1) 巨大长度（0xFFFFFFFF）：客户端必须**快速退出**（不许崩溃、不许挂住），退出码 2，
     并且 stderr 里有明确的"帧太大"信息
  2) MAX_MESSAGE+1（对端只发头、不发身体、也不关闭）：同样必须快速退出，不许挂住
  3) 正常小响应：客户端必须照常打印 value 并返回 0（围栏不能把正常路径搞坏）

【用法】python3 tests/kvclient_fake_server_test.py
"""

import os
import socket
import struct
import subprocess
import sys
import threading
import time

BIN = os.environ.get("KV_CLIENT", "./build/kvclient")
MAX_MESSAGE = 16 * 1024 * 1024      # 与 src/base/limits.h 的 kv::MAX_MESSAGE 对应
TIMEOUT = 5.0                        # 客户端应该在这个时间内退出

fails = []


def check(label, cond, detail=""):
    print(f"  {'ok   ' if cond else 'FAIL '}: {label}" + (f"   {detail}" if detail and not cond else ""))
    if not cond:
        fails.append(label)


def frame(payload: bytes) -> bytes:
    """正常的响应帧：4 字节大端长度 + payload"""
    return struct.pack(">I", len(payload)) + payload


def fake_server(header: bytes, keep_open: bool):
    """起一个假服务器：接受一条连接，回 header 字节；keep_open 时保持连接不关、不发更多数据。
       返回它监听的端口"""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]

    def loop():
        try:
            conn, _ = srv.accept()
            try:
                conn.recv(1 << 16)      # 把请求收掉（客户端发完会 shutdown 写端）
                if header:
                    conn.sendall(header)
                if keep_open:
                    time.sleep(30)      # 不发身体、也不关闭 → 逼客户端自己拒绝
            finally:
                try:
                    conn.close()
                except OSError:
                    pass
        except OSError:
            pass
        finally:
            srv.close()

    threading.Thread(target=loop, daemon=True).start()
    return port


def run_client(port: int):
    env = os.environ.copy()
    env["KV_HOST"] = "127.0.0.1"
    env["KV_PORT"] = str(port)
    t0 = time.time()
    try:
        r = subprocess.run([BIN, "get", "foo"], capture_output=True, env=env, timeout=TIMEOUT)
        return r.returncode, r.stdout, r.stderr.decode(errors="replace"), time.time() - t0, False
    except subprocess.TimeoutExpired as e:
        return None, e.stdout or b"", (e.stderr or b"").decode(errors="replace"), time.time() - t0, True


def main() -> int:
    if not os.path.exists(BIN):
        print(f"!! 找不到 {BIN}")
        return 2

    # ---------- 1) 巨大长度 + 对端保持沉默 ----------
    for name, declared in [("0xFFFFFFFF（4GB）", 0xFFFFFFFF),
                           (f"MAX_MESSAGE+1（{MAX_MESSAGE + 1}）", MAX_MESSAGE + 1)]:
        print(f"\n[1] 假服务器声明长度 = {name}，只发长度头，不发身体、也不关连接")
        port = fake_server(struct.pack(">I", declared), keep_open=True)
        rc, out, err, dt, timed_out = run_client(port)
        print(f"  退出码={rc} 用时={dt:.2f}s" + ("（超时，被测试杀掉）" if timed_out else "")
              + f"  stderr={err.strip()[:90]!r}")

        check(f"{name}：客户端没有崩溃（退出码非负）", rc is not None and rc >= 0,
              f"退出码={rc}（负数 = 被信号杀死，说明分配/清零了巨大内存）")
        check(f"{name}：客户端快速退出（不许挂住）", not timed_out, f"超过 {TIMEOUT:.0f}s 还在等数据")
        check(f"{name}：退出码 = 2", rc == 2, f"rc={rc}")
        check(f"{name}：stderr 说明是帧长问题", "too large" in err or "帧" in err,
              f"stderr={err.strip()[:90]!r}")

    # ---------- 2) 正常响应必须照旧工作 ----------
    print("\n[2] 正常小响应（value = hello）")
    body = bytes([0]) + struct.pack(">I", 5) + b"hello"   # [status=Ok][vlen=5][value]
    port = fake_server(frame(body), keep_open=False)
    rc, out, err, dt, timed_out = run_client(port)
    print(f"  退出码={rc} 用时={dt:.2f}s stdout={out!r}")
    check("正常响应：退出码 = 0 且打印出 value", rc == 0 and b"hello" in out,
          f"rc={rc} stdout={out!r} stderr={err.strip()[:80]!r}")

    print()
    if fails:
        print(f"[客户端帧长] FAIL —— {len(fails)} 项失败：{fails}")
        print("  → 修复方向：recv_frame 里 resize 之前先检查长度上限（> kv::MAX_MESSAGE 直接拒绝），")
        print("           并把错误信息说清楚；client.cpp 需要 include base/limits.h")
        return 1
    print("[客户端帧长] PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
