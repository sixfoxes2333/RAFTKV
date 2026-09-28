"""kvproto.py —— KV 协议（framing + 编解码）的 Python 实现，测试共用

线上格式（全部大端）：
    请求 payload = [1B op][4B key_len][4B val_len][key][value]
    响应 payload = [1B status][4B val_len][value]
    两者外面都再套一层 framing：[4B 帧长][payload]

op:     SET=1  GET=2  DEL=3
status: Ok=0  NotFound=1  Error=2
"""

import os
import socket
import struct
import subprocess
import time

PORT = int(os.environ.get("KV_PORT", "9006"))
BINARY = os.environ.get("KV_SERVER", "./build/kvserver")
MAX_MESSAGE = 16 * 1024 * 1024

OP_SET, OP_GET, OP_DEL = 1, 2, 3
ST_OK, ST_NOTFOUND, ST_ERROR = 0, 1, 2


# ---------- 编解码 ----------

def frame(payload: bytes) -> bytes:
    """加 framing 长度头"""
    return struct.pack(">I", len(payload)) + payload


def encode_request(op: int, key: bytes, value: bytes = b"") -> bytes:
    body = struct.pack(">BII", op, len(key), len(value)) + key + value
    return frame(body)


def decode_response(body: bytes):
    """返回 (status, value)；长度不合法抛 ValueError"""
    if len(body) < 5:
        raise ValueError(f"响应太短: {len(body)} 字节")
    status = body[0]
    vlen = struct.unpack(">I", body[1:5])[0]
    if len(body) != 5 + vlen:
        raise ValueError(f"响应长度不符: 声明 {vlen}，实际 {len(body) - 5}")
    return status, body[5:]


# ---------- 收发 ----------

def recv_exact(sock: socket.socket, n: int, timeout: float = 5.0) -> bytes:
    """收满 n 字节；对端提前关闭返回 None；超时抛 socket.timeout"""
    buf = b""
    sock.settimeout(timeout)
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def recv_frame(sock: socket.socket, timeout: float = 5.0):
    """读一个完整帧，返回 payload；对端关闭返回 None"""
    header = recv_exact(sock, 4, timeout)
    if header is None:
        return None
    length = struct.unpack(">I", header)[0]
    if length > MAX_MESSAGE:
        raise ValueError(f"帧长度超过上限: {length}")
    return recv_exact(sock, length, timeout)


def request(sock: socket.socket, op: int, key: bytes, value: bytes = b"", timeout: float = 5.0):
    """发一个请求并读回 (status, value)；连接关闭返回 (None, None)"""
    sock.sendall(encode_request(op, key, value))
    body = recv_frame(sock, timeout)
    if body is None:
        return None, None
    return decode_response(body)


def connect(port: int = PORT, timeout: float = 5.0) -> socket.socket:
    s = socket.create_connection(("127.0.0.1", port), timeout)
    s.settimeout(timeout)
    return s


# ---------- 服务器进程 ----------

def wait_port(port: int = PORT, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.create_connection(("127.0.0.1", port), 0.5)
            s.close()
            return True
        except OSError:
            time.sleep(0.05)
    return False


def start_server(db_path: str, log_path: str, port: int = PORT, binary: str = BINARY, env: dict = None):
    """启动服务器并等它就绪；返回 Popen

    没起来 / 起来后又退出 / 端口后面不是我们的服务 → 抛 RuntimeError。
    日志用【追加】而不是覆盖：一个测试里重启多次时，前面实例的日志不能被冲掉
    （否则 ASan/TSan 只会检查到最后一次实例的输出）。run_all.sh/run_asan.sh/run_tsan.sh
    在每轮开始前会清一次相关日志。
    """
    log = open(log_path, "ab")
    child_env = os.environ.copy()
    if env:
        child_env.update(env)
    proc = subprocess.Popen([binary, db_path], stdout=log, stderr=subprocess.STDOUT,
                            env=child_env)
    log.write(f"\n===== start_server pid={proc.pid} binary={binary} "
              f"at {time.strftime('%H:%M:%S')} =====\n".encode())
    log.flush()

    if not wait_port(port):
        proc.kill()
        proc.wait(timeout=5)
        raise RuntimeError(f"服务器未在超时内监听 {port}（日志: {log_path}）")

    # 端口开着 != "我们启动的那个进程"在服务：
    #   · 可能是别的进程占着这个端口（我们的进程其实已经退出了）
    #   · 可能我们的进程还活着但其实没在服务
    if proc.poll() is not None:
        raise RuntimeError(f"{binary} 启动后立刻退出（rc={proc.returncode}，日志: {log_path}）")

    # 握一手：端口后面必须是一个会说我们协议的服务器（只读探测，不写数据）
    try:
        s = connect(port)
        st, _ = request(s, OP_GET, b"__startup_probe__", timeout=5.0)
        s.close()
    except Exception as e:                       # noqa: BLE001
        proc.kill()
        proc.wait(timeout=5)
        raise RuntimeError(f"{port} 后面的服务没有按 KV 协议应答（{type(e).__name__}: {e}）")

    if st not in (ST_OK, ST_NOTFOUND):
        proc.kill()
        proc.wait(timeout=5)
        raise RuntimeError(f"{port} 后面的服务返回异常状态 status={st}（不是我们的 KV 服务器？）")

    return proc


def stop_server(proc, hard: bool = False):
    """hard=True 等价于 kill -9（不跑任何析构/收尾路径）"""
    if proc is None or proc.poll() is not None:
        return
    if hard:
        proc.kill()          # SIGKILL
    else:
        proc.terminate()     # SIGTERM
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def remove_db(db_path: str):
    for suffix in ("", ".tmp"):
        p = db_path + suffix
        if os.path.exists(p):
            os.remove(p)
