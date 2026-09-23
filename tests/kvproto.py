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
    """启动服务器并等端口可用；返回 Popen（端口没起来则抛 RuntimeError）"""
    log = open(log_path, "wb")
    child_env = os.environ.copy()
    if env:
        child_env.update(env)
    proc = subprocess.Popen([binary, db_path], stdout=log, stderr=subprocess.STDOUT,
                            env=child_env)
    if not wait_port(port):
        proc.kill()
        proc.wait(timeout=5)
        raise RuntimeError(f"服务器未在超时内监听 {port}（日志: {log_path}）")
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
