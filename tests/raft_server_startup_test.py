#!/usr/bin/env python3
"""实际 kvserver 的 M7 入口：状态契约、信号退出、预留 fd 和后台失败传播。"""
import argparse
import os
from pathlib import Path
import resource
import signal
import socket
import struct
import subprocess
import time

from raft_worker_process_test import fixture_files, hard_state

ROOT = Path(__file__).resolve().parents[1]


class Server:
    """每个用例只启动自己持有的进程，数据和日志留在本轮专属目录。"""
    def __init__(self, binary, directory, fd_limit=None, fail_vote=False, overrides=None):
        directory.mkdir(parents=True)
        self.data = directory / "data"
        self.data.mkdir()
        self.log = directory / "server.log"
        self.legacy = directory / "must-not-open.data"
        if fail_vote:
            fixture_files(self.data, 0)
            (self.data / "raft.state.tmp").mkdir()
        leases = [socket.socket(), socket.socket()]
        for lease in leases:
            lease.bind(("127.0.0.1", 0))
        self.client_port, raft_port = [s.getsockname()[1] for s in leases]
        env = os.environ.copy()
        env.update(KV_PORT=str(self.client_port), KV_RAFT_ID="1",
                   KV_RAFT_PEERS=f"1@127.0.0.1:{raft_port}", KV_RAFT_DATA=str(self.data))
        for key, value in (overrides or {}).items():
            if value is None:
                env.pop(key, None)
            else:
                env[key] = value
        for lease in leases:
            lease.close()
        self.output = self.log.open("wb")
        limit = None if fd_limit is None else lambda: resource.setrlimit(
            resource.RLIMIT_NOFILE, (fd_limit, fd_limit))
        self.proc = subprocess.Popen([str(binary), str(self.legacy)], env=env,
                                     cwd=self.data, stdout=self.output,
                                     stderr=subprocess.STDOUT, preexec_fn=limit)

    def text(self):
        text = self.log.read_text(errors="replace")
        reports = ("ERROR: AddressSanitizer", "runtime error:", "LeakSanitizer",
                   "WARNING: ThreadSanitizer", "AddressSanitizer:DEADLYSIGNAL")
        if any(report in text for report in reports):
            raise AssertionError(f"sanitizer report in {self.log}: {text[-3000:]}")
        return text

    def ready(self):
        """验证当前 PID 的完整 READY 和新鲜状态行，不靠 connect 成功冒充就绪。"""
        deadline = time.monotonic() + 4
        last = None
        while time.monotonic() < deadline:
            text = self.text()
            for line in text.splitlines(keepends=True):
                if not line.endswith("\n"):
                    continue
                if not line.startswith("RAFTSTATUS "):
                    continue
                try:
                    fields = dict(item.split("=", 1) for item in line.split()[1:])
                    required = ("id", "pid", "term", "commit", "last", "applied", "serving", "running", "updated_ms")
                    row = {key: int(fields[key]) for key in required}
                    row["role"] = fields["role"]
                except (ValueError, KeyError) as error:
                    raise AssertionError(f"malformed status row: {line}") from error
                assert row["id"] == 1 and row["pid"] == self.proc.pid, row
                assert row["role"] in ("follower", "candidate", "leader"), row
                assert row["commit"] <= row["last"] and row["applied"] == 0, row
                last = row
            assert self.proc.poll() is None, f"startup exited: {text}"
            expected = f"READY id=1 pid={self.proc.pid}"
            if expected in text.splitlines() and last is not None:
                assert last["running"] == 1
                assert 0 <= time.monotonic_ns() // 1_000_000 - last["updated_ms"] < 1000
                return last
            time.sleep(0.01)
        raise AssertionError(f"missing current-boot READY/status: {self.text()}")

    def close(self):
        """异常收尾也只处理本用例保存的 PID，日志保留。"""
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=3)
        self.output.close()
        self.text()  #退出期的 sanitizer 报告也不能漏掉。


def receive(sock, count):
    """按长度接收协议字段，EOF 不能算作正常 Error 回复。"""
    data = bytearray()
    while len(data) < count:
        chunk = sock.recv(count - len(data))
        assert chunk, "unexpected EOF"
        data += chunk
    return bytes(data)


def status_case(binary, artifacts):
    """状态就绪后，Raft 模式 GET/SET 都须明确拒绝，不能落到旧单机库。"""
    server = Server(binary, artifacts / "status")
    try:
        server.ready()
        with socket.create_connection(("127.0.0.1", server.client_port), timeout=2) as sock:
            #单节点也要先等本任期屏障提交并执行完成
            deadline = time.monotonic() + 5
            while True:
                key, value = b"m8", b"committed"
                body = struct.pack(">BII", 1, len(key), len(value)) + key + value
                sock.sendall(struct.pack(">I", len(body)) + body)
                length, = struct.unpack(">I", receive(sock, 4))
                payload = receive(sock, length)
                assert length == 5 and payload[0] in (0, 2), payload
                if payload[0] == 0:
                    break
                assert time.monotonic() < deadline, "SET never succeeded"
                time.sleep(0.05)

            body = struct.pack(">BII", 2, len(key), 0) + key
            sock.sendall(struct.pack(">I", len(body)) + body)
            length, = struct.unpack(">I", receive(sock, 4))
            payload = receive(sock, length)
            assert payload[0] == 0, payload
            assert payload[5:] == value, payload
        assert not server.legacy.exists()
        print("PASS main: READY/current PID/fresh status and KV Error gate", flush=True)
    finally:
        server.close()


def idle_fd_case(binary, artifacts):
    """fd 预算不足时必须响亮退出，不能留下“活着但不服务”的半启动进程。

    M8 之后启动会额外打开 eventfd/派生KV 等 fd，具体耗尽点随实现变化，
    因此这里只钉住真正的不变量：非零退出、没有 READY、错误信息可见。
    """
    for limit in (6, 8, 10, 12):
        server = Server(binary, artifacts / f"idle-fd-{limit}", fd_limit=limit)
        try:
            rc = server.proc.wait(timeout=8)
            text = server.text()
            assert rc != 0, (limit, rc, text)
            assert "READY " not in text, (limit, text)
            assert text.strip(), (limit, "no diagnostic output")
        finally:
            server.close()
    print("PASS main: fd exhaustion exits loudly without READY (limits 6/8/10/12)", flush=True)


def signals_case(binary, artifacts):
    """两种退出信号都经 signalfd 进入主线程停止流程，要求退出码为 0。"""
    for sig in (signal.SIGTERM, signal.SIGINT):
        server = Server(binary, artifacts / sig.name)
        try:
            server.ready()
            server.proc.send_signal(sig)
            assert server.proc.wait(timeout=3) == 0, server.text()
            print(f"PASS main: {sig.name} clean exit", flush=True)
        finally:
            server.close()


def failure_case(binary, artifacts):
    """已有 term=1 的单节点启动成功后，自投票保存失败必须传播为非零退出。"""
    server = Server(binary, artifacts / "save-failure", fail_vote=True)
    try:
        server.ready()
        assert server.proc.wait(timeout=5) == 1, server.text()
        text = server.text()
        assert "raft worker unavailable" in text or "raft shutdown" in text, text
        for line in text.splitlines():
            if line.startswith("RAFTSTATUS "):
                fields = dict(item.split("=", 1) for item in line.split()[1:])
                assert not (fields["role"] == "leader" and fields["running"] == "1"), line
        assert hard_state(server.data / "raft.state") == {"term": 1, "vote": 0}
        assert not server.legacy.exists()
        print("PASS main: background state-save failure propagated; no usable leader", flush=True)
    finally:
        server.close()


def config_case(binary, artifacts):
    """端口和部分/空 Raft 环境配置都须在打开数据库之前明确失败。"""
    variants = [{"KV_PORT": value} for value in
                ("", "0", "65536", "-1", "+1", "9006x", " 9006", "999999999999")]
    for key in ("KV_RAFT_ID", "KV_RAFT_PEERS", "KV_RAFT_DATA"):
        variants.extend(({key: None}, {key: ""}))
    for index, overrides in enumerate(variants):
        server = Server(binary, artifacts / f"bad-config-{index:02d}", overrides=overrides)
        try:
            assert server.proc.wait(timeout=3) == 1, server.text()
            assert "READY " not in server.text()
            assert not server.legacy.exists()
            assert list(server.data.iterdir()) == [], (overrides, server.text())
        finally:
            server.close()
    print(f"PASS main: {len(variants)} invalid port/partial-config cases rejected before storage", flush=True)


def main():
    if not __debug__:
        raise RuntimeError("run acceptance without Python -O")
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, default=ROOT / "build/kvserver")
    parser.add_argument("--case", choices=("all", "status", "idle-fd", "signals", "failure", "config"), default="all")
    parser.add_argument("--skip-idle-fd", action="store_true",
                        help="tiny RLIMIT fixture is normal-only; sanitizer runtimes may own extra fds")
    parser.add_argument("--artifacts", type=Path,
                        default=ROOT / "build/test-logs" / f"m7-main-startup-{time.time_ns()}")
    args = parser.parse_args()
    artifacts = args.artifacts.resolve()
    artifacts.mkdir(parents=True, exist_ok=False)
    cases = {"status": status_case, "idle-fd": idle_fd_case,
             "signals": signals_case, "failure": failure_case, "config": config_case}
    print(f"main acceptance artifacts={artifacts}", flush=True)
    for name, case in cases.items():
        if name == "idle-fd" and args.skip_idle_fd:
            print("SKIP idle-fd: tiny fd-limit fixture is normal-only", flush=True)
            continue
        if args.case in ("all", name):
            case(args.binary.resolve(), artifacts)
    print("kvserver M7 startup PASS (no KV apply)", flush=True)


if __name__ == "__main__":
    main()
