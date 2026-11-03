#!/usr/bin/env python3
"""M7 实际 Worker 进程：选举/心跳/kill -9 重选/同目录重启/130 条追平。

测试驱动只创建独立夹具、观察状态和控制自己启动的 PID；不替 Core 投票或提交。
不是 kvserver 客户端 KV 验收。
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import struct
import subprocess
import tempfile
import time
import zlib

ROOT = Path(__file__).resolve().parents[1]


def hard_state(path):
    """独立解码正式状态文件，检查 CRC 后再交出 term/vote。"""
    data = path.read_bytes()
    assert len(data) == 28 and data[:4] == b"RFST", str(path)
    assert struct.unpack_from(">I", data, 4)[0] == 1
    assert zlib.crc32(data[:24]) & 0xffffffff == struct.unpack_from(">I", data, 24)[0]
    return dict(zip(("term", "vote"), struct.unpack_from(">QQ", data, 8)))


def fixture_files(directory, count):
    """生成私有旧日志夹具；内容固定，Node 3 可从空日志通过真实 TCP 追平。"""
    head = struct.pack(">4sIQQ", b"RFST", 1, 1, 0)
    state = head + struct.pack(">I", zlib.crc32(head) & 0xffffffff)
    log = bytearray()
    for index in range(1, count + 1):
        command = b"fixture:" + struct.pack(">I", index) + b"\x00\xff"
        header = struct.pack(">4sIQQI", b"RLOG", 1, index, 1, len(command))
        log += header + struct.pack(">I", zlib.crc32(header) & 0xffffffff)
        log += command + struct.pack(">I", zlib.crc32(command) & 0xffffffff)
    for name, content in (("raft.state", state), ("raft.log", log)):
        with (directory / name).open("wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return bytes(log)


class Node:
    """每次启动有独立日志文件，状态必须同时匹配当前 PID 和新鲜采样时间。"""
    def __init__(self, cluster, node_id):
        self.cluster = cluster
        self.id = node_id
        self.directory = cluster.work / str(node_id)
        self.directory.mkdir()
        self.proc = None
        self.output = None
        self.log = None
        self.boot = 0
        self.expected_alive = False
        self.seen_term = 0
        self.seen_commit = 0
        self.last_row = None

    def launch_spec(self):
        """默认启动实际 Worker 驱动；kvserver 用同一观测器覆盖启动参数。"""
        return [str(self.cluster.binary), str(self.id), self.cluster.peers, str(self.directory)], None

    def start(self):
        self.cluster.release_port(self.id)
        self.boot += 1
        self.log = self.cluster.artifacts / f"node{self.id}-boot{self.boot}.log"
        self.output = self.log.open("wb")
        self.started_ms = time.monotonic_ns() // 1_000_000
        command, env = self.launch_spec()
        self.proc = subprocess.Popen(
            command, env=env, cwd=self.directory,
            stdout=self.output, stderr=subprocess.STDOUT,
        )
        self.expected_alive = True
        self.seen_commit = 0
        self.last_row = None

    def sample(self):
        if not self.expected_alive:
            return None
        text = self.log.read_text(errors="replace")
        reports = ("ERROR: AddressSanitizer", "runtime error:",
                   "LeakSanitizer", "WARNING: ThreadSanitizer")
        if any(report in text for report in reports):
            raise AssertionError(f"sanitizer report in node {self.id}: {self.log}\n{text[-3500:]}")
        if self.proc.poll() is not None:
            raise AssertionError(f"node {self.id} exited {self.proc.returncode}: {text[-2500:]}")
        ready = f"READY id={self.id} pid={self.proc.pid}"
        if ready not in text.splitlines():
            return None
        row = None
        required = {"id", "pid", "role", "term", "commit", "last", "running", "updated_ms"}
        for line in text.splitlines():
            if not line.startswith("RAFTSTATUS "):
                continue
            try:
                fields = dict(item.split("=", 1) for item in line.split()[1:])
                if not required <= fields.keys():
                    continue
                candidate = {key: (value if key == "role" else int(value))
                             for key, value in fields.items() if key in required}
            except (ValueError, TypeError):
                continue
            if candidate["id"] == self.id and candidate["pid"] == self.proc.pid:
                row = candidate
        if row is None:
            return None
        if not row["running"]:
            raise AssertionError(f"node {self.id} stopped Worker: {text[-2500:]}")
        now_ms = time.monotonic_ns() // 1_000_000
        if row["updated_ms"] < self.started_ms - 50 or now_ms - row["updated_ms"] > 1000:
            return None
        assert row["term"] >= self.seen_term, ("term regression", self.last_row, row)
        assert row["commit"] >= self.seen_commit and row["commit"] <= row["last"], row
        self.seen_term = row["term"]
        self.seen_commit = row["commit"]
        self.last_row = row
        return row

    def kill(self):
        """仅杀自己保存的进程对象，不按进程名称清理。"""
        self.expected_alive = False
        self.proc.kill()
        self.proc.wait(timeout=5)
        self.output.close()

    def close(self):
        self.expected_alive = False
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=4)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=4)
        if self.output is not None:
            self.output.close()


class Cluster:
    """预留三个互不重复的端口，启动节点时才释放对应监听端口。"""
    node_class = Node
    def __init__(self, binary, work, artifacts):
        self.binary = binary
        self.work = work
        self.artifacts = artifacts
        work.mkdir()
        artifacts.mkdir()
        self.reservations = {}
        self.ports = {}
        for node_id in (1, 2, 3):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            self.reservations[node_id] = sock
            self.ports[node_id] = sock.getsockname()[1]
        self.peers = ",".join(f"{i}@127.0.0.1:{self.ports[i]}" for i in (1, 2, 3))
        self.nodes = {i: self.node_class(self, i) for i in (1, 2, 3)}

    def release_port(self, node_id):
        sock = self.reservations.pop(node_id, None)
        if sock:
            sock.close()

    def start(self):
        for node in self.nodes.values():
            node.start()

    def rows(self):
        live = [n for n in self.nodes.values() if n.expected_alive]
        rows = [n.sample() for n in live]
        return None if any(row is None for row in rows) else rows

    def stable_leader(self, after_term=-1, last=None, timeout=15):
        """所有存活节点同任期且只一名 leader，持续观察 600ms。"""
        deadline = time.monotonic() + timeout
        key = None
        since = None
        while time.monotonic() < deadline:
            rows = self.rows()
            leaders = [r for r in (rows or []) if r["role"] == "leader"]
            good = (rows and len(rows) >= 2 and len(leaders) == 1 and
                    len({r["term"] for r in rows}) == 1 and
                    all(r["role"] in ("follower", "leader") for r in rows) and
                    leaders[0]["term"] > after_term and
                    (last is None or all(r["last"] == last for r in rows)))
            if good:
                current = (leaders[0]["id"], leaders[0]["term"])
                if current != key:
                    key, since = current, time.monotonic()
                if time.monotonic() - since >= 0.6:
                    return leaders[0], rows
            else:
                key, since = None, None
            time.sleep(0.04)
        raise AssertionError(f"no stable fresh leader; last rows={[n.last_row for n in self.nodes.values()]}")

    def votes(self, leader):
        """胜选的多数派选票必须实际保存在磁盘上。"""
        states = {i: hard_state(n.directory / "raft.state")
                  for i, n in self.nodes.items() if n.expected_alive}
        votes = sum(s["term"] == leader["term"] and s["vote"] == leader["id"]
                    for s in states.values())
        assert votes >= 2, (leader, states)
        return states

    def close(self):
        for node in self.nodes.values():
            node.close()
        for sock in self.reservations.values():
            sock.close()
        self.reservations.clear()


def fresh_election_kill_restart(binary, work, artifacts, cluster_type=Cluster):
    """全新三进程先稳定选主，心跳维持任期，再 kill -9 重选和原目录重启。"""
    cluster = cluster_type(binary, work, artifacts)
    evidence = {}
    try:
        cluster.start()
        leader, rows = cluster.stable_leader(last=0)
        evidence["first"] = dict(leader=leader, rows=rows, states=cluster.votes(leader))
        until = time.monotonic() + 2.4  # 超过当前最大选举超时；期间持续检查状态。
        while time.monotonic() < until:
            current = cluster.rows()
            assert current is not None, "stale/missing status during heartbeat window"
            assert all(r["term"] == leader["term"] for r in current), current
            assert [r["id"] for r in current if r["role"] == "leader"] == [leader["id"]], current
            time.sleep(0.04)
        print(f"PASS processes: 3 live PIDs elected node {leader['id']} term {leader['term']}; heartbeat stable", flush=True)

        killed = cluster.nodes[leader["id"]]
        old_pid = killed.proc.pid
        killed.kill()
        saved = hard_state(killed.directory / "raft.state")
        next_leader, rows = cluster.stable_leader(after_term=leader["term"], last=0)
        assert next_leader["id"] != killed.id
        evidence["after_kill"] = dict(killed_pid=old_pid, leader=next_leader,
                                      rows=rows, states=cluster.votes(next_leader))
        print(f"PASS processes: SIGKILL pid {old_pid}; node {next_leader['id']} elected term {next_leader['term']}", flush=True)

        killed.start()
        joined, rows = cluster.stable_leader(after_term=leader["term"], last=0)
        restarted = next(r for r in rows if r["id"] == killed.id)
        assert restarted["pid"] != old_pid and restarted["term"] >= saved["term"]
        evidence["restart"] = dict(leader=joined, rows=rows, saved_before_restart=saved,
                                   states=cluster.votes(joined))
        print("PASS processes: same data directory restarted with a new PID and fresh monotonic state", flush=True)
        return evidence
    finally:
        cluster.close()


def old_log_catchup(binary, work, artifacts, cluster_type=Cluster):
    """两个节点保存任期 1 的 130 条旧日志，空日志节点通过实际 Worker 追平。"""
    cluster = cluster_type(binary, work, artifacts)
    try:
        expected = fixture_files(cluster.nodes[1].directory, 130)
        assert fixture_files(cluster.nodes[2].directory, 130) == expected
        fixture_files(cluster.nodes[3].directory, 0)
        cluster.start()
        leader, rows = cluster.stable_leader(last=130, timeout=20)
        assert leader["term"] > 1, leader
        assert all(r["commit"] == 0 for r in rows), rows
        for node in cluster.nodes.values():
            assert (node.directory / "raft.log").read_bytes() == expected, node.id
        states = cluster.votes(leader)
        print("PASS processes: empty follower caught up 130 durable entries; old-term majority stayed uncommitted", flush=True)
        return dict(leader=leader, rows=rows, states=states, records=130)
    finally:
        cluster.close()


def main():
    if not __debug__:
        raise RuntimeError("run process acceptance without Python -O")
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path,
                        default=Path(os.environ.get("KV_RAFT_WORKER_NODE", ROOT / "build/raft_worker_node")))
    parser.add_argument("--artifacts", type=Path,
                        default=ROOT / "build/test-logs" / f"m7-worker-process-{time.time_ns()}")
    args = parser.parse_args()
    binary = args.binary.resolve()
    assert binary.is_file(), f"build actual raft_worker_node first: {binary}"
    artifacts = args.artifacts.resolve()
    artifacts.mkdir(parents=True, exist_ok=False)
    work = Path(tempfile.mkdtemp(prefix="raft-worker-process."))
    success = False
    try:
        evidence = {"binary": str(binary), "fixtures": str(work)}
        evidence["fresh"] = fresh_election_kill_restart(binary, work / "fresh", artifacts / "fresh")
        evidence["catchup"] = old_log_catchup(binary, work / "catchup", artifacts / "catchup")
        (artifacts / "evidence.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
        success = True
        print(f"raft worker process PASS (actual Worker processes; not kvserver/KV); artifacts={artifacts}")
    finally:
        if success:
            shutil.rmtree(work)
        else:
            shutil.copytree(work, artifacts / "failed-data")
            print(f"FAILED evidence retained: fixtures={work} artifacts={artifacts}", flush=True)


if __name__ == "__main__":
    main()
