#!/usr/bin/env python3
"""M7 实际 kvserver 三进程入口验收；共用 Worker 的 PID/新鲜状态/磁盘观察器。"""
import argparse
import json
import os
from pathlib import Path
import socket
import struct
import time

import raft_worker_process_test as worker
from raft_server_startup_test import receive

ROOT = Path(__file__).resolve().parents[1]


class ServerNode(worker.Node):
    """用真正 kvserver 的环境配置启动，状态观察和进程控制仍沿用同一套规则。"""
    def __init__(self, cluster, node_id):
        super().__init__(cluster, node_id)
        self.checked = set()

    def launch_spec(self):
        lease = self.cluster.client_leases.pop(self.id, None)
        if lease is not None:
            lease.close()
        env = os.environ.copy()
        env.update(KV_RAFT_ID=str(self.id), KV_RAFT_PEERS=self.cluster.peers,
                   KV_RAFT_DATA=str(self.directory),
                   KV_PORT=str(self.cluster.client_ports[self.id]))
        return [str(self.cluster.binary), str(self.directory / "must-not-open.data")], env

    def sample(self):
        """新启动或角色变化时都验证 GET/SET 不绕过 Raft 写入旧单机库。"""
        row = super().sample()
        if row is None:
            return None
        key = (self.boot, row["role"])
        if key not in self.checked:
            with socket.create_connection(
                    ("127.0.0.1", self.cluster.client_ports[self.id]), timeout=2) as sock:
                for op, value in ((1, b"not-a-local-write"), (2, b"")):
                    name = b"m7-cluster"
                    body = struct.pack(">BII", op, len(name), len(value)) + name + value
                    sock.sendall(struct.pack(">I", len(body)) + body)
                    length, = struct.unpack(">I", receive(sock, 4))
                    assert length == 5 and receive(sock, length) == b"\x02\x00\x00\x00\x00"
            assert not (self.directory / "must-not-open.data").exists()
            assert not (self.directory / "kvdb.data").exists()
            self.checked.add(key)
        return row


class ServerCluster(worker.Cluster):
    """Raft 端口之外再预留三个客户端端口，共六个互不重复的端口。"""
    node_class = ServerNode

    def __init__(self, binary, work, artifacts):
        super().__init__(binary, work, artifacts)
        self.client_leases = {}
        self.client_ports = {}
        try:
            for node_id in (1, 2, 3):
                lease = socket.socket()
                lease.bind(("127.0.0.1", 0))
                self.client_leases[node_id] = lease
                self.client_ports[node_id] = lease.getsockname()[1]
        except Exception:
            self.close()
            raise

    def close(self):
        try:
            super().close()
        finally:
            for lease in self.client_leases.values():
                lease.close()
            self.client_leases.clear()
        for node in self.nodes.values():
            if node.log is not None:
                text = node.log.read_text(errors="replace")
                reports = ("ERROR: AddressSanitizer", "runtime error:", "LeakSanitizer",
                           "WARNING: ThreadSanitizer", "AddressSanitizer:DEADLYSIGNAL")
                assert not any(report in text for report in reports), str(node.log)


def main():
    if not __debug__:
        raise RuntimeError("run acceptance without Python -O")
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path,
                        default=Path(os.environ.get("KV_SERVER", ROOT / "build/kvserver")))
    parser.add_argument("--artifacts", type=Path,
                        default=ROOT / "build/test-logs" / f"m7-kvserver-cluster-{time.time_ns()}")
    args = parser.parse_args()
    binary = args.binary.resolve()
    assert binary.is_file(), binary
    artifacts = args.artifacts.resolve()
    artifacts.mkdir(parents=True, exist_ok=False)
    fixtures = artifacts / "fixtures"
    fixtures.mkdir()
    print(f"kvserver cluster artifacts={artifacts}", flush=True)
    evidence = dict(binary=str(binary))
    evidence["fresh"] = worker.fresh_election_kill_restart(
        binary, fixtures / "fresh", artifacts / "fresh", cluster_type=ServerCluster)
    evidence["catchup"] = worker.old_log_catchup(
        binary, fixtures / "catchup", artifacts / "catchup", cluster_type=ServerCluster)
    (artifacts / "evidence.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print("kvserver M7 cluster PASS (3 actual kvserver PIDs; KV Error gate; no KV apply)")


if __name__ == "__main__":
    main()
