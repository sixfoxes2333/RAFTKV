#!/usr/bin/env python3
"""M8 端到端：真实 kvserver 三进程上的 SET/GET/DEL 必须经 Raft 提交后执行。"""
import argparse
import json
import os
from pathlib import Path
import socket
import struct
import time

import raft_server_cluster_test as cluster_mod
from raft_server_startup_test import receive

ROOT = Path(__file__).resolve().parents[1]

# kv::Op
SET, GET, DEL = 1, 2, 3
# kv::Status
OK, NOT_FOUND, ERROR = 0, 1, 2


class Client:
    """最小协议客户端：4 字节长度头 + 请求正文，逐条请求逐条回复。"""

    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)

    def call(self, op, key=b"", value=b""):
        body = struct.pack(">BII", op, len(key), len(value)) + key + value
        self.sock.sendall(struct.pack(">I", len(body)) + body)
        length, = struct.unpack(">I", receive(self.sock, 4))
        payload = receive(self.sock, length)
        status, vlen = struct.unpack(">BI", payload[:5])
        return status, payload[5:5 + vlen]

    def close(self):
        self.sock.close()


def leader_of(cluster, timeout=20):
    leader, rows = cluster.stable_leader(timeout=timeout)
    return leader, rows


def retry(fn, timeout=20, interval=0.05):
    """领导权刚切换时客户端会收到 Error，重试直到服务真的就绪。"""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            result = fn()
            if result[0] != ERROR:
                return result
            last = result
        except (OSError, AssertionError) as error:
            last = error
        time.sleep(interval)
    raise AssertionError(f"operation never succeeded: {last}")


def run(binary, work, artifacts):
    cluster = cluster_mod.ServerCluster(binary, work, artifacts)
    evidence = {}
    try:
        cluster.start()
        leader, rows = cluster.stable_leader()
        port = cluster.client_ports[leader["id"]]

        client = Client(port)
        try:
            status, _ = retry(lambda: client.call(SET, b"name", b"sixfoxes"))
            assert status == OK, status
            status, value = retry(lambda: client.call(GET, b"name"))
            assert status == OK and value == b"sixfoxes", (status, value)

            #所有节点都必须把这条命令执行到本地派生 KV
            status, value = retry(lambda: client.call(GET, b"name"))
            assert status == OK and value == b"sixfoxes"
            rows = cluster.rows()
            assert all(r["applied"] == r["commit"] for r in rows), rows
            for node in cluster.nodes.values():
                cache = node.directory / "kv.cache"
                assert cache.is_file() and b"sixfoxes" in cache.read_bytes(), node.id

            status, _ = retry(lambda: client.call(DEL, b"name"))
            assert status == OK, status
            status, _ = retry(lambda: client.call(GET, b"name"))
            assert status == NOT_FOUND, status
            print("PASS m8: SET/GET/DEL committed through Raft and applied on all 3 nodes", flush=True)
            evidence["first"] = dict(leader=leader, rows=rows)
        finally:
            client.close()

        #杀掉 leader，新 leader 必须能看到已提交的数据
        old = leader["id"]
        cluster.nodes[old].kill()
        new_leader, rows = cluster.stable_leader(after_term=leader["term"])
        assert new_leader["id"] != old
        client = Client(cluster.client_ports[new_leader["id"]])
        try:
            status, _ = retry(lambda: client.call(SET, b"after", b"recovery"))
            assert status == OK, status
            status, value = retry(lambda: client.call(GET, b"after"))
            assert status == OK and value == b"recovery", (status, value)
            print(f"PASS m8: leader {old} killed; node {new_leader['id']} accepted new committed write",
                  flush=True)
            evidence["after_kill"] = dict(leader=new_leader, rows=rows)
        finally:
            client.close()

        #重启旧节点：它必须从日志重放并保持数据一致
        cluster.nodes[old].start()
        joined, rows = cluster.stable_leader(after_term=leader["term"])
        restarted = next(r for r in rows if r["id"] == old)
        assert restarted["applied"] == restarted["commit"]
        assert b"recovery" in (cluster.nodes[old].directory / "kv.cache").read_bytes()
        print(f"PASS m8: node {old} restarted and replayed committed log", flush=True)
        evidence["restart"] = dict(leader=joined, rows=rows)

        for node in cluster.nodes.values():
            assert not (node.directory / "must-not-open.data").exists()
        return evidence
    finally:
        cluster.close()


def main():
    if not __debug__:
        raise RuntimeError("run acceptance without Python -O")
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path,
                        default=Path(os.environ.get("KV_SERVER", ROOT / "build/kvserver")))
    parser.add_argument("--artifacts", type=Path,
                        default=ROOT / "build/test-logs" / f"m8-kv-e2e-{time.time_ns()}")
    args = parser.parse_args()
    binary = args.binary.resolve()
    assert binary.is_file(), binary
    artifacts = args.artifacts.resolve()
    artifacts.mkdir(parents=True, exist_ok=False)
    print(f"m8 kv e2e artifacts={artifacts}", flush=True)
    evidence = run(binary, artifacts / "fixtures", artifacts / "logs")
    (artifacts / "evidence.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print("raft KV M8 e2e PASS (3 real kvserver processes; committed apply)")


if __name__ == "__main__":
    main()
