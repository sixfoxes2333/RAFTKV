#!/usr/bin/env python3
"""M9 故障验收：真三进程 kvserver 上的少数派、追平、重启与有界失败。

只用真实进程和真实客户端协议；不注入内核故障，也不模拟 Raft。
"""
import argparse
import json
import os
from pathlib import Path
import socket
import struct
import time

import raft_server_cluster_test as cluster_mod
from raft_kv_e2e_test import Client, GET, OK, ERROR, retry
from raft_server_startup_test import receive

ROOT = Path(__file__).resolve().parents[1]


def quiescent(cluster, timeout=25):
    """所有存活节点 same term / 单 leader / applied==commit==last。"""
    leader, rows = cluster.stable_leader(timeout=timeout)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rows = cluster.rows()
        if rows and all(r["applied"] == r["commit"] == r["last"] for r in rows):
            return leader, rows
        time.sleep(0.05)
    raise AssertionError(f"nodes never reached applied==commit==last: {cluster.rows()}")


def logs_identical(cluster):
    """静止后三个节点的 Raft 日志必须逐字节相同；相同日志 + 确定性执行 = 相同状态。"""
    data = {i: (n.directory / "raft.log").read_bytes()
            for i, n in cluster.nodes.items() if n.expected_alive}
    reference = next(iter(data.values()))
    for node_id, blob in data.items():
        assert blob == reference, f"node {node_id} log differs"
    return len(reference)


def probe_status(port):
    """开一条新连接问一次：返回 kv::Status，连接问题直接抛出。"""
    sock = Client(port)
    try:
        return sock.call(2, b"probe")[0]
    finally:
        sock.close()


def expect_error_within(port, timeout=12.0):
    """没有多数派时，客户端必须在有界时间内拿到 Error，不能被永久挂住。"""
    started = time.monotonic()
    deadline = started + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            status = probe_status(port)
            if status == ERROR:
                return time.monotonic() - started
            last = status
        except OSError as error:
            last = error
        time.sleep(0.05)
    raise AssertionError(f"no bounded failure after {timeout}s, last={last}")


def op(client, opcode, key, value=b""):
    return retry(lambda: client.call(opcode, key, value))


def run(binary, work, artifacts):
    cluster = cluster_mod.ServerCluster(binary, work, artifacts)
    evidence = {}
    try:
        cluster.start()
        leader, rows = quiescent(cluster)
        first = cluster.nodes[leader["id"]]
        client = Client(cluster.client_ports[leader["id"]])
        try:
            status, _ = op(client, 1, b"durable", b"confirmed")
            assert status == OK

            #A：静止后日志逐字节一致，且三节点执行进度追上提交
            leader, rows = quiescent(cluster)
            size = logs_identical(cluster)
            assert all(r["applied"] == r["commit"] == r["last"] for r in rows), rows
            evidence["quiescent"] = dict(leader=leader, rows=rows, log_bytes=size)
            print("PASS m9: quiescent 3 nodes share identical raft.log and applied==commit==last", flush=True)

            #B：已确认的写在 leader 被 kill -9 后仍然存在
            old = leader["id"]
            cluster.nodes[old].kill()
            new_leader, rows = quiescent(cluster)
            assert new_leader["id"] != old
            client.close()
            client = Client(cluster.client_ports[new_leader["id"]])
            status, value = op(client, 2, b"durable")
            assert status == OK and value == b"confirmed", (status, value)
            evidence["leader_kill"] = dict(killed=old, leader=new_leader, rows=rows)
            print(f"PASS m9: confirmed write survived SIGKILL of leader {old}", flush=True)
        finally:
            client.close()

        #重启被杀节点，它必须追平到同一份日志
        cluster.nodes[old].start()
        leader, rows = quiescent(cluster)
        logs_identical(cluster)
        print(f"PASS m9: node {old} rejoined and converged to the same log", flush=True)

        #C：多数派丢失时，写必须显式失败而不是假装成功
        alive = [i for i, n in cluster.nodes.items() if n.expected_alive]
        for victim in alive:
            if victim != leader["id"]:
                cluster.nodes[victim].kill()
        survivor = leader["id"]
        if not cluster.nodes[survivor].expected_alive:
            survivor = alive[0]
        def attempt(port):
            sock = Client(port)
            try:
                return sock.call(1, b"minority", b"must-not-succeed")
            finally:
                sock.close()

        started = time.monotonic()
        status = None
        while time.monotonic() - started < 10:
            try:
                status, _ = attempt(cluster.client_ports[survivor])
            except OSError:
                status = ERROR
            if status != OK:
                break
            time.sleep(0.05)
        assert status != OK, "minority node acknowledged a write"
        waited = time.monotonic() - started
        evidence["minority"] = dict(survivor=survivor, status=status, seconds=round(waited, 2))
        print(f"PASS m9: minority could not confirm a write (rejected after {waited:.2f}s)", flush=True)

        #恢复多数派：无论那条写是否被提交，各节点必须一致
        for node_id in alive:
            if not cluster.nodes[node_id].expected_alive:
                cluster.nodes[node_id].start()
        leader, rows = quiescent(cluster, timeout=30)
        logs_identical(cluster)
        values = {}
        for node_id, node in cluster.nodes.items():
            blob = (node.directory / "kv.cache").read_bytes()
            values[node_id] = b"minority" in blob
        assert len(set(values.values())) == 1, values
        evidence["repaired"] = dict(leader=leader, rows=rows, minority_present=values)
        print(f"PASS m9: after healing all nodes agree on whether the minority write exists ({values})",
              flush=True)

        #D：followers 全部离线后，剩余节点无 leader，客户端必须有界失败
        leader, rows = quiescent(cluster)
        keeper = leader["id"]
        for node_id, node in cluster.nodes.items():
            if node_id != keeper:
                node.kill()
        waiting = time.monotonic() + 6
        role = None
        while time.monotonic() < waiting:
            rows = cluster.rows()
            if rows:
                role = rows[0]["role"]
                if role != "leader":
                    break
            time.sleep(0.05)
        #check-quorum：联系不上多数派的 leader 必须主动退位
        assert role != "leader", f"lone leader kept claiming leadership: {role}"
        waited = expect_error_within(cluster.client_ports[keeper])
        evidence["no_quorum"] = dict(node=keeper, role=role, seconds=round(waited, 2))
        print(f"PASS m9: lone leader stepped down and failed reads within {waited:.2f}s", flush=True)

        for node_id, node in cluster.nodes.items():
            if not node.expected_alive:
                node.start()
        leader, rows = quiescent(cluster, timeout=30)
        logs_identical(cluster)
        evidence["final"] = dict(leader=leader, rows=rows)
        print("PASS m9: cluster recovered to a single stable leader after all partitions healed",
              flush=True)
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
                        default=ROOT / "build/test-logs" / f"m9-fault-{time.time_ns()}")
    args = parser.parse_args()
    binary = args.binary.resolve()
    assert binary.is_file(), binary
    artifacts = args.artifacts.resolve()
    artifacts.mkdir(parents=True, exist_ok=False)
    print(f"m9 fault acceptance artifacts={artifacts}", flush=True)
    evidence = run(binary, artifacts / "fixtures", artifacts / "logs")
    (artifacts / "evidence.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print("raft KV M9 fault acceptance PASS (real processes; no injected disk faults)")


if __name__ == "__main__":
    main()
