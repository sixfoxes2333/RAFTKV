#!/usr/bin/env python3
"""旧 CLI 的兼容入口，统一使用 M7 的当前 PID/新鲜状态/稳定任期观察器。

--smoke / --leader / --kill-leader 保留；端口动态预留，Raft 端口取 PEERS。
每次在 --root 下建立独立运行目录，不删除既有目录或复用历史 leader 行。
"""
import argparse
import json
import os
from pathlib import Path
import time

from raft_server_cluster_test import ServerCluster


def main():
    """控制本次创建的三个 kvserver；失败或默认结束时只清理这些进程。"""
    if not __debug__:
        raise RuntimeError("run acceptance without Python -O")
    parser = argparse.ArgumentParser(description="实际 kvserver 三节点验收")
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--smoke", action="store_true")
    modes.add_argument("--leader", action="store_true")
    modes.add_argument("--kill-leader", action="store_true")
    parser.add_argument("--size", type=int, choices=(3,), default=3)
    parser.add_argument("--root", type=Path, default=Path("/tmp/kv_raft_cluster"))
    parser.add_argument("--binary", type=Path,
                        default=Path(os.environ.get("KV_SERVER", "./build/kvserver")))
    parser.add_argument("--timeout", type=float, default=15)
    parser.add_argument("--keep", action="store_true", help="成功后保留本次进程并打印 PID")
    args = parser.parse_args()
    binary = args.binary.resolve()
    if not binary.is_file() or args.timeout <= 0:
        parser.error("binary must exist and timeout must be positive")
    run = args.root.resolve() / f"run-{time.time_ns()}"
    run.mkdir(parents=True)
    cluster = ServerCluster(binary, run / "data", run / "logs")
    success = False
    try:
        cluster.start()
        if args.smoke:
            deadline = time.monotonic() + args.timeout
            rows = None
            while time.monotonic() < deadline:
                rows = cluster.rows()
                if rows is not None and len(rows) == 3:
                    break
                time.sleep(0.04)
            assert rows is not None and len(rows) == 3, "no three fresh live statuses"
        else:
            leader, rows = cluster.stable_leader(timeout=args.timeout)
            cluster.votes(leader)
            if args.kill_leader:
                cluster.nodes[leader["id"]].kill()
                replacement, rows = cluster.stable_leader(
                    after_term=leader["term"], timeout=args.timeout)
                assert replacement["id"] != leader["id"]
                cluster.votes(replacement)
                print(f"SIGKILL node {leader['id']}; new leader={replacement['id']} term={replacement['term']}")
        print(json.dumps(rows, indent=2))
        success = True
        print(f"raft_cluster PASS; artifacts={run}")
        if args.keep:
            print("retained PIDs:", [n.proc.pid for n in cluster.nodes.values() if n.expected_alive])
    finally:
        if not (success and args.keep):
            cluster.close()


if __name__ == "__main__":
    main()
