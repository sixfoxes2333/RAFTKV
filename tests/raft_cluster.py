"""raft_cluster.py —— Raft 多节点测试脚手架

用法：
    python3 tests/raft_cluster.py --smoke          # W1 验收：起 3 个进程、能读到状态行、干净退出
    python3 tests/raft_cluster.py --leader         # W2 验收：等到恰好一个 leader（并打印三节点状态）
    python3 tests/raft_cluster.py --kill-leader    # W2/W3：kill -9 leader，要求重选出新 leader

约定（见 D:\\TW\\Raft-W1-接口约定.md）：
    · 每个节点独立数据目录 + 客户端端口 + Raft 端口，配置全部走【环境变量】：
        KV_PORT / KV_RAFT_ID / KV_RAFT_PORT / KV_RAFT_PEERS
    · 节点每 500ms 往 stdout 打一行：
        RAFTSTATUS id=1 role=leader term=3 commit=42 last=42 applied=42
    · 状态行是脚手架判断"谁是 leader / 谁追平了"的唯一依据 —— 自由文本日志不可靠。

设计要点：
    · 日志一律【追加】写入（同 kvproto.start_server 的理由：重启多次时旧日志不能被冲掉），
      每轮 run 前由调用方清理；解析时只看最后一行 RAFTSTATUS。
    · kill 用 SIGKILL（kill -9）：模拟崩溃，不跑任何收尾路径。
    · 失败时把三个节点的日志尾部一起打出来，不用手动去翻文件。
"""

import argparse
import os
import re
import shutil
import signal
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import kvproto  # noqa: E402

STATUS_RE = re.compile(
    r"^RAFTSTATUS id=(\d+) role=(\w+) term=(\d+) commit=(\d+) last=(\d+) applied=(\d+)"
)

ROLES = ("follower", "candidate", "leader")


class Status:
    __slots__ = ("node_id", "role", "term", "commit", "last", "applied")

    def __init__(self, node_id, role, term, commit, last, applied):
        self.node_id = node_id
        self.role = role
        self.term = term
        self.commit = commit
        self.last = last
        self.applied = applied

    def __repr__(self):
        return (f"<node{self.node_id} {self.role} term={self.term} "
                f"commit={self.commit} last={self.last} applied={self.applied}>")


class Node:
    def __init__(self, node_id, client_port, raft_port, data_dir, log_path):
        self.node_id = node_id
        self.client_port = client_port
        self.raft_port = raft_port
        self.data_dir = data_dir
        self.log_path = log_path
        self.proc = None

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    @property
    def data_file(self):
        """位置参数用的是 KV 数据【文件】路径（沿用现状）；
        Raft 的日志/状态文件放同一目录下，由服务器自己拼路径。"""
        return os.path.join(self.data_dir, "kvdb.data")


class Cluster:
    def __init__(self, size=3, base_client_port=9001, base_raft_port=9101,
                 binary=None, root="/tmp/kv_raft_cluster", extra_env=None):
        self.size = size
        self.binary = binary or os.environ.get("KV_SERVER", "./build/kvserver")
        self.root = root
        self.extra_env = dict(extra_env or {})
        self.nodes = [
            Node(i + 1,
                 base_client_port + i,
                 base_raft_port + i,
                 os.path.join(root, f"n{i + 1}"),
                 os.path.join(root, f"n{i + 1}.log"))
            for i in range(size)
        ]

    # ---------- 生命周期 ----------

    def peers_arg(self):
        return ",".join(f"{n.node_id}@127.0.0.1:{n.raft_port}" for n in self.nodes)

    def start_node(self, node):
        os.makedirs(node.data_dir, exist_ok=True)
        env = os.environ.copy()
        env.update(self.extra_env)
        env["KV_PORT"] = str(node.client_port)
        env["KV_RAFT_ID"] = str(node.node_id)
        env["KV_RAFT_PORT"] = str(node.raft_port)
        env["KV_RAFT_PEERS"] = self.peers_arg()

        log = open(node.log_path, "ab")
        log.write(f"\n===== node{node.node_id} start at {time.strftime('%H:%M:%S')} "
                  f"port={node.client_port} raft_port={node.raft_port} =====\n".encode())
        log.flush()
        node.proc = subprocess.Popen([self.binary, node.data_file],
                                     stdout=log, stderr=subprocess.STDOUT, env=env)
        return node

    def start(self):
        for node in self.nodes:
            self.start_node(node)
        return self

    def stop_node(self, node, hard=True):
        if node.proc is None:
            return
        if node.proc.poll() is None:
            node.proc.send_signal(signal.SIGKILL if hard else signal.SIGTERM)
        try:
            node.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            node.proc.kill()
            node.proc.wait(timeout=5)
        node.proc = None

    def stop_all(self):
        for node in self.nodes:
            self.stop_node(node)

    def kill(self, node_id):
        """kill -9 一个节点（模拟崩溃）"""
        node = self.node(node_id)
        self.stop_node(node, hard=True)
        return node

    def restart(self, node_id):
        node = self.node(node_id)
        self.stop_node(node, hard=True)
        return self.start_node(node)

    def node(self, node_id):
        for n in self.nodes:
            if n.node_id == node_id:
                return n
        raise KeyError(f"没有 node {node_id}")

    def wait_client_ready(self, timeout=10.0):
        """所有节点的客户端端口都要能连上（Raft 端口是否就绪不在这里管）"""
        for node in self.nodes:
            if not kvproto.wait_port(node.client_port, timeout):
                raise RuntimeError(f"node{node.node_id} 未监听客户端端口 {node.client_port}")
        return True

    # ---------- 状态解析 ----------

    def last_line(self, node):
        try:
            with open(node.log_path, "rb") as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                f.seek(max(0, size - 65536))
                data = f.read().decode("utf-8", "replace")
        except FileNotFoundError:
            return ""
        return data.strip().splitlines()[-1] if data.strip() else ""

    def status_of(self, node):
        """返回该节点最后一行 RAFTSTATUS 解析出的 Status；没有则 None"""
        try:
            with open(node.log_path, "rb") as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                f.seek(max(0, size - 65536))
                data = f.read().decode("utf-8", "replace")
        except FileNotFoundError:
            return None
        for line in reversed(data.splitlines()):
            m = STATUS_RE.match(line.strip())
            if m:
                nid, role, term, commit, last, applied = m.groups()
                return Status(int(nid), role, int(term), int(commit), int(last), int(applied))
        return None

    def statuses(self):
        out = {}
        for node in self.nodes:
            st = self.status_of(node)
            if st is not None:
                out[node.node_id] = st
        return out

    def leaders(self):
        return [st for st in self.statuses().values() if st.role == "leader"]

    # ---------- 等待/断言 ----------

    def wait_until(self, pred, timeout, desc, interval=0.1):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            last = pred()
            if last:
                return last
            time.sleep(interval)
        self.dump_logs()
        raise AssertionError(f"等待超时（{timeout}s）：{desc}\n最后观测：{last}")

    def wait_status_lines(self, timeout=5.0):
        """每个节点都至少打过一行 RAFTSTATUS"""
        return self.wait_until(
            lambda: len(self.statuses()) == self.size,
            timeout,
            "每个节点都应打印 RAFTSTATUS（检查 main.cpp 里的状态行实现 / KV_RAFT_* 环境变量）")

    def wait_for_leader(self, timeout=10.0):
        def exactly_one():
            leaders = self.leaders()
            return leaders[0] if len(leaders) == 1 else None

        st = self.wait_until(exactly_one, timeout,
                             "应恰好有一个 leader（0 个 = 选不出来；多个 = 脑裂/role 判断有问题）")
        return st

    def wait_for_apply(self, index, timeout=15.0):
        """所有节点的 applied 都 >= index（即日志已经收敛并应用）"""
        def all_applied():
            sts = self.statuses()
            if len(sts) != self.size:
                return None
            if all(st.applied >= index for st in sts.values()):
                return sts
            return None

        return self.wait_until(all_applied, timeout, f"所有节点 applied >= {index}")

    def data_of(self, node_id, key):
        """直接问某个节点的客户端接口（验证"另一个节点的存储里也看到了"）"""
        node = self.node(node_id)
        s = kvproto.connect(node.client_port)
        try:
            return kvproto.request(s, kvproto.OP_GET, key.encode() if isinstance(key, str) else key)
        finally:
            s.close()

    def write(self, node_id, key, value):
        """通过某个节点的客户端接口写（Raft 下应该转发/拒绝给 follower —— W3 再断言具体行为）"""
        node = self.node(node_id)
        s = kvproto.connect(node.client_port)
        try:
            return kvproto.request(s, kvproto.OP_SET,
                                   key.encode() if isinstance(key, str) else key,
                                   value.encode() if isinstance(value, str) else value)
        finally:
            s.close()

    # ---------- 诊断 ----------

    def dump_logs(self, tail=25, stream=sys.stderr):
        print("\n------ 节点日志（尾部）------", file=stream)
        for node in self.nodes:
            print(f"\n=== node{node.node_id} (port={node.client_port}, "
                  f"raft_port={node.raft_port}, pid={node.proc.pid if node.proc else '-'}, "
                  f"{'alive' if node.alive() else 'dead'}) ===", file=stream)
            try:
                with open(node.log_path, "r", errors="replace") as f:
                    lines = f.read().splitlines()
                for line in lines[-tail:]:
                    print("  " + line, file=stream)
            except FileNotFoundError:
                print("  (无日志)", file=stream)
        print("------ 状态摘要 ------", file=stream)
        for nid, st in self.statuses().items():
            print(f"  node{nid}: {st}", file=stream)
        stream.flush()

    def status_summary(self):
        rows = []
        for node in self.nodes:
            st = self.status_of(node)
            if st is None:
                rows.append(f"  node{node.node_id}: (无状态行) {'alive' if node.alive() else 'dead'}")
            else:
                rows.append(f"  node{node.node_id}: {st} {'alive' if node.alive() else 'dead'}")
        return "\n".join(rows)


# ---------- 命令行 ----------

def prepare(root, clean=True):
    if clean and os.path.isdir(root):
        shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root, exist_ok=True)


def main():
    ap = argparse.ArgumentParser(description="Raft 多节点测试脚手架")
    ap.add_argument("--smoke", action="store_true", help="起 3 节点，读到状态行即可（W1 验收）")
    ap.add_argument("--leader", action="store_true", help="等到恰好一个 leader（W2 验收）")
    ap.add_argument("--kill-leader", action="store_true", help="kill -9 leader 后要求重选出新 leader")
    ap.add_argument("--size", type=int, default=3)
    ap.add_argument("--root", default="/tmp/kv_raft_cluster")
    ap.add_argument("--binary", default=os.environ.get("KV_SERVER", "./build/kvserver"))
    ap.add_argument("--timeout", type=float, default=15.0)
    ap.add_argument("--keep", action="store_true", help="结束后保留进程（调试用）")
    args = ap.parse_args()

    if not (args.smoke or args.leader or args.kill_leader):
        ap.print_help()
        return 2

    prepare(args.root)
    c = Cluster(size=args.size, binary=args.binary, root=args.root)
    ok = False
    try:
        c.start()
        c.wait_client_ready(timeout=args.timeout)
        c.wait_status_lines(timeout=args.timeout)
        print("状态行可读：")
        print(c.status_summary())

        if args.smoke:
            ok = True
        elif args.leader:
            leader = c.wait_for_leader(timeout=args.timeout)
            print(f"leader = node{leader.node_id}（term={leader.term}）")
            ok = True
        elif args.kill_leader:
            leader = c.wait_for_leader(timeout=args.timeout)
            print(f"当前 leader = node{leader.node_id}（term={leader.term}），kill -9 它")
            c.kill(leader.node_id)
            new_leader = c.wait_for_leader(timeout=args.timeout)
            assert new_leader.node_id != leader.node_id, "leader 应当换人"
            assert new_leader.term > leader.term, "新 leader 的 term 应当更大"
            print(f"新 leader = node{new_leader.node_id}（term={new_leader.term}）✓")
            ok = True

        if not ok:
            print("未选择任何模式", file=sys.stderr)
    except AssertionError as e:
        print(f"\n❌ FAIL: {e}", file=sys.stderr)
        c.dump_logs()
        return 1
    except Exception as e:  # noqa: BLE001
        print(f"\n❌ ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        c.dump_logs()
        return 1
    finally:
        if not args.keep:
            c.stop_all()
    print("\n✅ PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
