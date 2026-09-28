#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
net_startup_fail_test.py —— 启动期系统调用失败必须"响亮地死掉"（Step 6）

【这个测试在验证什么】
    同一个端口已经有一个 kvserver 在监听时，再启动一个 kvserver，
    它的 bind() 必然失败（Address already in use）。此时进程必须：
      1) 打印明确的错误信息
      2) 以非 0 退出码退出
    而不是"忽略返回值继续往下跑"。

【当前为什么是红的（Step 6 之前）】
    main() 里 bind()/listen() 的返回值都没检查：
      · bind 失败 → 被忽略
      · listen 在一个未绑定的 socket 上调用 → Linux 会**自动绑到一个随机端口**并开始监听
        → 进程一路跑到 reactor.loop()，永远卡在那里
    结果：第二个实例既不报错也不退出，运维视角就是"起了个假进程"。
    本用例断言"3 秒内必须退出且退出码非 0" → 现在必然失败。

【判据】
    1) 3 秒内退出（不是一直活着）
    2) 退出码 != 0
    3) 日志里能看到错误信息（含 bind / Address already in use 之一）

【用法】
    python3 tests/net_startup_fail_test.py
    环境变量：KV_SERVER / KV_PORT
"""
import os
import socket
import subprocess
import sys
import time

SERVER = os.environ.get("KV_SERVER", "build/kvserver")
PORT = int(os.environ.get("KV_PORT", "9006"))
SERVER_LOG_A = "/tmp/kv_startup_a.log"
SERVER_LOG_B = "/tmp/kv_startup_b.log"

#两个实例必须用【不同的数据文件】：否则第二个实例会先被数据库文件锁挡住
#（那是 db_lock_test.py 的场景），根本走不到 bind 失败 —— 本测试验证的是"端口冲突必须响亮退出"
DB_A = "/tmp/kv_startup_a.data"
DB_B = "/tmp/kv_startup_b.data"

EXIT_WAIT = 3.0        # 第二个实例应该在这个时间内退出


def wait_ready(proc, log_path):
    """等端口可连接（服务器启动完成的判据）"""
    deadline = time.time() + 5.0
    while time.time() < deadline:
        if proc.poll() is not None:
            break
        try:
            s = socket.create_connection(("127.0.0.1", PORT), timeout=0.2)
            s.close()
            return True
        except OSError:
            time.sleep(0.05)
    return False


def main() -> int:
    print(f"[启动失败测试] 先正常启动一个 {SERVER}（占住 {PORT} 端口）")
    log_a = open(SERVER_LOG_A, "wb")
    for p in (DB_A,DB_B):
        if os.path.exists(p):
            os.remove(p)

    proc_a = subprocess.Popen([SERVER,DB_A], stdout=log_a, stderr=subprocess.STDOUT,
                              cwd=os.getcwd())
    proc_b = None
    try:
        if not wait_ready(proc_a, SERVER_LOG_A):
            print("!! 第一个实例没能启动，检查端口是否被别的进程占用")
            return 2

        print("  再启动第二个实例（同一个端口 → bind 必然失败）")
        log_b = open(SERVER_LOG_B, "wb")
        proc_b = subprocess.Popen([SERVER,DB_B], stdout=log_b, stderr=subprocess.STDOUT,
                                  cwd=os.getcwd())

        # ---- 等它退出 ----
        deadline = time.time() + EXIT_WAIT
        while time.time() < deadline and proc_b.poll() is None:
            time.sleep(0.1)
        log_b.flush()

        exited = proc_b.poll() is not None
        code = proc_b.returncode
        try:
            text = open(SERVER_LOG_B, encoding="utf-8", errors="replace").read()
        except OSError:
            text = ""

        if not exited:
            proc_b.terminate()
            try:
                proc_b.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc_b.kill()
                proc_b.wait(timeout=3)

        ok_exit = exited
        ok_code = exited and (code != 0)
        ok_msg = ("bind" in text.lower()) or ("address already in use" in text.lower())

        print(f"  第二个实例：退出 = {exited}，退出码 = {code}，"
              f"日志含错误信息 = {ok_msg}")
        if not ok_msg and text.strip():
            print("  它的日志内容（前 3 行）：")
            for line in text.strip().splitlines()[:3]:
                print(f"    | {line}")

        if ok_exit and ok_code and ok_msg:
            print("[启动失败测试] PASS")
            return 0

        print("[启动失败测试] FAIL")
        if not ok_exit:
            print("  原因：第二个实例一直活着 —— 说明 bind()/listen() 的失败被忽略了，"
                  "进程跑到事件循环里空转（运维视角 = 起了个假进程）")
        elif not ok_code:
            print("  原因：虽然退出了，但退出码是 0 —— 启动失败必须是非 0 退出码")
        else:
            print("  原因：日志里没有 bind/Address already in use 这类明确的错误信息")
        print(f"  日志：{SERVER_LOG_B}")
        return 1
    finally:
        if proc_a.poll() is None:
            proc_a.terminate()
            try:
                proc_a.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc_a.kill()
                proc_a.wait(timeout=3)


if __name__ == "__main__":
    sys.exit(main())
