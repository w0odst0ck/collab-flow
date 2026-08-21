#!/usr/bin/env python3
"""flow-task-core.py 单元测试(M1):纯函数 T1-T4 + store S1-S7 + runner R1-R10。

用法: python3 -m unittest discover tests
零 API、全程 FLOW_TASK_DIR 临时目录隔离。纯函数直接 import(零 I/O);
runner 集成用进程内调用 + fd 重定向模拟 spawn(输出落 logs/<id>.log)。
"""

import contextlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_TASK_CORE = os.path.join(_HERE, "..", "scripts", "flow-task-core.py")

_spec = importlib.util.spec_from_file_location("flow_task_core", _TASK_CORE)
tc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tc)

# 故意拼接,避免源码静态 deny-list 命中(sk-[A-Za-z0-9]{10});串须匹配 DENY_RE(sk- 后 10 字母数字)
_FAKE_SECRET = "sk-" + "fakekey12345"


def _cfg(**kw):
    base = {"task": {"schema_version": 1, "max_parallel": 2, "default_priority": "P2",
                     "log_tail_bytes": 2000, "kill_grace_s": 1}}
    base["task"].update(kw)
    return base


def _entry(tid="t-000000000001", state="queued", priority="P2", workitem=None,
           command="sleep 0.01", created_at="2026-08-21T10:00:00+00:00",
           started_at=None, finished_at=None, exit_code=None, failure_tail=None,
           expected_seconds=None, kill_on_timeout=False, pid=None):
    return {
        "id": tid, "workitem": workitem, "command": command, "priority": priority,
        "state": state, "expected_seconds": expected_seconds,
        "kill_on_timeout": kill_on_timeout, "created_at": created_at,
        "started_at": started_at, "finished_at": finished_at, "exit_code": exit_code,
        "failure_tail": failure_tail, "pid": pid, "heartbeat_at": None,
    }


class _FDRedirect:
    """把进程 fd 1/2 重定向到文件(命令子进程输出随 fd 继承进日志),用毕恢复。"""

    def __init__(self, path):
        self.path = path

    def __enter__(self):
        self._saved = (os.dup(1), os.dup(2))
        self._fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND)
        os.dup2(self._fd, 1)
        os.dup2(self._fd, 2)
        return self

    def __exit__(self, *exc):
        os.dup2(self._saved[0], 1)
        os.dup2(self._saved[1], 2)
        os.close(self._fd)
        os.close(self._saved[0])
        os.close(self._saved[1])


class TaskIsoBase(unittest.TestCase):
    """FLOW_TASK_DIR 临时目录隔离基类。"""

    def setUp(self):
        self._old_env = os.environ.get("FLOW_TASK_DIR")
        self.task_dir = tempfile.mkdtemp(prefix="flow-task-test-")
        os.environ["FLOW_TASK_DIR"] = self.task_dir

    def tearDown(self):
        if self._old_env is None:
            os.environ.pop("FLOW_TASK_DIR", None)
        else:
            os.environ["FLOW_TASK_DIR"] = self._old_env
        shutil.rmtree(self.task_dir, ignore_errors=True)

    def _write_registry(self, reg):
        os.makedirs(self.task_dir, exist_ok=True)
        with open(os.path.join(self.task_dir, "tasks.json"), "w", encoding="utf-8") as f:
            json.dump(reg, f, ensure_ascii=False)

    def _run_runner(self, tid, cfg):
        """进程内同步执行 _runner,fd 重定向使命令输出落 logs/<id>.log(模拟 spawn)。"""
        os.makedirs(tc.logs_dir(), exist_ok=True)
        with _FDRedirect(tc.log_path(tid)):
            return tc._runner(tid, cfg)


# ---------------------------------------------------------------------------
# T1-T4:状态机纯函数(零 I/O)
# ---------------------------------------------------------------------------

class TaskStateMachineTests(unittest.TestCase):
    """T1-T4 状态机纯函数单测(§2.1)。"""

    def test_T1_terminal_state(self):
        self.assertEqual(tc.terminal_state(0, False), ("done", None))
        self.assertEqual(tc.terminal_state(1, False), ("failed", "exit_code=1"))
        self.assertEqual(tc.terminal_state(124, False), ("timeout", "timeout_exceeded"))
        self.assertEqual(tc.terminal_state(0, True), ("timeout", "timeout_exceeded"))

    def test_T2_plan_dispatch_slots(self):
        reg = {"schema_version": 1, "tasks": {
            "t-000000000001": _entry(tid="t-000000000001", state="queued", created_at="2026-08-21T10:00:00+00:00"),
            "t-000000000002": _entry(tid="t-000000000002", state="queued", created_at="2026-08-21T10:00:01+00:00"),
            "t-000000000003": _entry(tid="t-000000000003", state="queued", created_at="2026-08-21T10:00:02+00:00"),
            "t-000000000004": _entry(tid="t-000000000004", state="running", created_at="2026-08-21T10:00:03+00:00"),
            "t-000000000005": _entry(tid="t-000000000005", state="done", created_at="2026-08-21T10:00:04+00:00"),
        }}
        promoted = tc.plan_dispatch(reg, 2)  # 3 queued + 1 running, free=1
        self.assertEqual([t["id"] for t in promoted], ["t-000000000001"])
        promoted = tc.plan_dispatch(reg, 5)
        self.assertEqual([t["id"] for t in promoted],
                         ["t-000000000001", "t-000000000002", "t-000000000003"])
        # 纯函数不改 reg
        self.assertEqual(reg["tasks"]["t-000000000001"]["state"], "queued")
        # 终态不入队
        self.assertNotIn("t-000000000005", [t["id"] for t in promoted])

    def test_T3_sort_priority_fifo(self):
        tasks = [
            _entry(tid="t-00000000000a", priority="P1", created_at="2026-08-21T10:00:05+00:00"),
            _entry(tid="t-00000000000b", priority="P0", created_at="2026-08-21T10:00:06+00:00"),
            _entry(tid="t-00000000000c", priority="P2", created_at="2026-08-21T10:00:01+00:00"),
            _entry(tid="t-00000000000d", priority="P0", created_at="2026-08-21T10:00:04+00:00"),
            _entry(tid="t-00000000000e", state="running", priority="P0"),
        ]
        ids = [t["id"] for t in tc.sort_queue(tasks)]
        # P0 同级 FIFO(created_at 升序):d(10:00:04) 先于 b(10:00:06)
        self.assertEqual(ids, ["t-00000000000d", "t-00000000000b", "t-00000000000a", "t-00000000000c"])
        # running 不入队
        self.assertNotIn("t-00000000000e", ids)

    def test_T4_validate_entry(self):
        with self.assertRaises(tc.StoreError):
            tc.validate_entry(_entry(state="bogus"))
        with self.assertRaises(tc.StoreError):
            tc.validate_entry(_entry(priority="P9"))
        with self.assertRaises(tc.StoreError):
            tc.validate_entry(_entry(tid="abc-123"))
        with self.assertRaises(tc.StoreError):
            tc.validate_entry(_entry(tid="t-xyz"))
        # 合法条目通过;未知字段忽略(向前兼容)
        ok = tc.validate_entry(dict(_entry(), extra_field="ignored"))
        self.assertEqual(ok["state"], "queued")


# ---------------------------------------------------------------------------
# S1-S7:store(flock 注册表)
# ---------------------------------------------------------------------------

class TaskStoreTests(TaskIsoBase):
    """S1-S7 store 单测(§2.1)。"""

    def test_S1_registry_roundtrip(self):
        # 首用缺失文件 → 空注册表
        reg = tc.load_registry()
        self.assertEqual(reg, {"schema_version": 1, "tasks": {}})
        entry = _entry()
        reg["tasks"][entry["id"]] = entry
        tc.save_registry_atomic(reg)
        reg2 = tc.load_registry()
        self.assertEqual(reg2["tasks"][entry["id"]], entry)  # 字段无损
        self.assertEqual(reg2["schema_version"], 1)

    def test_S2_registry_concurrent_write(self):
        n = 8
        errors = []

        def worker(i):
            try:
                def _do():
                    reg = tc.load_registry()
                    reg["tasks"][f"t-{i:012x}"] = _entry(tid=f"t-{i:012x}")
                    tc.save_registry_atomic(reg)
                tc.with_registry_flock(_do)
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        reg = tc.load_registry()
        self.assertEqual(len(reg["tasks"]), n)  # 无丢失
        for i in range(n):
            self.assertIn(f"t-{i:012x}", reg["tasks"])

    def test_S3_atomic_write_no_half(self):
        reg = tc.empty_registry()
        for i in range(50):
            reg["tasks"][f"t-{i:012x}"] = _entry(tid=f"t-{i:012x}")
        tc.save_registry_atomic(reg)
        # 无 tmp 残留
        leftovers = [f for f in os.listdir(self.task_dir) if ".tmp." in f]
        self.assertEqual(leftovers, [])
        # 并发读写:读方永不观察半写
        stop = threading.Event()
        errors = []

        def writer():
            while not stop.is_set():
                try:
                    tc.save_registry_atomic(tc.load_registry())
                except Exception as e:  # noqa: BLE001
                    errors.append(e)
                    break

        def reader():
            for _ in range(200):
                data = json.loads(open(os.path.join(self.task_dir, "tasks.json"),
                                       encoding="utf-8").read())
                if data.get("schema_version") != 1:
                    errors.append(ValueError("schema_version 丢失/半写"))
                    break
            stop.set()

        w = threading.Thread(target=writer)
        r = threading.Thread(target=reader)
        w.start()
        r.start()
        r.join()
        w.join(timeout=5)
        stop.set()
        self.assertEqual(errors, [])
        reg2 = tc.load_registry()
        self.assertEqual(len(reg2["tasks"]), 50)

    def test_S4_dedup_reject(self):
        cfg = _cfg()
        # queued/running 非终态 → 拒绝
        t1 = tc.add_task(cfg, "sleep 0.05", workitem="w1")
        with self.assertRaises(tc.DuplicateWorkitem) as cm:
            tc.add_task(cfg, "sleep 0.05", workitem="w1")
        self.assertEqual(cm.exception.args[0], t1)
        self.assertEqual(cm.exception.args[1], "running")  # 自动 dispatch 已提升
        time.sleep(0.3)  # 等 runner 结束落 done
        # 终态(done)不阻塞重跑(重试 = 重新 add)
        t2 = tc.add_task(cfg, "sleep 0.05", workitem="w1")
        self.assertNotEqual(t2, t1)
        time.sleep(0.3)

    def test_S5_dedup_race(self):
        cfg = _cfg()
        results = []

        def worker():
            try:
                tid = tc.add_task(cfg, "sleep 0.05", workitem="race-w")
                results.append(("ok", tid))
            except tc.DuplicateWorkitem as e:
                results.append(("dup", e.args[0]))

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        kinds = sorted(r[0] for r in results)
        self.assertEqual(kinds, ["dup", "ok"])  # 恰一个成功一个拒绝
        time.sleep(0.3)  # 清理残留 runner

    def test_S6_load_corrupt(self):
        with open(os.path.join(self.task_dir, "tasks.json"), "w", encoding="utf-8") as f:
            f.write("not json {{{")
        with self.assertRaises(tc.StoreError):
            tc.load_registry()
        with open(os.path.join(self.task_dir, "tasks.json"), "w", encoding="utf-8") as f:
            f.write("[1,2,3]")
        with self.assertRaises(tc.StoreError):
            tc.load_registry()
        with open(os.path.join(self.task_dir, "tasks.json"), "w", encoding="utf-8") as f:
            f.write('{"schema_version": 99, "tasks": {}}')
        with self.assertRaises(tc.StoreError):
            tc.load_registry()
        # 条目非法 state → fail-closed
        bad = tc.empty_registry()
        bad["tasks"]["t-000000000001"] = _entry(state="bogus")
        self._write_registry(bad)
        with self.assertRaises(tc.StoreError):
            tc.load_registry()

    def test_S7_deny_secret(self):
        with self.assertRaises(tc.StoreError):
            tc.add_task(_cfg(), f"echo {_FAKE_SECRET}", workitem=None)
        # 整表保存前过 deny-list(failure_tail 含 secret 亦拒绝落盘)
        reg = tc.empty_registry()
        entry = _entry()
        entry["failure_tail"] = f"boom {_FAKE_SECRET}"
        reg["tasks"][entry["id"]] = entry
        with self.assertRaises(tc.StoreError):
            tc.save_registry_atomic(reg)


# ---------------------------------------------------------------------------
# R1-R10:runner 落账 + 熔断 + 回收 + 日志(临时目录集成)
# ---------------------------------------------------------------------------

class TaskRunnerTests(TaskIsoBase):
    """R1-R10 runner/回收/日志单测(§2.1)。"""

    def _seed_running(self, tid, **over):
        reg = tc.load_registry()
        entry = _entry(tid=tid, state="running", started_at="2026-08-21T10:00:00+00:00", **over)
        reg["tasks"][tid] = entry
        tc.save_registry_atomic(reg)

    def _status(self, tid):
        return tc.load_registry()["tasks"][tid]

    def test_R1_runner_done_ledger(self):
        self._seed_running("t-000000000001", command="sleep 0.1")
        rc = self._run_runner("t-000000000001", _cfg())
        self.assertEqual(rc, 0)
        t = self._status("t-000000000001")
        self.assertEqual(t["state"], "done")
        self.assertEqual(t["exit_code"], 0)
        self.assertIsNotNone(t["finished_at"])
        self.assertIsNone(t["failure_tail"])

    def test_R2_runner_failed_ledger(self):
        self._seed_running("t-000000000002", command="echo boom >&2; exit 3")
        rc = self._run_runner("t-000000000002", _cfg())
        self.assertEqual(rc, 0)
        t = self._status("t-000000000002")
        self.assertEqual(t["state"], "failed")
        self.assertEqual(t["exit_code"], 3)
        self.assertIsNotNone(t["finished_at"])
        self.assertIn("boom", t["failure_tail"] or "")

    def test_R3_runner_timeout(self):
        self._seed_running("t-000000000003", command="sleep 5", expected_seconds=1)
        rc = self._run_runner("t-000000000003", _cfg())
        self.assertEqual(rc, 0)
        t = self._status("t-000000000003")
        self.assertEqual(t["state"], "timeout")
        self.assertEqual(t["exit_code"], 124)

    def test_R4_runner_kill_on_timeout(self):
        marker = os.path.join(self.task_dir, "killed-marker")
        cmd = f'trap "" TERM; sleep 5; touch {marker}'
        self._seed_running("t-000000000004", command=cmd,
                           expected_seconds=1, kill_on_timeout=True)
        rc = self._run_runner("t-000000000004", _cfg())
        self.assertEqual(rc, 0)
        t = self._status("t-000000000004")
        self.assertEqual(t["state"], "timeout")  # 超时→timeout 终态判定不削弱
        self.assertEqual(t["exit_code"], 124)
        self.assertFalse(os.path.exists(marker))  # SIGKILL 保证进程确已死(SIGTERM 被忽略)

    def test_R5_reconcile_stale(self):
        reg = tc.load_registry()
        reg["tasks"]["t-000000000005"] = _entry(
            tid="t-000000000005", state="running", pid=999999,  # 死 pid → 立即回收
            started_at="2026-08-21T10:00:00+00:00")
        reg["tasks"]["t-000000000006"] = _entry(
            tid="t-000000000006", state="running", pid=None,  # 新提升窗口 → 不回收
            started_at=tc.now_iso())
        reg["tasks"]["t-000000000007"] = _entry(
            tid="t-000000000007", state="running", pid=None,  # 陈旧 pid=None(spawn 崩溃) → 回收
            started_at="2020-01-01T00:00:00+00:00")
        tc.save_registry_atomic(reg)
        reaped = tc.reconcile_running(reg)
        self.assertIn("t-000000000005", reaped)
        self.assertIn("t-000000000007", reaped)
        self.assertNotIn("t-000000000006", reaped)
        t5 = reg["tasks"]["t-000000000005"]
        self.assertEqual(t5["state"], "failed")
        self.assertIn("失联", t5["failure_tail"] or "")
        self.assertEqual(reg["tasks"]["t-000000000006"]["state"], "running")

    def test_R6_extract_tail(self):
        logf = os.path.join(self.task_dir, "logs")
        os.makedirs(logf, exist_ok=True)
        path = os.path.join(logf, "x.log")
        with open(path, "w", encoding="utf-8") as f:
            f.write("0123456789abcdef")
        self.assertEqual(tc.extract_tail(path, 6), "abcdef")  # 尾部 N 字节精确(末 6 字节)
        self.assertEqual(tc.extract_tail(path, 100), "0123456789abcdef")  # 超长不截断
        with open(path, "w", encoding="utf-8") as f:
            f.write("")  # 空文件
        self.assertEqual(tc.extract_tail(path, 6), "")
        self.assertEqual(tc.extract_tail(os.path.join(logf, "nope.log"), 6), "")  # 缺失 → 空串

    def test_R7_extract_tail_redact(self):
        logf = os.path.join(self.task_dir, "logs")
        os.makedirs(logf, exist_ok=True)
        path = os.path.join(logf, "x.log")
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"boom {_FAKE_SECRET} end")
        tail = tc.extract_tail(path, 2000)
        self.assertNotIn(_FAKE_SECRET, tail)
        self.assertIn("[REDACTED]", tail)

    def test_R8_extract_tail_binary(self):
        logf = os.path.join(self.task_dir, "logs")
        os.makedirs(logf, exist_ok=True)
        path = os.path.join(logf, "x.log")
        with open(path, "wb") as f:
            f.write(b"abc\xff\xfe\x00def")
        tail = tc.extract_tail(path, 2000)  # errors=replace 不抛
        self.assertIn("abc", tail)
        self.assertIn("def", tail)

    def test_R9_no_fcntl_degrade(self):
        saved = tc.fcntl
        tc.fcntl = None
        try:
            self.assertTrue(tc.pid_alive(12345))  # 恒真(非 POSIX 降级)
            ran = []

            def fn():
                ran.append(1)
                return 42

            self.assertEqual(tc.with_registry_flock(fn), 42)  # 无锁直执行不抛
            self.assertEqual(ran, [1])
        finally:
            tc.fcntl = saved

    def test_R10_runner_noop_terminal(self):
        reg = tc.load_registry()
        reg["tasks"]["t-00000000000a"] = _entry(
            tid="t-00000000000a", state="done", finished_at="2026-08-21T10:00:00+00:00")
        tc.save_registry_atomic(reg)
        rc = self._run_runner("t-00000000000a", _cfg())
        self.assertEqual(rc, 0)  # no-op exit 0(幂等)
        self.assertEqual(self._status("t-00000000000a")["state"], "done")  # 不被覆盖


if __name__ == "__main__":
    unittest.main()
