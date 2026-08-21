#!/usr/bin/env python3
"""flow-task-core.py + flow-core.py M2 单测:事件层 EV1-EV5 / 状态 ST1-ST2 / 种子 SD1-SD6 /
入队 EQ1-EQ6 / 宿主钩子 HK1-HK7(design .flow/workitems/queue-m2-async-first/design.md §2.1)。

用法: python3 -m unittest discover tests
零 API、全程 FLOW_TASK_DIR/FLOW_DATA_DIR/HOME 临时目录隔离 + stub(fc 子进程/notify)。
纯函数直接 import(seed_expected / update_seed / classify_event_state / render_wake_text);
I/O 用临时目录 + flock;入队侧 monkeypatch fc.subprocess.run 捕获命令串。
"""

import contextlib
import importlib.util
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_FLOW_CORE = os.path.join(_HERE, "..", "scripts", "flow-core.py")
_TASK_CORE = os.path.join(_HERE, "..", "scripts", "flow-task-core.py")

_fc_spec = importlib.util.spec_from_file_location("flow_core_m2", _FLOW_CORE)
fc = importlib.util.module_from_spec(_fc_spec)
_fc_spec.loader.exec_module(fc)

_tc_spec = importlib.util.spec_from_file_location("flow_task_core_m2", _TASK_CORE)
tc = importlib.util.module_from_spec(_tc_spec)
_tc_spec.loader.exec_module(tc)

# 故意拼接,避免源码静态 deny-list 命中(sk-[A-Za-z0-9]{10})
_FAKE_SECRET = "sk-" + "fakekey12345"

_ENV_PREFIXES = ("FLOW_", "COLLABFLOW_", "STUB_", "DSH_", "DEEPSEEK", "RX_")


def _tc_cfg(**kw):
    base = {"task": {"schema_version": 1, "max_parallel": 2, "default_priority": "P2",
                     "log_tail_bytes": 2000, "kill_grace_s": 1,
                     "expected_seconds_seed": {"design": 480, "execute": 1800},
                     "seed_history_len": 8},
            "host": {"notify": None, "wake_template": None}}
    base["task"].update(kw)
    return base


def _fc_cfg():
    return {
        "workitem": {"plane_id": "control", "lock_ttl_s": 3600, "id_max_len": 64},
        "gates": {"takeover_after_same_defect": 2, "iteration_limit": 3},
        "executor": {"default": "reasonix", "timeout_s": 1800, "diff_scope": None},
    }


def _fc_status(wi_id="w1", state="created"):
    return {
        "schema_version": 1, "id": wi_id, "state": state, "iteration": 0,
        "same_defect_count": 0, "primary_defect_type": None, "takeover": False,
        "re_execute_count": 0, "process_version": "1.0.0",
        "created_at": "2026-08-14T10:00:00+00:00",
        "updated_at": "2026-08-14T10:00:00+00:00",
        "event_seq": 1, "locked_by": None, "lock_expires_at": None,
    }


def _task_entry(tid="t-000000000001", state="queued", kind=None, workitem=None,
                command="sleep 0.01", expected_seconds=None,
                started_at=None, finished_at=None, exit_code=None, pid=None):
    return {
        "id": tid, "workitem": workitem, "command": command, "priority": "P2",
        "state": state, "kind": kind, "expected_seconds": expected_seconds,
        "kill_on_timeout": False, "created_at": "2026-08-21T10:00:00+00:00",
        "started_at": started_at, "finished_at": finished_at, "exit_code": exit_code,
        "failure_tail": None, "pid": pid, "heartbeat_at": None,
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


class M2IsoBase(unittest.TestCase):
    """FLOW_TASK_DIR + FLOW_DATA_DIR + HOME 临时目录隔离。"""

    def setUp(self):
        self._saved = {k: v for k, v in os.environ.items() if k.startswith(_ENV_PREFIXES)}
        for k in list(self._saved):
            os.environ.pop(k, None)
        self.tmp = tempfile.mkdtemp(prefix="flow-m2-")
        os.environ["FLOW_TASK_DIR"] = os.path.join(self.tmp, "task")
        os.environ["FLOW_DATA_DIR"] = os.path.join(self.tmp, ".flow")
        os.environ["HOME"] = os.path.join(self.tmp, "home")
        os.makedirs(os.environ["HOME"], exist_ok=True)

    def tearDown(self):
        for k in [k for k in list(os.environ) if k.startswith(_ENV_PREFIXES)]:
            os.environ.pop(k, None)
        os.environ.update(self._saved)
        shutil.rmtree(self.tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# 事件层(§1.3,EV1-EV5)
# ---------------------------------------------------------------------------

class EventLayerTests(M2IsoBase):
    """EV1-EV5:只增不删、seq 单调、并发安全、deny、坏行重放。"""

    def _append(self, tid, state="queued", n=1):
        for _ in range(n):
            tc.append_task_event(tid, {"schema_version": 1, "task_id": tid, "state": state})

    def test_EV1_append_seq_monotonic(self):
        tid = "t-000000000001"
        self._append(tid, n=3)
        path = tc.event_path(tid)
        lines = [l for l in open(path, encoding="utf-8") if l.strip()]
        self.assertEqual(len(lines), 3)
        self.assertEqual([json.loads(l)["seq"] for l in lines], [1, 2, 3])
        with open(path, "rb") as f:
            self.assertTrue(f.read().endswith(b"\n"))  # 行尾换行

    def test_EV2_append_only_no_truncate(self):
        tid = "t-000000000002"
        self._append(tid, n=2)
        path = tc.event_path(tid)
        with open(path, "rb") as f:
            before = f.read()
        self._append(tid, n=1)
        with open(path, "rb") as f:
            after = f.read()
        self.assertTrue(after.startswith(before))          # 旧行字节不变(open "a" 无截断)
        self.assertGreater(len(after), len(before))

    def test_EV3_deny_secret(self):
        tid = "t-000000000003"
        with self.assertRaises(tc.StoreError):
            tc.append_task_event(tid, {"task_id": tid, "diagnostic": f"boom {_FAKE_SECRET}"})
        path = tc.event_path(tid)
        self.assertFalse(os.path.exists(path))             # 拒绝写入,不落盘

    def test_EV4_concurrent_append(self):
        tid = "t-000000000004"
        n = 8
        errors = []

        def worker(i):
            try:
                tc.append_task_event(tid, {"task_id": tid, "state": f"s{i}"})
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        lines = [l for l in open(tc.event_path(tid), encoding="utf-8") if l.strip()]
        seqs = sorted(json.loads(l)["seq"] for l in lines)
        self.assertEqual(len(lines), n)
        self.assertEqual(seqs, list(range(1, n + 1)))      # 无重复无空洞

    def test_EV5_replay_corrupt_line(self):
        tid = "t-000000000005"
        self._append(tid, n=1)
        path = tc.event_path(tid)
        with open(path, "a", encoding="utf-8") as f:       # 手工插入坏行
            f.write("not-json-garbage\n")
        self._append(tid, n=1)
        recs = []
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    recs.append(json.loads(line))
                except ValueError:                          # 重放跳过坏行不崩
                    continue
        self.assertEqual([r["seq"] for r in recs], [1, 3])  # 坏行仍占行号,seq 不重号


# ---------------------------------------------------------------------------
# 状态分类(§1.4(3),ST1-ST2)
# ---------------------------------------------------------------------------

class StateTests(M2IsoBase):
    """ST1-ST2:终态分类纯函数 + partial-complete 检测(降级安全)。"""

    def test_ST1_classify_event_state(self):
        self.assertEqual(tc.classify_event_state(0, False), "done")
        self.assertEqual(tc.classify_event_state(3, False), "failed")
        self.assertEqual(tc.classify_event_state(124, False, False), "timeout")
        self.assertEqual(tc.classify_event_state(124, False, True), "partial-complete")
        self.assertEqual(tc.classify_event_state(124, True, True), "partial-complete")
        self.assertEqual(tc.classify_event_state(5, True, False), "timeout")

    def _write_result(self, wi_id, text):
        p = os.path.join(fc.workitems_dir(), wi_id, "executor", "result.json")
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            f.write(text)

    def test_ST2_is_partial_complete(self):
        entry = _task_entry(kind="execute", workitem="w1")
        # result.json 标 partial(两种 status 写法都认)
        self._write_result("w1", '{"status": "partial-complete", "partial_complete": true}')
        self.assertTrue(tc._is_partial_complete(entry, 124))
        self._write_result("w1", '{"status": "ok"}')
        self.assertFalse(tc._is_partial_complete(entry, 124))
        # 缺失/损坏 → False(降级 timeout,安全侧)
        self._write_result("w1", "{not-valid-json")
        self.assertFalse(tc._is_partial_complete(entry, 124))
        os.remove(os.path.join(fc.workitems_dir(), "w1", "executor", "result.json"))
        self.assertFalse(tc._is_partial_complete(entry, 124))
        # kind=null/generic / rc≠124 → False
        self.assertFalse(tc._is_partial_complete(_task_entry(workitem="w1"), 124))
        self.assertFalse(tc._is_partial_complete(_task_entry(kind="design", workitem="w1"), 124))
        self.assertFalse(tc._is_partial_complete(entry, 3))
        self.assertFalse(tc._is_partial_complete(_task_entry(kind="execute"), 124))


# ---------------------------------------------------------------------------
# 种子(§1.4(2),SD1-SD6)
# ---------------------------------------------------------------------------

class SeedTests(M2IsoBase):
    """SD1-SD6:EMA×1.5 + fallback 下限 + last 截断 + 损坏回退。"""

    def test_SD1_seed_empty_fallback(self):
        self.assertEqual(tc.seed_expected([], 480), 480)
        self.assertEqual(tc.seed_expected([], 1800), 1800)

    def test_SD2_seed_single(self):
        # [100] → EMA=100 → 1.5x=150;fallback 低时取 max(fallback,150)
        self.assertEqual(tc.seed_expected([100], 10), 150)

    def test_SD3_seed_ema_clamp(self):
        self.assertEqual(tc.seed_expected([100, 100], 10), 150)   # EMA 恒定 100
        self.assertEqual(tc.seed_expected([10, 10], 480), 480)    # 低于 fallback → clamp
        # EMA: 0.5*200 + 0.5*100 = 150 → 1.5x = 225
        self.assertEqual(tc.seed_expected([100, 200], 10), 225)

    def test_SD4_update_seed(self):
        store = {"schema_version": 1, "kinds": {}}
        s = tc.update_seed(store, "design", 100)
        s = tc.update_seed(s, "design", 200)
        self.assertEqual(s["kinds"]["design"]["count"], 2)
        self.assertEqual(s["kinds"]["design"]["last"], [100, 200])
        self.assertAlmostEqual(s["kinds"]["design"]["ema"], 150.0)
        # last 截断到 seed_history_len
        s = {"schema_version": 1, "kinds": {}}
        for d in range(1, 6):
            s = tc.update_seed(s, "execute", d * 10, max_len=3)
        k = s["kinds"]["execute"]
        self.assertEqual(k["last"], [30, 40, 50])
        self.assertEqual(k["count"], 5)
        self.assertAlmostEqual(k["ema"], 40.625)

    def test_SD5_update_seed_bad(self):
        store = {"schema_version": 1, "kinds": {}}
        for bad in (0, -1, "5", 1.5, None):
            s = tc.update_seed(store, "design", bad)
            self.assertEqual(s, store)                    # 非法 duration 不更新
        self.assertNotIn("design", store["kinds"])

    def test_SD6_seed_corrupt(self):
        os.makedirs(tc.task_dir(), exist_ok=True)
        with open(tc.seed_path(), "w", encoding="utf-8") as f:
            f.write("{not-valid-json")
        self.assertEqual(tc.load_seed(), tc.empty_seed())  # 非 JSON → 空(回退)
        self.assertEqual(tc.seed_expected(tc._load_seed_kind("design"), 480), 480)
        with open(tc.seed_path(), "w", encoding="utf-8") as f:
            f.write("[1,2,3]")
        self.assertEqual(tc.load_seed(), tc.empty_seed())  # 顶层非 dict → 空


# ---------------------------------------------------------------------------
# 入队(flow-core 侧,EQ1-EQ6)
# ---------------------------------------------------------------------------

class EnqueueTests(M2IsoBase):
    """EQ1-EQ6:命令串构造/quote 注入/重复入队/state 守卫/子进程失败/联动锁失败。"""

    def _fake_add(self, captured, rc=0, stdout=None, stderr=""):
        stdout = stdout or ('{"status":"ok","id":"t-00000000000a","state":"queued",'
                            '"workitem":"w1","priority":"P2","kind":"design",'
                            '"expected_seconds":480}')

        def fake_run(cmd, **kw):
            captured["cmd"] = list(cmd)
            return subprocess.CompletedProcess(cmd, rc, stdout=stdout, stderr=stderr)

        return fake_run

    def test_EQ1_build_command(self):
        captured = {}
        orig = fc.subprocess.run
        fc.subprocess.run = self._fake_add(captured)
        try:
            rc = fc._enqueue_workitem_op(
                _fc_cfg(), "design", "w1",
                ["workitem", "design", "w1", "--sync", "--json"],
                "design", False, "created")
        finally:
            fc.subprocess.run = orig
        self.assertEqual(rc, 0)
        cmd = captured["cmd"]
        self.assertTrue(os.path.isabs(cmd[0]))                 # flow 绝对路径
        self.assertTrue(cmd[0].endswith(os.path.join("scripts", "flow")))
        command = cmd[cmd.index("--command") + 1]
        parts = shlex.split(command)
        self.assertEqual(parts, [cmd[0], "workitem", "design", "w1", "--sync", "--json"])
        self.assertIn("--workitem", cmd)
        self.assertIn("w1", cmd)
        self.assertIn("--kind", cmd)
        self.assertIn("design", cmd)
        # 数据路径不拼入命令串(经 env 继承)
        for token in ("FLOW_DATA_DIR", "workitems", "tasks.json"):
            self.assertNotIn(token, command)

    def test_EQ2_quote_injection(self):
        captured = {}
        evil_executor = "a b;touch /tmp/flow-eq2-injected"
        orig = fc.subprocess.run
        fc.subprocess.run = self._fake_add(captured)
        try:
            rc = fc._enqueue_workitem_op(
                _fc_cfg(), "execute", "w1",
                ["workitem", "execute", "w1", "--sync", "--executor", evil_executor,
                 "--timeout", "7", "--json"],
                "execute", False, "translated", timeout=7)
        finally:
            fc.subprocess.run = orig
        self.assertEqual(rc, 0)
        cmd = captured["cmd"]
        command = cmd[cmd.index("--command") + 1]
        parts = shlex.split(command)                           # quote 后还原 argv 不破坏
        self.assertEqual(parts[1:], ["workitem", "execute", "w1", "--sync",
                                     "--executor", evil_executor, "--timeout", "7", "--json"])
        # execute 外层 expected-seconds = timeout+115(确定性安全网)
        self.assertIn("--expected-seconds", cmd)
        self.assertEqual(cmd[cmd.index("--expected-seconds") + 1], "122")

    def test_EQ3_duplicate_workitem(self):
        orig = fc.subprocess.run
        fc.subprocess.run = self._fake_add(
            {}, rc=2, stdout="",
            stderr='{"status":"failed","error":"duplicate_workitem",'
                   '"detail":"已有非终态任务 t-00000000000b (running)"}')
        try:
            rc = fc._enqueue_workitem_op(
                _fc_cfg(), "design", "w1",
                ["workitem", "design", "w1", "--sync", "--json"],
                "design", False, "created")
        finally:
            fc.subprocess.run = orig
        self.assertEqual(rc, 2)

    def test_EQ4_state_guard(self):
        # design 非 created(无 force) → exit 2,不调用子进程
        wi_dir = os.path.join(fc.workitems_dir(), "w1")
        os.makedirs(wi_dir, exist_ok=True)
        fc.save_status_atomic(wi_dir, _fc_status("w1", "designed"))
        calls = []

        def fake_run(*a, **k):
            calls.append(a)
            return subprocess.CompletedProcess([], 0)

        orig = fc.subprocess.run
        fc.subprocess.run = fake_run
        try:
            self.assertEqual(fc.cmd_design(["w1"], _fc_cfg()), 2)
            # execute 非 translated(无 force) → exit 2,不调用子进程
            fc.save_status_atomic(wi_dir, _fc_status("w1", "created"))
            self.assertEqual(fc.cmd_execute(["w1", "--executor", "stub"], _fc_cfg()), 2)
        finally:
            fc.subprocess.run = orig
        self.assertEqual(calls, [])                            # 全程未触碰子进程

    def test_EQ5_subprocess_fail(self):
        def boom(*a, **k):
            raise OSError("no such file or directory")

        orig = fc.subprocess.run
        fc.subprocess.run = boom
        try:
            rc = fc._enqueue_workitem_op(
                _fc_cfg(), "design", "w1",
                ["workitem", "design", "w1", "--sync", "--json"],
                "design", False, "created")
        finally:
            fc.subprocess.run = orig
        self.assertEqual(rc, 1)

    def test_EQ6_linkage_lock_fail(self):
        """子进程 --sync 转移被锁拒 → 任务 failed(runner 侧),workitem 状态不变。"""
        # stub dsh(供 flow workitem design --sync 的同步链路)
        stub_dsh = os.path.join(self.tmp, "stub-dsh.sh")
        with open(stub_dsh, "w", encoding="utf-8") as f:
            f.write("#!/usr/bin/env bash\ncat << 'MD'\n# stub 方案\nMD\nexit 0\n")
        os.chmod(stub_dsh, 0o755)
        os.environ["DSH_BIN"] = stub_dsh
        os.environ["DSH_HOME"] = os.path.join(self.tmp, "dshhome")
        os.environ["DSH_DESIGN_PRO_PATCH"] = os.path.join(self.tmp, "pro.patch.yml")
        os.environ["DEEPSEEK_API_KEY"] = _FAKE_SECRET
        # workitem w1 created + 持锁(未过期)
        wi_dir = os.path.join(fc.workitems_dir(), "w1")
        os.makedirs(wi_dir, exist_ok=True)
        st = _fc_status("w1", "created")
        st["locked_by"] = "someone"
        st["lock_expires_at"] = "2099-01-01T00:00:00+00:00"
        fc.save_status_atomic(wi_dir, st)
        # 种子任务(running 状态,直接写注册表避免 add_task 自动 spawn)
        flow_bin = os.path.join(_HERE, "..", "scripts", "flow")
        tid = "t-0000000000a1"
        reg = tc.empty_registry()
        reg["tasks"][tid] = _task_entry(
            tid=tid, state="running", kind="design", workitem="w1",
            command=f"{flow_bin} workitem design w1 --sync --json",
            expected_seconds=30, started_at="2026-08-21T10:00:00+00:00")
        tc.save_registry_atomic(reg)
        os.makedirs(tc.logs_dir(), exist_ok=True)
        with _FDRedirect(tc.log_path(tid)):
            self.assertEqual(tc._runner(tid, _tc_cfg()), 0)
        t = tc.load_registry()["tasks"][tid]
        self.assertEqual(t["state"], "failed")                 # 转移失败 → 任务 failed
        self.assertEqual(fc.load_status(wi_dir)["state"], "created")  # workitem 不变


# ---------------------------------------------------------------------------
# 宿主钩子(§1.5,HK1-HK7)
# ---------------------------------------------------------------------------

class HostHookTests(M2IsoBase):
    """HK1-HK7:wake 模板渲染 + notify 触发/失败/控制字符拒绝。"""

    def _ctx(self):
        return {"task_id": "t-000000000001", "workitem": "w1", "kind": "design",
                "state": "running", "exit_code": "", "elapsed_seconds": "3",
                "expected_seconds": "480", "log_path": "/tmp/l.log",
                "event_path": "/tmp/e.jsonl", "next_command": "flow workitem design w1 --check"}

    def test_HK1_render_default(self):
        text = tc.render_wake_text(None, self._ctx())
        self.assertIn("t-000000000001", text)
        self.assertIn("running", text)
        self.assertIn("flow workitem design w1 --check", text)
        self.assertIn("下一步", text)

    def test_HK2_render_custom(self):
        text = tc.render_wake_text("自定义模板: task={task_id} state={state}", self._ctx())
        self.assertEqual(text, "自定义模板: task=t-000000000001 state=running")

    def test_HK3_missing_var(self):
        text = tc.render_wake_text("未知 {foo} 保留", self._ctx())
        self.assertEqual(text, "未知 {foo} 保留")               # 未知占位符保留字面量

    def test_HK4_notify_disabled(self):
        calls = []
        orig = tc.subprocess.run
        tc.subprocess.run = lambda *a, **k: calls.append(a) or subprocess.CompletedProcess([], 0)
        try:
            tc._notify_if_configured({"host": {"notify": None}}, {"task_id": "t-1"})
            tc._notify_if_configured({"host": {}}, {"task_id": "t-1"})
        finally:
            tc.subprocess.run = orig
        self.assertEqual(calls, [])                             # 未配置 → 零调用

    def test_HK5_notify_stdin(self):
        stub = os.path.join(self.tmp, "stub-notify.sh")
        record = os.path.join(self.tmp, "rec.json")
        with open(stub, "w", encoding="utf-8") as f:
            f.write("#!/usr/bin/env bash\ncat > \"$1\"\n")
        os.chmod(stub, 0o755)
        event = {"task_id": "t-000000000002", "state": "done", "exit_code": 0}
        tc._notify_if_configured({"host": {"notify": f"{stub} {record}"}}, event)
        with open(record, encoding="utf-8") as f:
            self.assertEqual(json.load(f), event)  # stdin 收到事件 JSON

    def test_HK6_notify_fail(self):
        stub = os.path.join(self.tmp, "stub-fail.sh")
        with open(stub, "w", encoding="utf-8") as f:
            f.write("#!/usr/bin/env bash\nexit 3\n")
        os.chmod(stub, 0o755)
        # exit≠0 → 不抛(仅告警,不影响任务终态)
        tc._notify_if_configured({"host": {"notify": stub}}, {"task_id": "t-1"})

    def test_HK7_notify_control_chars(self):
        calls = []
        orig = tc.subprocess.run
        tc.subprocess.run = lambda *a, **k: calls.append(a) or subprocess.CompletedProcess([], 0)
        try:
            tc._notify_if_configured({"host": {"notify": "bad\x00template"}},
                                     {"task_id": "t-1"})
            tc._notify_if_configured({"host": {"notify": "bad\x1ftemplate"}},
                                     {"task_id": "t-1"})
        finally:
            tc.subprocess.run = orig
        self.assertEqual(calls, [])                             # 控制字符 → 拒绝执行(fail-closed)


if __name__ == "__main__":
    unittest.main(verbosity=2)
