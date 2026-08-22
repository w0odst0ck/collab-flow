#!/usr/bin/env python3
"""flow-task-core.py + flow-core.py flow-queue-obsv 单测:心跳 HB1-HB4 / workdir WD1-WD2 /
prune PR1-PR7 / DENY_RE 收紧 DN1-DN3(design .flow/workitems/flow-queue-obsv/design.md §4)。

用法: python3 -m unittest discover tests
零 API、全程 FLOW_TASK_DIR 临时目录隔离。纯函数直接 import;心跳/runner 集成进程内调用;
入队侧 monkeypatch fc.subprocess.run 捕获命令串。task add 用 max_parallel=0 避免 spawn 子进程。
"""

import contextlib
import importlib.util
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
_FLOW_CORE = os.path.join(_HERE, "..", "scripts", "flow-core.py")
_TASK_CORE = os.path.join(_HERE, "..", "scripts", "flow-task-core.py")

_fc_spec = importlib.util.spec_from_file_location("flow_core_obsv", _FLOW_CORE)
fc = importlib.util.module_from_spec(_fc_spec)
_fc_spec.loader.exec_module(fc)

_tc_spec = importlib.util.spec_from_file_location("flow_task_core_obsv", _TASK_CORE)
tc = importlib.util.module_from_spec(_tc_spec)
_tc_spec.loader.exec_module(tc)

# 拼接避免源码静态 deny-list 命中;真 key 形状 sk- + 32 字符(主流最短,§1.4)
_REAL_KEY = "sk-" + "k" * 32
# workitem 名含 sk-observabil(10 字符)→ 收紧前误报,收紧后放行(真实 bug 复现)
_WORKITEM_OBSV = "flow-task-observability"


def _cfg(**kw):
    base = {"task": {"schema_version": 1, "max_parallel": 2, "default_priority": "P2",
                     "log_tail_bytes": 2000, "kill_grace_s": 1}}
    base["task"].update(kw)
    return base


def _fc_cfg():
    return {
        "workitem": {"plane_id": "control", "lock_ttl_s": 3600, "id_max_len": 64},
        "gates": {"takeover_after_same_defect": 2, "iteration_limit": 3},
        "executor": {"default": "reasonix", "timeout_s": 1800, "diff_scope": None},
    }


def _entry(tid="t-000000000001", state="queued", priority="P2", workitem=None,
           command="sleep 0.01", created_at="2026-08-21T10:00:00+00:00",
           started_at=None, finished_at=None, exit_code=None, failure_tail=None,
           expected_seconds=None, kill_on_timeout=False, pid=None, workdir=None):
    return {
        "id": tid, "workitem": workitem, "command": command, "priority": priority,
        "state": state, "expected_seconds": expected_seconds,
        "kill_on_timeout": kill_on_timeout, "created_at": created_at,
        "started_at": started_at, "finished_at": finished_at, "exit_code": exit_code,
        "failure_tail": failure_tail, "pid": pid, "heartbeat_at": None,
        "workdir": workdir,
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


class ObsvIsoBase(unittest.TestCase):
    """FLOW_TASK_DIR 临时目录隔离(同 TaskIsoBase 约定)。"""

    def setUp(self):
        self._old_env = os.environ.get("FLOW_TASK_DIR")
        self.task_dir = tempfile.mkdtemp(prefix="flow-obsv-")
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

    def _seed_running(self, tid, **over):
        reg = tc.load_registry()
        entry = _entry(tid=tid, state="running", started_at=tc.now_iso(), **over)
        reg["tasks"][tid] = entry
        tc.save_registry_atomic(reg)

    def _run_runner(self, tid, cfg, heartbeat_interval=30):
        os.makedirs(tc.logs_dir(), exist_ok=True)
        with _FDRedirect(tc.log_path(tid)):
            return tc._runner(tid, cfg, heartbeat_interval=heartbeat_interval)


# ---------------------------------------------------------------------------
# HB1-HB4:运行中心跳(§1.1)
# ---------------------------------------------------------------------------

class HeartbeatTests(ObsvIsoBase):
    """HB1-HB4:interval 注入推进 heartbeat_at / runner 集成 / 写失败静默 / 终态停写。"""

    def test_HB1_heartbeat_advances(self):
        """running 任务 heartbeat_at 随时间前进(interval 注入,§4 验收 1)。"""
        tid = "t-000000000001"
        self._seed_running(tid)
        stop = threading.Event()
        th = threading.Thread(target=tc._heartbeat_loop, args=(tid, stop),
                              kwargs={"interval": 0.05}, daemon=True)
        th.start()
        try:
            time.sleep(0.2)
            hb1 = tc.load_registry()["tasks"][tid]["heartbeat_at"]
            time.sleep(0.2)
            hb2 = tc.load_registry()["tasks"][tid]["heartbeat_at"]
        finally:
            stop.set()
        th.join(timeout=2)
        self.assertFalse(th.is_alive())
        self.assertIsNotNone(hb1)
        self.assertIsNotNone(hb2)
        self.assertGreater(datetime.fromisoformat(hb2),
                           datetime.fromisoformat(hb1))  # 随时间前进

    def test_HB2_runner_heartbeat(self):
        """_runner 集成:运行期间心跳前进;命令结束终态落账(join 不阻塞)。"""
        tid = "t-000000000002"
        self._seed_running(tid, command="sleep 0.4")
        rc = self._run_runner(tid, _cfg(), heartbeat_interval=0.05)
        self.assertEqual(rc, 0)
        t = tc.load_registry()["tasks"][tid]
        self.assertEqual(t["state"], "done")
        self.assertIsNotNone(t["heartbeat_at"])
        self.assertGreater(datetime.fromisoformat(t["heartbeat_at"]),
                           datetime.fromisoformat(t["started_at"]))  # 运行中更新过

    def test_HB3_heartbeat_silent_retry(self):
        """写 registry 失败(锁冲突模拟)静默重试,不崩;恢复后成功写入(§3 错误表 #1)。"""
        tid = "t-000000000003"
        self._seed_running(tid)
        stop = threading.Event()
        orig = tc.save_registry_atomic
        calls = []

        def flaky(reg):
            calls.append(1)
            if len(calls) <= 2:
                raise tc.StoreError("锁冲突模拟")
            orig(reg)

        tc.save_registry_atomic = flaky
        th = threading.Thread(target=tc._heartbeat_loop, args=(tid, stop),
                              kwargs={"interval": 0.05}, daemon=True)
        th.start()
        try:
            time.sleep(0.35)
        finally:
            stop.set()
            tc.save_registry_atomic = orig
        th.join(timeout=2)
        self.assertFalse(th.is_alive())  # 线程不因异常退出
        self.assertGreater(len(calls), 2)  # 失败后仍继续尝试
        self.assertIsNotNone(tc.load_registry()["tasks"][tid]["heartbeat_at"])

    def test_HB4_stop_after_terminal(self):
        """任务终态后心跳停止更新(不再写 registry)。"""
        tid = "t-000000000004"
        self._seed_running(tid)
        stop = threading.Event()
        th = threading.Thread(target=tc._heartbeat_loop, args=(tid, stop),
                              kwargs={"interval": 0.05}, daemon=True)
        th.start()
        time.sleep(0.15)  # 至少一拍(running)
        reg = tc.load_registry()
        reg["tasks"][tid]["state"] = "done"
        tc.save_registry_atomic(reg)
        hb1 = tc.load_registry()["tasks"][tid]["heartbeat_at"]
        time.sleep(0.15)
        stop.set()
        th.join(timeout=2)
        hb2 = tc.load_registry()["tasks"][tid]["heartbeat_at"]
        self.assertEqual(hb1, hb2)  # 终态后零更新


# ---------------------------------------------------------------------------
# WD1-WD2:task workdir 记录(§1.2)
# ---------------------------------------------------------------------------

class WorkdirTests(ObsvIsoBase):
    """WD1-WD2:add --workdir 落 entry;list --json 输出 workdir。"""

    def test_WD1_add_workdir(self):
        """add_task 带 workdir → registry 有值;默认 os.getcwd()(max_parallel=0 免 spawn)。"""
        tid = tc.add_task(_cfg(max_parallel=0), "sleep 0.01",
                          workitem="wd1", workdir="/tmp/proj-a")
        t = tc.load_registry()["tasks"][tid]
        self.assertEqual(t["workdir"], "/tmp/proj-a")
        tid2 = tc.add_task(_cfg(max_parallel=0), "sleep 0.01", workitem="wd2")
        self.assertEqual(tc.load_registry()["tasks"][tid2]["workdir"], os.getcwd())

    def test_WD2_cmd_add_and_list_json(self):
        """cmd_add --workdir + cmd_list --json 均输出 workdir(§4 验收 2)。"""
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            # flow-task-ledger 门禁(§1.4):补 kind/priority/expected/why + 项目路径锚定
            # + --force(自由命令不在模板白名单,显式撬开;audit.force_reason 落账)
            rc = tc.cmd_add(["--command", "FLOW_WORKDIR=/projects/wd2 sleep 0.01",
                             "--workitem", "wd2", "--workdir", "/opt/proj-b",
                             "--kind", "design", "--priority", "P2",
                             "--expected-seconds", "30", "--why", "test",
                             "--force", "--force-reason", "test", "--json"],
                            _cfg(max_parallel=0))
        self.assertEqual(rc, 0)
        info = json.loads(buf.getvalue().strip().splitlines()[-1])
        self.assertEqual(info["workdir"], "/opt/proj-b")
        tid = info["id"]
        self.assertEqual(tc.load_registry()["tasks"][tid]["workdir"], "/opt/proj-b")
        buf2 = io.StringIO()
        with contextlib.redirect_stdout(buf2):
            rc = tc.cmd_list(["--json"], _cfg(max_parallel=0))
        self.assertEqual(rc, 0)
        out = json.loads(buf2.getvalue().strip().splitlines()[-1])
        self.assertEqual(out["tasks"][0]["workdir"], "/opt/proj-b")


# ---------------------------------------------------------------------------
# PR1-PR7:终态任务清理 prune(§1.3)
# ---------------------------------------------------------------------------

class PruneTests(ObsvIsoBase):
    """PR1-PR7:删终态保 running/queued;--state/--older-than;无匹配幂等;非法 state exit 2。"""

    def _seed_mixed(self):
        reg = tc.empty_registry()
        reg["tasks"]["t-000000000001"] = _entry(
            tid="t-000000000001", state="done",
            finished_at="2026-08-20T10:00:00+00:00")
        reg["tasks"]["t-000000000002"] = _entry(
            tid="t-000000000002", state="failed",
            finished_at="2026-08-19T10:00:00+00:00")
        reg["tasks"]["t-000000000003"] = _entry(
            tid="t-000000000003", state="timeout",
            finished_at="2026-08-18T10:00:00+00:00")
        reg["tasks"]["t-000000000004"] = _entry(
            tid="t-000000000004", state="running",
            started_at="2026-08-21T09:00:00+00:00")
        reg["tasks"]["t-000000000005"] = _entry(tid="t-000000000005", state="queued")
        tc.save_registry_atomic(reg)

    def test_PR1_prune_terminals_only(self):
        """默认删全部终态;running/queued 绝不删(§4 验收 3)。"""
        self._seed_mixed()
        reg = tc.load_registry()
        removed = tc.prune_tasks(reg)
        self.assertEqual(sorted(removed),
                         ["t-000000000001", "t-000000000002", "t-000000000003"])
        self.assertIn("t-000000000004", reg["tasks"])
        self.assertIn("t-000000000005", reg["tasks"])

    def test_PR2_state_filter(self):
        """--state failed 只删 failed。"""
        self._seed_mixed()
        reg = tc.load_registry()
        removed = tc.prune_tasks(reg, states=("failed",))
        self.assertEqual(removed, ["t-000000000002"])
        self.assertIn("t-000000000001", reg["tasks"])
        self.assertIn("t-000000000003", reg["tasks"])

    def test_PR3_older_than(self):
        """--older-than N 只删 finished_at 早于 N 天前的(§4 验收 3)。"""
        self._seed_mixed()
        reg = tc.load_registry()
        now = datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)
        removed = tc.prune_tasks(reg, older_than_days=2, now=now)
        self.assertEqual(removed, ["t-000000000002", "t-000000000003"])  # 1 天前的不删
        self.assertIn("t-000000000001", reg["tasks"])
        self.assertIn("t-000000000004", reg["tasks"])

    def test_PR4_no_match_idempotent(self):
        """无匹配 → exit 0 输出 "无匹配"(幂等,§3 错误表 #4)。"""
        self._seed_mixed()
        reg = tc.load_registry()
        reg["tasks"].clear()
        tc.save_registry_atomic(reg)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = tc.cmd_prune(["--force"], _cfg())
        self.assertEqual(rc, 0)
        self.assertIn("无匹配", buf.getvalue())

    def test_PR5_invalid_state(self):
        """--state 非法值 → 用法错误 exit 2(§3 错误表 #3)。"""
        self.assertEqual(tc.cmd_prune(["--state", "bogus"], _cfg()), 2)
        self.assertEqual(tc.cmd_prune(["--state", "queued"], _cfg()), 2)  # 非终态白名单外

    def test_PR6_missing_finished_conservative(self):
        """--older-than 时 finished_at 缺失/非法/naive 的终态保守不删(不崩溃)。"""
        reg = tc.empty_registry()
        reg["tasks"]["t-000000000001"] = _entry(tid="t-000000000001", state="done",
                                                finished_at=None)
        reg["tasks"]["t-000000000002"] = _entry(tid="t-000000000002", state="failed",
                                                finished_at="not-a-date")
        reg["tasks"]["t-000000000003"] = _entry(tid="t-000000000003", state="timeout",
                                                finished_at="2026-08-20T10:00:00")  # naive,无时区
        tc.save_registry_atomic(reg)
        removed = tc.prune_tasks(reg, older_than_days=1)
        self.assertEqual(removed, [])
        self.assertIn("t-000000000001", reg["tasks"])
        self.assertIn("t-000000000002", reg["tasks"])
        self.assertIn("t-000000000003", reg["tasks"])

    def test_PR7_cmd_prune_keeps_running(self):
        """cmd_prune 实际删除:终态移除、running 保留(显式 --force)。"""
        self._seed_mixed()
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = tc.cmd_prune(["--force"], _cfg())  # 显式 force,不依赖 stdin tty 状态
        self.assertEqual(rc, 0)
        self.assertIn("pruned: 3", buf.getvalue())
        reg = tc.load_registry()
        self.assertNotIn("t-000000000001", reg["tasks"])
        self.assertNotIn("t-000000000002", reg["tasks"])
        self.assertNotIn("t-000000000003", reg["tasks"])
        self.assertIn("t-000000000004", reg["tasks"])  # running 绝不删
        self.assertIn("t-000000000005", reg["tasks"])  # queued 绝不删


# ---------------------------------------------------------------------------
# DN1-DN3:DENY_RE 收紧,防 workitem 名误报(§1.4)
# ---------------------------------------------------------------------------

class DenyReTests(ObsvIsoBase):
    """DN1-DN3:sk-+32 真 key 仍拒绝;flow-task-observability 名不误报正常入队。"""

    def test_DN1_real_key_rejected(self):
        """真 key(sk- + 32 字符)仍被拒绝(fail-closed,§4 验收 4)。"""
        with self.assertRaises(tc.StoreError):
            tc.add_task(_cfg(max_parallel=0), f"echo {_REAL_KEY}", workitem=None)
        # flow-core 入队侧同样拒绝
        rc = fc._enqueue_workitem_op(
            _fc_cfg(), "design", "w1",
            ["workitem", "design", "w1", "--sync", "--model", _REAL_KEY, "--json"],
            "design", False, "created")
        self.assertEqual(rc, 2)

    def test_DN2_workitem_name_no_false_positive(self):
        """workitem 名 flow-task-observability(含 sk-observabil,10 字符)不误报,正常入队。"""
        self.assertIsNone(fc.DENY_RE.search(_WORKITEM_OBSV))
        tid = tc.add_task(_cfg(max_parallel=0), "sleep 0.01", workitem=_WORKITEM_OBSV)
        t = tc.load_registry()["tasks"][tid]
        self.assertEqual(t["workitem"], _WORKITEM_OBSV)  # 入队成功

    def test_DN3_enqueue_workitem_name_ok(self):
        """flow-core 入队命令含该 workitem 名不误报(2026-08-21 实测 bug 回归)。"""
        captured = {}
        stdout = ('{"status":"ok","id":"t-00000000000a","state":"queued",'
                  '"workitem":"%s","priority":"P2","kind":"design",'
                  '"expected_seconds":480}' % _WORKITEM_OBSV)

        def fake_run(cmd, **kw):
            captured["cmd"] = list(cmd)
            return subprocess.CompletedProcess(cmd, 0, stdout=stdout, stderr="")

        orig = fc.subprocess.run
        fc.subprocess.run = fake_run
        try:
            rc = fc._enqueue_workitem_op(
                _fc_cfg(), "design", _WORKITEM_OBSV,
                ["workitem", "design", _WORKITEM_OBSV, "--sync", "--json"],
                "design", False, "created")
        finally:
            fc.subprocess.run = orig
        self.assertEqual(rc, 0)  # 不误报 → 正常入队


if __name__ == "__main__":
    unittest.main()
