#!/usr/bin/env python3
"""flow-task-core.py task-ledger 升级单测(flow-cost-ledger §2.1):门禁 G1-G11 /
入队 scheduled S1-S4 / pump P1-P5 / reschedule R1-R3 / cost C1-C5 / run-id B1 / suggest_wake W1。

用法: python3 -m unittest discover tests(连字符文件由 tests/test_task_ledger.py 桥接加载)
零 API、全程 FLOW_TASK_DIR/FLOW_DATA_DIR/HOME 临时目录隔离 + stub(sleep / sh / 手写
reasonix log / 进程内 _runner),pump 时间用注入 now;不触真实 ~/.collabflow/~/.reasonix。
"""

import contextlib
import fcntl
import importlib.util
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
_TASK_CORE = os.path.join(_HERE, "..", "scripts", "flow-task-core.py")
_WINDOW = os.path.join(_HERE, "..", "scripts", "window.py")

_tc_spec = importlib.util.spec_from_file_location("flow_task_core_tl", _TASK_CORE)
tc = importlib.util.module_from_spec(_tc_spec)
_tc_spec.loader.exec_module(tc)

_w_spec = importlib.util.spec_from_file_location("window_tl", _WINDOW)
window = importlib.util.module_from_spec(_w_spec)
_w_spec.loader.exec_module(window)

_FAKE_SECRET = "sk-" + "fakekey1234567890123456789"  # ≥16 字符模拟真 key,规避源码静态 deny-list

_ENV_PREFIXES = ("FLOW_", "COLLABFLOW_", "STUB_", "DSH_", "DEEPSEEK", "RX_")


def _cfg(**task_over):
    task = {"schema_version": 1, "max_parallel": 2, "default_priority": "P2",
            "log_tail_bytes": 2000, "kill_grace_s": 1,
            "expected_seconds_seed": {"design": 480, "execute": 1800},  # 2026-08-22 收窄
            "seed_history_len": 8, "queue_cap": 50}
    task.update(task_over)
    return {"task": task, "host": {"notify": None, "wake_template": None}}


def _entry(tid="t-000000000001", state="queued", workitem=None, kind="design",
           command="sleep 0.01", priority="P2", expected_seconds=30,
           scheduled_at=None, created_at="2026-08-21T10:00:00+00:00",
           started_at=None, finished_at=None, exit_code=None, workdir=None):
    return {"id": tid, "workitem": workitem, "command": command,
            "priority": priority, "state": state, "kind": kind,
            "expected_seconds": expected_seconds, "kill_on_timeout": False,
            "workdir": workdir,
            "scheduled_at": scheduled_at, "why": "test", "cost_usd": None,
            "audit": None,
            "created_at": created_at, "started_at": started_at,
            "finished_at": finished_at, "exit_code": exit_code,
            "failure_tail": None, "pid": None, "heartbeat_at": None}


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


class LedgerIsoBase(unittest.TestCase):
    """FLOW_TASK_DIR + FLOW_DATA_DIR + HOME 临时目录隔离(不触真实目录)。"""

    def setUp(self):
        self._saved = {k: v for k, v in os.environ.items() if k.startswith(_ENV_PREFIXES)}
        for k in list(self._saved):
            os.environ.pop(k, None)
        self._home_old = os.environ.get("HOME")
        self.tmp = tempfile.mkdtemp(prefix="flow-tl-")
        self.task_dir = os.path.join(self.tmp, "task")
        self.data = os.path.join(self.tmp, ".flow")
        self.home = os.path.join(self.tmp, "home")
        os.environ["FLOW_TASK_DIR"] = self.task_dir
        os.environ["FLOW_DATA_DIR"] = self.data
        os.environ["HOME"] = self.home
        os.makedirs(self.home, exist_ok=True)

    def tearDown(self):
        for k in [k for k in list(os.environ) if k.startswith(_ENV_PREFIXES)]:
            os.environ.pop(k, None)
        if self._home_old is None:
            os.environ.pop("HOME", None)
        else:
            os.environ["HOME"] = self._home_old
        os.environ.update(self._saved)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _mk_wi(self, wi_id):
        """创建 workitem 目录(锚定校验用):.flow/workitems/<id>/status.yaml。"""
        d = os.path.join(self.data, "workitems", wi_id)
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "status.yaml"), "w", encoding="utf-8") as f:
            f.write("state: created\n")

    def _write_reg(self, reg):
        os.makedirs(self.task_dir, exist_ok=True)
        with open(os.path.join(self.task_dir, "tasks.json"), "w", encoding="utf-8") as f:
            json.dump(reg, f, ensure_ascii=False)

    def _valid_opts(self, **over):
        """过门禁的基线 opts(自由命令 + 项目路径锚定 + --force,聚焦被测步骤)。"""
        opts = {"command": "FLOW_WORKDIR=/projects/demo sleep 0.01",
                "kind": "design", "priority": "P2", "expected-seconds": "30",
                "why": "test", "force": True, "force-reason": "test"}
        opts.update(over)
        return opts

    def _run_runner(self, tid, cfg):
        os.makedirs(tc.logs_dir(), exist_ok=True)
        with _FDRedirect(tc.log_path(tid)):
            return tc._runner(tid, cfg)


# ---------------------------------------------------------------------------
# 门禁校验链 G1-G11(§1.4,fail-closed)
# ---------------------------------------------------------------------------

class GateTests(LedgerIsoBase):
    """G1-G11:10 步顺序链,任一不过抛 GateReject;--force 仅撬模板白名单。"""

    def test_G1_kind_whitelist(self):
        # 收窄白名单(2026-08-22 用户拍板):reminder/verify/review/batch 全拒,design/execute 过
        for k, hint in (("reminder", "提醒请走 cron"),
                        ("verify", "同步命令,不入队"),
                        ("review", "同步命令,不入队"),
                        ("batch", "触发动作非任务")):
            with self.assertRaises(tc.GateReject) as cm:
                tc.gate_validate(self._valid_opts(kind=k), _cfg())
            self.assertEqual(cm.exception.args[0], "invalid_kind", k)
            self.assertIn(hint, cm.exception.args[1], k)
        with self.assertRaises(tc.GateReject):
            tc.gate_validate(self._valid_opts(kind=None), _cfg())
        # 合法 kind(design/execute)全过
        for k in ("design", "execute"):
            tc.gate_validate(self._valid_opts(kind=k), _cfg())

    def test_G2_command_empty(self):
        for cmd in ("", None, "   "):
            with self.assertRaises(tc.GateReject) as cm:
                tc.gate_validate(self._valid_opts(command=cmd), _cfg())
            self.assertEqual(cm.exception.args[0], "empty_command")

    def test_G3_bash_syntax(self):
        tc.gate_validate(self._valid_opts(
            command="FLOW_WORKDIR=/projects/demo sleep 5"), _cfg())  # 过
        with self.assertRaises(tc.GateReject) as cm:
            tc.gate_validate(self._valid_opts(command='for ((;"))'), _cfg())
        self.assertEqual(cm.exception.args[0], "command_syntax_error")

    def test_G4_anchor(self):
        # 无 workitem 且 command 无项目路径 → 拒
        with self.assertRaises(tc.GateReject) as cm:
            tc.gate_validate(self._valid_opts(command="sleep 1", workitem=None), _cfg())
        self.assertEqual(cm.exception.args[0], "not_anchored")
        # command 含项目路径 → 过
        tc.gate_validate(self._valid_opts(command="FLOW_WORKDIR=/projects/demo sleep 1",
                                          workitem=None), _cfg())
        tc.gate_validate(self._valid_opts(command="cd /projects/alpha && make test",
                                          workitem=None), _cfg())
        # workitem 存在 → 过;不存在且命令无项目路径 → 拒
        self._mk_wi("w1")
        tc.gate_validate(self._valid_opts(workitem="w1"), _cfg())
        with self.assertRaises(tc.GateReject) as cm:
            tc.gate_validate(self._valid_opts(command="sleep 1", workitem="nope"), _cfg())
        self.assertEqual(cm.exception.args[0], "not_anchored")

    def test_G5_priority_required(self):
        with self.assertRaises(tc.GateReject) as cm:
            tc.gate_validate(self._valid_opts(priority=None), _cfg())
        self.assertEqual(cm.exception.args[0], "priority_required")  # 不猜默认 P2
        with self.assertRaises(tc.GateReject):
            tc.gate_validate(self._valid_opts(priority="P3"), _cfg())

    def test_G6_expected_required(self):
        for bad in (None, "0", "-1", "abc", "1.5"):
            with self.assertRaises(tc.GateReject) as cm:
                tc.gate_validate(self._valid_opts(**{"expected-seconds": bad}), _cfg())
            self.assertEqual(cm.exception.args[0], "expected_required", bad)

    def test_G7_idempotent_nonterminal(self):
        self._mk_wi("w1")
        for st in ("scheduled", "queued", "running"):
            reg = tc.empty_registry()
            reg["tasks"]["t-000000000001"] = _entry(state=st, workitem="w1")
            self._write_reg(reg)
            with self.assertRaises(tc.GateReject) as cm:
                tc.gate_validate(self._valid_opts(workitem="w1"), _cfg())
            self.assertEqual(cm.exception.args[0], "duplicate_workitem", st)
            self.assertIn("t-000000000001", cm.exception.args[1])
        # done 不阻塞(重跑路径)
        reg = tc.empty_registry()
        reg["tasks"]["t-000000000001"] = _entry(state="done", workitem="w1")
        self._write_reg(reg)
        tc.gate_validate(self._valid_opts(workitem="w1"), _cfg())

    def test_G8_queue_cap(self):
        self._mk_wi("new")
        cap_cfg = _cfg(queue_cap=3)
        reg = tc.empty_registry()
        for i in range(3):
            reg["tasks"][f"t-00000000000{i + 1}"] = _entry(
                state="queued", workitem=f"q{i}", kind="design")
        self._write_reg(reg)
        with self.assertRaises(tc.GateReject) as cm:
            tc.gate_validate(self._valid_opts(workitem="new"), cap_cfg)
        self.assertEqual(cm.exception.args[0], "queue_full")
        self.assertIn("3", cm.exception.args[1])
        # 终态不计入:2 非终态 + 1 done → 过
        reg = tc.empty_registry()
        reg["tasks"]["t-000000000001"] = _entry(state="queued", workitem="q1")
        reg["tasks"]["t-000000000002"] = _entry(state="running", workitem="q2")
        reg["tasks"]["t-000000000003"] = _entry(state="done", workitem="q3")
        self._write_reg(reg)
        tc.gate_validate(self._valid_opts(workitem="new"), cap_cfg)

    def test_G9_why_required(self):
        for why in (None, "", "   "):
            with self.assertRaises(tc.GateReject) as cm:
                tc.gate_validate(self._valid_opts(why=why), _cfg())
            self.assertEqual(cm.exception.args[0], "why_required")

    def test_G10_template_whitelist(self):
        self._mk_wi("x")
        # flow workitem 白名单命中 + workitem 存在 → 过(无需 --force)
        tc.gate_validate(self._valid_opts(
            command="flow workitem design x --sync --json",
            workitem="x", force=False), _cfg())
        # 自由命令(有锚点)不在白名单 → 拒,提示 --force
        with self.assertRaises(tc.GateReject) as cm:
            tc.gate_validate(self._valid_opts(
                command="FLOW_WORKDIR=/projects/demo rm -rf /",
                workitem=None, force=False), _cfg())
        self.assertEqual(cm.exception.args[0], "command_not_whitelisted")
        self.assertIn("--force", cm.exception.args[1])
        # --force 跳过白名单且 audit.force_reason 落账
        # gate_validate 不再返回 patch(audit 由 add_task 统一维护);此处断言 add_task 落账结果
        tc.gate_validate(self._valid_opts(
            command="FLOW_WORKDIR=/projects/demo rm -rf /",
            workitem=None, force=True, **{"force-reason": "r"}), _cfg())
        tid = tc.add_task(_cfg(), command="FLOW_WORKDIR=/projects/demo rm -rf /",
                          priority="P2", expected_seconds=30, kind="design",
                          why="t", force=True, force_reason="r")
        reg = tc.load_registry()
        self.assertEqual(reg["tasks"][tid]["audit"]["force_reason"], "r")

    def test_G11_force_not_bypass_others(self):
        """红线:--force 仅撬模板白名单,其余 9 门禁不可绕过。"""
        with self.assertRaises(tc.GateReject) as cm:
            tc.gate_validate(self._valid_opts(kind="reminder", force=True), _cfg())
        self.assertEqual(cm.exception.args[0], "invalid_kind")
        with self.assertRaises(tc.GateReject):
            tc.gate_validate(self._valid_opts(priority="P3", force=True), _cfg())
        with self.assertRaises(tc.GateReject):
            tc.gate_validate(self._valid_opts(why="", force=True), _cfg())
        with self.assertRaises(tc.GateReject):
            tc.gate_validate(self._valid_opts(
                command="sleep 1", workitem=None, force=True), _cfg())


# ---------------------------------------------------------------------------
# scheduled 入队 S1-S4(§1.5(1)/parse_scheduled_at)
# ---------------------------------------------------------------------------

class ScheduledTests(LedgerIsoBase):
    """S1-S4:--at → scheduled 落账不 auto-dispatch;过去/naive 拒绝;HH:MM 简写归一。"""

    def _future(self, hours=6):
        return (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat(
            timespec="seconds")

    def test_S1_add_scheduled(self):
        self._mk_wi("x")  # 锚定:FLOW_WORKITEM_RE 命中后 workitem 需存在
        buf = io.StringIO()
        at = self._future()
        with contextlib.redirect_stdout(buf):
            rc = tc.cmd_add(["--command", "flow workitem design x --sync --json",
                             "--workitem", "x", "--kind", "design",
                             "--priority", "P2", "--expected-seconds", "480",
                             "--why", "test", "--at", at, "--json"],
                            _cfg(max_parallel=2))
        self.assertEqual(rc, 0)
        info = json.loads(buf.getvalue().strip().splitlines()[-1])
        self.assertEqual(info["state"], "scheduled")
        tid = info["id"]
        t = tc.load_registry()["tasks"][tid]
        self.assertEqual(t["state"], "scheduled")
        # UTC 归一落账
        self.assertTrue(t["scheduled_at"].endswith("+00:00"))
        self.assertIsNone(t["started_at"])  # 不 auto-dispatch
        # 与入参同刻(归一)
        self.assertEqual(datetime.fromisoformat(t["scheduled_at"]),
                         datetime.fromisoformat(at).astimezone(timezone.utc))

    def test_S2_add_at_past_reject(self):
        past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(timespec="seconds")
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rc = tc.cmd_add(["--command", "FLOW_WORKDIR=/projects/demo sleep 1",
                             "--kind", "design", "--priority", "P2",
                             "--expected-seconds", "30", "--why", "test",
                             "--force", "--force-reason", "test",
                             "--at", past, "--json"], _cfg())
        self.assertEqual(rc, 2)
        self.assertIn("过去", err.getvalue())
        self.assertEqual(tc.load_registry()["tasks"], {})  # 无部分落账

    def test_S3_parse_at_shorthand(self):
        now = datetime.fromisoformat("2026-08-22T02:00:00+00:00")  # 10:00 北京
        got = tc.parse_scheduled_at("18:00", now=now)
        self.assertEqual(got, "2026-08-22T10:00:00+00:00")  # 今天 18:00 北京
        now2 = datetime.fromisoformat("2026-08-22T11:00:00+00:00")  # 19:00 北京,已过
        got2 = tc.parse_scheduled_at("18:00", now=now2)
        self.assertEqual(got2, "2026-08-23T10:00:00+00:00")  # 次日 18:00 北京
        got3 = tc.parse_scheduled_at("09:30:15", now=now)
        # 09:30 北京 < 10:00(now)→ 次日
        self.assertEqual(got3, "2026-08-23T01:30:15+00:00")

    def test_S4_parse_at_naive_reject(self):
        with self.assertRaises(tc.UsageError) as cm:
            tc.parse_scheduled_at("2026-08-22T12:00:00")
        self.assertIn("+08:00", str(cm.exception))
        with self.assertRaises(tc.UsageError):
            tc.parse_scheduled_at("bogus")


# ---------------------------------------------------------------------------
# pump P1-P5(§1.5(2):plan_pump + cmd_pump + 心跳)
# ---------------------------------------------------------------------------

class PumpTests(LedgerIsoBase):
    """P1-P5:排序/提升/槽位/心跳/并发 flock 幂等。"""

    def test_P1_plan_pump_order(self):
        now = datetime.fromisoformat("2026-08-22T12:00:00+00:00")
        reg = tc.empty_registry()
        reg["tasks"]["t-000000000001"] = _entry(
            tid="t-000000000001", state="scheduled", priority="P2",
            scheduled_at="2026-08-22T11:00:00+00:00")
        reg["tasks"]["t-000000000002"] = _entry(
            tid="t-000000000002", state="scheduled", priority="P1",
            scheduled_at="2026-08-22T10:00:00+00:00")
        reg["tasks"]["t-000000000003"] = _entry(
            tid="t-000000000003", state="scheduled", priority="P0",
            scheduled_at="2026-08-22T09:00:00+00:00")
        reg["tasks"]["t-000000000004"] = _entry(
            tid="t-000000000004", state="scheduled", priority="P2",
            scheduled_at="2026-08-22T10:30:00+00:00")  # 同 P2,更早
        reg["tasks"]["t-000000000005"] = _entry(
            tid="t-000000000005", state="scheduled", priority="P2",
            scheduled_at="2026-08-22T13:00:00+00:00")  # 未到期
        reg["tasks"]["t-000000000006"] = _entry(
            tid="t-000000000006", state="done", priority="P2",
            scheduled_at="2026-08-22T08:00:00+00:00")  # 终态
        reg["tasks"]["t-000000000007"] = _entry(
            tid="t-000000000007", state="queued", priority="P0",
            scheduled_at="2026-08-22T07:00:00+00:00")  # queued 不碰
        promoted = tc.plan_pump(reg, 5, now)
        self.assertEqual([t["id"] for t in promoted],
                         ["t-000000000003", "t-000000000002", "t-000000000004",
                          "t-000000000001"])  # P0<P1<P2 + scheduled_at 升序
        self.assertNotIn("t-000000000005", [t["id"] for t in promoted])  # 未到期
        self.assertNotIn("t-000000000006", [t["id"] for t in promoted])  # 终态
        self.assertNotIn("t-000000000007", [t["id"] for t in promoted])  # queued 源分离
        # 纯函数不改 reg
        self.assertEqual(reg["tasks"]["t-000000000003"]["state"], "scheduled")

    def test_P2_pump_promote(self):
        cfg = _cfg(max_parallel=2)
        past = (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat(timespec="seconds")
        tid = "t-000000000001"
        reg = tc.empty_registry()
        reg["tasks"][tid] = _entry(state="scheduled", scheduled_at=past,
                                   command="sleep 0.01", kind="design")
        self._write_reg(reg)
        orig = tc.spawn_runner

        def inline(rid):
            os.makedirs(tc.logs_dir(), exist_ok=True)
            with _FDRedirect(tc.log_path(rid)):
                return tc._runner(rid, cfg)

        tc.spawn_runner = inline
        try:
            rc = tc.cmd_pump([], cfg)
        finally:
            tc.spawn_runner = orig
        self.assertEqual(rc, 0)
        t = tc.load_registry()["tasks"][tid]
        self.assertEqual(t["state"], "done")
        self.assertEqual(t["exit_code"], 0)
        self.assertIsNone(t["failure_tail"])
        self.assertIsNotNone(t["finished_at"])

    def test_P3_pump_slot_cap(self):
        now = datetime.fromisoformat("2026-08-22T12:00:00+00:00")
        reg = tc.empty_registry()
        reg["tasks"]["t-000000000001"] = _entry(
            tid="t-000000000001", state="running", kind="design")
        reg["tasks"]["t-000000000002"] = _entry(
            tid="t-000000000002", state="scheduled",
            scheduled_at="2026-08-22T10:00:00+00:00")
        reg["tasks"]["t-000000000003"] = _entry(
            tid="t-000000000003", state="scheduled",
            scheduled_at="2026-08-22T09:00:00+00:00")
        # running 占 1 槽:max_parallel=1 → 0 个;=2 → 1 个(P0/P2 同,按 scheduled_at 升序)
        self.assertEqual(tc.plan_pump(reg, 1, now), [])
        self.assertEqual([t["id"] for t in tc.plan_pump(reg, 2, now)],
                         ["t-000000000003"])

    def test_P4_pump_heartbeat(self):
        cfg = _cfg(max_parallel=2)
        self.assertEqual(tc.cmd_pump([], cfg), 0)
        self.assertTrue(os.path.isfile(tc.pump_path()))
        hb1 = json.loads(tc._fc.read_file(tc.pump_path()))["heartbeat_at"]
        self.assertEqual(tc.cmd_pump([], cfg), 0)
        hb2 = json.loads(tc._fc.read_file(tc.pump_path()))["heartbeat_at"]
        self.assertGreaterEqual(hb2, hb1)  # 心跳前进
        rec = json.loads(tc._fc.read_file(tc.pump_path()))
        self.assertEqual(rec["phase"], "end")
        self.assertEqual(rec["schema_version"], 1)

    def test_P5_pump_concurrent_skip(self):
        """flock 被占 → 直接返回(幂等),不双提升。"""
        cfg = _cfg(max_parallel=2)
        past = (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat(timespec="seconds")
        tid = "t-000000000001"
        reg = tc.empty_registry()
        reg["tasks"][tid] = _entry(state="scheduled", scheduled_at=past)
        self._write_reg(reg)
        os.makedirs(tc.task_dir(), exist_ok=True)
        fd = os.open(tc.pump_lock_path(), os.O_CREAT | os.O_RDWR)
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            rc = tc.cmd_pump([], cfg)
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
        self.assertEqual(rc, 0)
        self.assertEqual(tc.load_registry()["tasks"][tid]["state"], "scheduled")  # 未提升


# ---------------------------------------------------------------------------
# reschedule R1-R3(§1.5(3))
# ---------------------------------------------------------------------------

class RescheduleTests(LedgerIsoBase):
    """R1-R3:仅 scheduled 可改期;naive/过去/非法 --at 拒绝;目标不存在拒绝。"""

    def _future(self, hours=6):
        return (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat(
            timespec="seconds")

    def test_R1_reschedule_ok(self):
        tid = "t-000000000001"
        at1, at2 = self._future(3), self._future(8)
        reg = tc.empty_registry()
        reg["tasks"][tid] = _entry(state="scheduled", scheduled_at=at1)
        self._write_reg(reg)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = tc.cmd_reschedule([tid, "--at", at2, "--json"], _cfg())
        self.assertEqual(rc, 0)
        out = json.loads(buf.getvalue().strip().splitlines()[-1])
        self.assertEqual(out["scheduled_at"],
                         datetime.fromisoformat(at2).astimezone(timezone.utc)
                         .isoformat(timespec="seconds"))
        self.assertEqual(tc.load_registry()["tasks"][tid]["scheduled_at"], out["scheduled_at"])

    def test_R2_reschedule_reject_non_scheduled(self):
        tid = "t-000000000001"
        at = self._future()
        for st in ("queued", "running", "done"):
            reg = tc.empty_registry()
            reg["tasks"][tid] = _entry(state=st, scheduled_at=None)
            self._write_reg(reg)
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = tc.cmd_reschedule([tid, "--at", at], _cfg())
            self.assertEqual(rc, 2, st)
            self.assertIn("仅 scheduled 可改期", err.getvalue())

    def test_R3_reschedule_missing_id(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rc = tc.cmd_reschedule(["t-00000000dead", "--at", self._future()], _cfg())
        self.assertEqual(rc, 2)
        self.assertIn("task not found", err.getvalue())
        # 过去 --at → 拒绝(同 E12/E13 解析器)
        past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(timespec="seconds")
        tid = "t-000000000001"
        reg = tc.empty_registry()
        reg["tasks"][tid] = _entry(state="scheduled",
                                   scheduled_at=self._future())
        self._write_reg(reg)
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rc = tc.cmd_reschedule([tid, "--at", past], _cfg())
        self.assertEqual(rc, 2)
        self.assertIn("过去", err.getvalue())


# ---------------------------------------------------------------------------
# cost 摘取 C1-C5(§1.5(4):best-effort,价目表不硬编码)
# ---------------------------------------------------------------------------

class CostTests(LedgerIsoBase):
    """C1-C5:result.json.cost 优先 / runs log 累加 / null 不阻断 / dsh manifest / secret 安全。"""

    def _executor_dir(self, wi="w1"):
        d = os.path.join(self.data, "workitems", wi, "executor")
        os.makedirs(d, exist_ok=True)
        return d

    def test_C1_cost_reasonix_result_json(self):
        d = self._executor_dir()
        with open(os.path.join(d, "result.json"), "w", encoding="utf-8") as f:
            json.dump({"status": "ok", "cost": 0.0042}, f)
        got = tc.extract_reasonix_cost(d, "2026-08-22T02:00:00+00:00", None)
        self.assertEqual(got, 0.0042)

    def test_C2_cost_reasonix_log_sum(self):
        runs = os.path.join(self.home, ".reasonix", "runs")
        os.makedirs(runs, exist_ok=True)
        # started_at=10:00:01 北京;log 名 100001 就近匹配
        with open(os.path.join(runs, "20260822-100001.log"), "w", encoding="utf-8") as f:
            f.write("step done $0.0002\nstep2 $0.0003\nfinal $0.0001\n")
        # 歧义候选:100100 距 59s,更远 → 不取
        with open(os.path.join(runs, "20260822-100100.log"), "w", encoding="utf-8") as f:
            f.write("other $9.0000\n")
        got = tc.extract_reasonix_cost(
            self._executor_dir(), "2026-08-22T02:00:01+00:00", None)
        self.assertAlmostEqual(got, 0.0006)  # 0.0002+0.0003+0.0001,且取就近唯一
        # 无匹配时间窗 → None
        self.assertIsNone(tc.extract_reasonix_cost(
            self._executor_dir(), "2026-08-22T12:00:00+00:00", None))

    def test_C3_cost_null_no_block(self):
        # 无 result.json/log → None,且终态流转不受影响
        self.assertIsNone(tc.extract_reasonix_cost(self._executor_dir(),
                                                   "2026-08-22T02:00:00+00:00", None))
        tid = "t-000000000001"
        reg = tc.empty_registry()
        reg["tasks"][tid] = _entry(state="running", kind="execute", workitem="w1",
                                   command="sleep 0.01")
        self._write_reg(reg)
        self.assertEqual(self._run_runner(tid, _cfg()), 0)
        t = tc.load_registry()["tasks"][tid]
        self.assertEqual(t["state"], "done")
        self.assertIsNone(t["cost_usd"])  # 摘不到 → null,不阻断

    def test_C4_cost_dsh_null(self):
        manifest = os.path.join(self.data, "designs", ".dsh-design", "manifest.jsonl")
        os.makedirs(os.path.dirname(manifest), exist_ok=True)
        wi_dir = os.path.join(self.data, "workitems", "w1")
        os.makedirs(wi_dir, exist_ok=True)
        with open(manifest, "w", encoding="utf-8") as f:
            f.write(json.dumps({"session": "s1", "usage": {"tokens": 100}}))
        self.assertIsNone(tc.extract_dsh_cost(wi_dir))  # 无 cost_usd → null,不乘价目表
        with open(manifest, "a", encoding="utf-8") as f:
            f.write("\n" + json.dumps({"session": "s2", "cost_usd": 0.0123}))
        self.assertEqual(tc.extract_dsh_cost(wi_dir), 0.0123)
        self.assertIsNone(tc.extract_dsh_cost(None))

    def test_C5_cost_secret_safe(self):
        # cost 提取只读数值,不触碰 key
        d = self._executor_dir()
        with open(os.path.join(d, "result.json"), "w", encoding="utf-8") as f:
            json.dump({"status": "ok", "cost": 0.1, "api_key": _FAKE_SECRET}, f)
        self.assertEqual(tc.extract_reasonix_cost(d, "2026-08-22T02:00:00+00:00", None), 0.1)
        # 注册表写前 DENY_RE 兜底:含疑似 secret → 拒写
        reg = tc.empty_registry()
        reg["tasks"]["t-000000000001"] = _entry(command=f"echo {_FAKE_SECRET}")
        with self.assertRaises(tc.StoreError):
            tc.save_registry_atomic(reg)


# ---------------------------------------------------------------------------
# run <id> B1(§1.7:单任务强制触发,绕过窗口)
# ---------------------------------------------------------------------------

class RunIdTests(LedgerIsoBase):
    """B1:run <id> 强制 scheduled/queued→running;终态拒绝。"""

    def test_B1_run_id_trigger(self):
        cfg = _cfg(max_parallel=2)
        for st in ("scheduled", "queued"):
            tid = f"t-00000000000{'1' if st == 'scheduled' else '2'}"
            reg = tc.empty_registry()
            reg["tasks"][tid] = _entry(state=st, scheduled_at="2026-08-22T10:00:00+00:00"
                                       if st == "scheduled" else None)
            self._write_reg(reg)
            orig = tc.spawn_runner
            tc.spawn_runner = lambda _t: None
            try:
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    rc = tc.cmd_run([tid, "--json"], cfg)
            finally:
                tc.spawn_runner = orig
            self.assertEqual(rc, 0, st)
            self.assertEqual(tc.load_registry()["tasks"][tid]["state"], "running", st)
            self.assertIsNotNone(tc.load_registry()["tasks"][tid]["started_at"])
        # 终态 id → 拒绝
        tid = "t-000000000003"
        reg = tc.empty_registry()
        reg["tasks"][tid] = _entry(state="done")
        self._write_reg(reg)
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rc = tc.cmd_run([tid], cfg)
        self.assertEqual(rc, 2)
        self.assertIn("仅 queued/scheduled 可强制触发", err.getvalue())
        # 不存在 id → 拒绝
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rc = tc.cmd_run(["t-00000000dead"], cfg)
        self.assertEqual(rc, 2)
        self.assertIn("task not found", err.getvalue())


# ---------------------------------------------------------------------------
# suggest_wake W1(§1.5(1):cmd_add 复用 window.window_suggest)
# ---------------------------------------------------------------------------

class SuggestWakeTests(LedgerIsoBase):
    """W1:add 成功输出 suggest_wake ∈ 决策表值,与 window.window_suggest 一致。"""

    def test_W1_suggest_wake(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = tc.cmd_add(["--command", "FLOW_WORKDIR=/projects/demo sleep 1",
                             "--kind", "design", "--priority", "P2",
                             "--expected-seconds", "2400", "--why", "test",
                             "--force", "--force-reason", "test", "--json"],
                            _cfg(max_parallel=0))
        self.assertEqual(rc, 0)
        info = json.loads(buf.getvalue().strip().splitlines()[-1])
        now = datetime.now(timezone.utc)
        expect = window.window_suggest({"priority": "P2", "expected_seconds": 2400}, now)
        self.assertEqual(info["suggest_wake"], expect)
        self.assertIn(info["suggest_wake"],
                      ("立即", "现在可跑", "顺延18:00", "12:00 午间", "18:00 晚间"))


# ---------------------------------------------------------------------------
# 跨仓 env 注入 ENV1/ENV2(任务书 §4:实测必修,跨仓 workitem 解析)
# ---------------------------------------------------------------------------

class EnvInjectionTests(LedgerIsoBase):
    """ENV1/ENV2:_runner_env 显式注入 workdir 的 FLOW_DATA_DIR/FLOW_WORKDIR
    (workitem 解析落到任务归属仓而非 collab-flow 默认);workdir 缺失回退 env/默认。"""

    def test_ENV1_runner_env_inject(self):
        base = {"PATH": "/usr/bin", "FLOW_DATA_DIR": "/old/.flow", "FLOW_WORKDIR": "/old"}
        env = tc._runner_env(_entry(workdir="/repo/proj-a"), base_env=base)
        self.assertEqual(env["FLOW_DATA_DIR"], "/repo/proj-a/.flow")  # 覆盖为 workdir 仓
        self.assertEqual(env["FLOW_WORKDIR"], "/repo/proj-a")
        self.assertEqual(env["PATH"], "/usr/bin")  # 其余键保留

    def test_ENV2_runner_env_fallback(self):
        # workdir 缺失 → 保留 base_env 现值(不覆盖已设键)
        env = tc._runner_env(_entry(workdir=None),
                             base_env={"FLOW_DATA_DIR": "/keep/.flow",
                                       "FLOW_WORKDIR": "/keep"})
        self.assertEqual(env["FLOW_DATA_DIR"], "/keep/.flow")
        self.assertEqual(env["FLOW_WORKDIR"], "/keep")
        # FLOW_DATA_DIR 未设 → 默认 cwd/.flow(与 flow-core.data_dir 口径一致)
        env2 = tc._runner_env(_entry(workdir=None), base_env={})
        self.assertEqual(env2["FLOW_DATA_DIR"], os.path.join(os.getcwd(), ".flow"))
        self.assertNotIn("FLOW_WORKDIR", env2)
        # workdir == 当前 cwd(默认归属,add 未显式 --workdir)→ 不注入,保持 env 现状
        env3 = tc._runner_env(_entry(workdir=os.getcwd()),
                              base_env={"FLOW_DATA_DIR": "/keep/.flow"})
        self.assertEqual(env3["FLOW_DATA_DIR"], "/keep/.flow")
        self.assertNotIn("FLOW_WORKDIR", env3)


if __name__ == "__main__":
    unittest.main()
