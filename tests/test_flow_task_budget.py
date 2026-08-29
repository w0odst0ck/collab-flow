#!/usr/bin/env python3
"""cost-budget-guard 单测(design .flow/workitems/cost-budget-guard/design.md §3.1 + E 表)。

覆盖:聚合口径(月/ISO 周边界、cost 非 null、rate)、超限判定(月/周/双超限/禁用)、
门禁(add 拒绝/P0 放行/短任务放行/锁内二次校验)、pump 暂停与恢复、
告警状态机(进入/冷却/换级/重置/状态损坏/写失败/notify 未配置或失败)、
fail-closed(配置缺失损坏/rate/pause/cooldown 非法/tasks.json 损坏)、精度、目录隔离。

全程 FLOW_TASK_DIR 指向 tempfile.mkdtemp();I/O 用临时目录 + flock;
CLI 侧 mock spawn_runner/run_stale_gate 避免真实子进程。
"""

import contextlib
import importlib.util
import io
import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

_HERE = os.path.dirname(os.path.abspath(__file__))
_TASK_CORE = os.path.join(_HERE, "..", "scripts", "flow-task-core.py")

_tc_spec = importlib.util.spec_from_file_location("flow_task_core_budget", _TASK_CORE)
tc = importlib.util.module_from_spec(_tc_spec)
_tc_spec.loader.exec_module(tc)

TZ_CN = tc.TZ_CN

_ENV_KEYS = ("FLOW_TASK_DIR", "FLOW_BUDGET_MONTH_CNY", "FLOW_BUDGET_WEEK_CNY",
             "FLOW_BUDGET_EXCHANGE_RATE", "COLLABFLOW_DEFAULTS", "COLLABFLOW_CONFIG")


def _cfg(budget=None, **task_kw):
    """最小 task/host/chain + budget 段(缺省:月 600/周 0,不启用周)。"""
    base = {
        "task": {"max_parallel": 2, "queue_cap": 50,
                 "expected_seconds_seed": {"design": 480, "execute": 1800},
                 "same_workdir_serial": 1, "pro_serial": 1},
        "host": {"notify": None, "wake_template": None},
        "chain": {"enabled": True},
        "roles": {}, "executor": {},
        "budget": {"month_cny": 600, "week_cny": 0, "exchange_rate": 7.2,
                   "alert_cooldown_h": 24, "pause_long_task_s": 600},
    }
    if budget is not None:
        base["budget"].update(budget)
    base["task"].update(task_kw)
    return base


def _task(tid="t-000000000001", state="done", cost_usd=None, finished_at=None,
          expected_seconds=None, priority="P2", kind="execute", workitem=None,
          scheduled_at=None, workdir="/projects/foo"):
    return {
        "id": tid, "workitem": workitem, "command": "sleep 0.01", "priority": priority,
        "state": state, "kind": kind, "expected_seconds": expected_seconds,
        "kill_on_timeout": False, "created_at": "2026-08-01T00:00:00+00:00",
        "started_at": None, "finished_at": finished_at, "exit_code": None,
        "failure_tail": None, "pid": None, "heartbeat_at": None, "cost_usd": cost_usd,
        "scheduled_at": scheduled_at, "workdir": workdir, "why": "test",
        "model": None, "audit": None,
    }


def _seed(tasks):
    reg = tc.empty_registry()
    reg["tasks"] = {t["id"]: t for t in tasks}
    tc.save_registry_atomic(reg)


def _cn_now_str():
    """上海当前时刻 ISO(带 +08:00):保证构造的任务稳定归当月/当周。"""
    return datetime.now(timezone.utc).astimezone(TZ_CN).isoformat(timespec="seconds")


def _spend_task(tid, cost_usd):
    """已落账的终态任务(当前月/周,用于打满预算)。"""
    return _task(tid=tid, state="done", cost_usd=cost_usd, finished_at=_cn_now_str())


class _IsoBase(unittest.TestCase):
    """临时 FLOW_TASK_DIR 隔离基类。"""

    def setUp(self):
        self._saved = {k: os.environ.get(k) for k in _ENV_KEYS}
        self.tmp = tempfile.mkdtemp(prefix="ftb-")
        os.environ["FLOW_TASK_DIR"] = self.tmp
        for k in ("FLOW_BUDGET_MONTH_CNY", "FLOW_BUDGET_WEEK_CNY",
                  "FLOW_BUDGET_EXCHANGE_RATE", "COLLABFLOW_DEFAULTS", "COLLABFLOW_CONFIG"):
            os.environ.pop(k, None)

    def tearDown(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        shutil.rmtree(self.tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# 聚合口径(§1.1):月/周边界、cost 非 null、running 跳过、rate 精度
# ---------------------------------------------------------------------------

class AggregateTests(_IsoBase):
    def test_aggregate_month_boundary(self):
        """finished_at 月末 23:59:59+08:00 归 8 月,9 月 1 日 00:00:00+08:00 归 9 月(E15)。"""
        now = datetime(2026, 9, 2, 0, 0, 0, tzinfo=TZ_CN)
        _seed([
            _task(tid="t-000000000001", cost_usd=1.0, finished_at="2026-08-31T23:59:59+08:00"),
            _task(tid="t-000000000002", cost_usd=2.0, finished_at="2026-09-01T00:00:00+08:00"),
        ])
        month_used, week_used = tc.budget_usage(now, _cfg())
        self.assertEqual(month_used, 2.0 * 7.2)   # 仅 9 月任务
        self.assertEqual(week_used, 3.0 * 7.2)    # 8/31(一)+9/1(二)同 ISO 周

    def test_aggregate_week_boundary(self):
        """周日 23:59:59 归当周、周一 00:00:00 归下周(ISO 周一起始,E15)。"""
        now = datetime(2026, 8, 30, 23, 30, 0, tzinfo=TZ_CN)  # 周日晚上
        _seed([
            _task(tid="t-000000000001", cost_usd=1.0, finished_at="2026-08-30T23:59:59+08:00"),
            _task(tid="t-000000000002", cost_usd=2.0, finished_at="2026-08-31T00:00:00+08:00"),
        ])
        _month, week_used = tc.budget_usage(now, _cfg())
        self.assertEqual(week_used, 1.0 * 7.2)    # 只含周日任务

    def test_aggregate_cost_null_skip(self):
        """cost_usd=None 跳过;=0 计入但贡献 0;非数值跳过(E5)。"""
        _seed([
            _task(tid="t-000000000001", cost_usd=None, finished_at=_cn_now_str()),
            _task(tid="t-000000000002", cost_usd=0, finished_at=_cn_now_str()),
            _task(tid="t-000000000003", cost_usd="oops", finished_at=_cn_now_str()),
            _task(tid="t-000000000004", cost_usd=5.0, finished_at=_cn_now_str()),
        ])
        month_used, _week_used = tc.budget_usage(datetime.now(timezone.utc), _cfg())
        self.assertEqual(month_used, 5.0 * 7.2)

    def test_aggregate_running_skip(self):
        """无 finished_at(running/queued)不计;非法/naive 时间戳跳过(E6)。"""
        now = _cn_now_str()
        _seed([
            _task(tid="t-000000000001", state="running", cost_usd=9.0, finished_at=None),
            _task(tid="t-000000000002", state="queued", cost_usd=9.0, finished_at=None),
            _task(tid="t-000000000003", state="done", cost_usd=9.0, finished_at="not-a-date"),
            _task(tid="t-000000000004", state="done", cost_usd=9.0, finished_at="2026-08-01T10:00:00"),
            _task(tid="t-000000000005", state="done", cost_usd=3.0, finished_at=now),
        ])
        month_used, _week_used = tc.budget_usage(datetime.now(timezone.utc), _cfg())
        self.assertEqual(month_used, 3.0 * 7.2)

    def test_aggregate_rate_applied(self):
        """Σcost_usd×exchange_rate 正确且 round 2 位(E14)。"""
        now = _cn_now_str()
        _seed([
            _task(tid="t-000000000001", cost_usd=0.5, finished_at=now),
            _task(tid="t-000000000002", cost_usd=1.0, finished_at=now),
            _task(tid="t-000000000003", cost_usd=2.5, finished_at=now),
        ])
        month_used, _week_used = tc.budget_usage(datetime.now(timezone.utc), _cfg())
        self.assertEqual(month_used, round(4.0 * 7.2, 2))

    def test_budget_precision(self):
        """round 后比较:599.99976 → 600.0 判超限;599.994 → 599.99 不超(E14)。"""
        now = _cn_now_str()
        _seed([_task(tid="t-000000000001", cost_usd=83.3333, finished_at=now)])
        exceeded, level, used, limit = tc.budget_exceeded(_cfg())
        self.assertTrue(exceeded)                      # round(599.99976,2)=600.0 ≥ 600.0
        self.assertEqual((level, used, limit), ("month", 600.0, 600.0))
        _seed([_task(tid="t-000000000002", cost_usd=83.3325, finished_at=now)])
        month_used, _w = tc.budget_usage(datetime.now(timezone.utc), _cfg())
        self.assertEqual(month_used, 599.99)          # round(599.994,2)=599.99 < 600
        # 未超限 → 按 §1.1 契约返回 (False, None, 0.0, 0.0)
        self.assertEqual(tc.budget_exceeded(_cfg()), (False, None, 0.0, 0.0))


# ---------------------------------------------------------------------------
# 超限判定(§1.1):月/周/双超限/禁用
# ---------------------------------------------------------------------------

class ExceededTests(_IsoBase):
    def test_exceeded_month(self):
        _seed([_spend_task("t-000000000001", 15.0)])  # 15×7.2=108 ≥ 100
        exceeded, level, used, limit = tc.budget_exceeded(_cfg(budget={"month_cny": 100}))
        self.assertEqual((exceeded, level), (True, "month"))
        self.assertEqual(used, 108.0)
        self.assertEqual(limit, 100.0)

    def test_exceeded_week(self):
        _seed([_spend_task("t-000000000001", 10.0)])  # 72 ≥ 50
        exceeded, level, _u, _l = tc.budget_exceeded(
            _cfg(budget={"month_cny": 0, "week_cny": 50}))
        self.assertEqual((exceeded, level), (True, "week"))

    def test_exceeded_both(self):
        """月与周同时超限 → level 取 month(主阈值优先,E16)。"""
        _seed([_spend_task("t-000000000001", 15.0)])  # 108 ≥ 100 且 ≥ 50
        exceeded, level, _u, _l = tc.budget_exceeded(
            _cfg(budget={"month_cny": 100, "week_cny": 50}))
        self.assertEqual((exceeded, level), (True, "month"))

    def test_exceeded_disabled(self):
        """month_cny=0/week_cny=0 → 不启用,高额也不超限。"""
        _seed([_spend_task("t-000000000001", 9999.0)])
        self.assertEqual(tc.budget_exceeded(_cfg(budget={"month_cny": 0, "week_cny": 0})),
                         (False, None, 0.0, 0.0))

    def test_budget_limit_invalid(self):
        """month/week 非数值/负数/NaN → 该周期 0(不启用,E2)。"""
        for bad in ("abc", -5, "nan"):
            L = tc.budget_limits(_cfg(budget={"month_cny": bad, "week_cny": bad}))
            self.assertEqual((L["month_cny"], L["week_cny"]), (0.0, 0.0), bad)
        # 全部非法 → 恒不超限
        exceeded, level, _u, _l = tc.budget_exceeded(
            _cfg(budget={"month_cny": "abc", "week_cny": -1}))
        self.assertEqual((exceeded, level), (False, None))

    def test_budget_rate_invalid(self):
        """exchange_rate 缺失/非数值/≤0/NaN → 回退 7.2(E3)。"""
        for bad in (None, "abc", 0, -1, "nan"):
            kw = {} if bad is None else {"exchange_rate": bad}
            L = tc.budget_limits(_cfg(budget=kw))
            self.assertEqual(L["exchange_rate"], 7.2, bad)
        _seed([_spend_task("t-000000000001", 1.0)])
        month_used, _w = tc.budget_usage(
            datetime.now(timezone.utc), _cfg(budget={"exchange_rate": "nan"}))
        self.assertEqual(month_used, 7.2)


# ---------------------------------------------------------------------------
# tasks.json fail-closed(E4)+ 配置缺失/损坏(E1)
# ---------------------------------------------------------------------------

class FailClosedTests(_IsoBase):
    def test_budget_tasks_corrupt(self):
        """tasks.json 缺失/损坏/顶层非对象 → 用量 0,不误杀(E4)。"""
        self.assertEqual(tc.budget_usage(datetime.now(timezone.utc), _cfg()), (0.0, 0.0))
        self.assertEqual(tc.budget_exceeded(_cfg())[0], False)
        with open(tc.registry_path(), "w", encoding="utf-8") as f:
            f.write("{oops")
        self.assertEqual(tc.budget_usage(datetime.now(timezone.utc), _cfg()), (0.0, 0.0))
        self.assertEqual(tc.budget_exceeded(_cfg())[0], False)
        with open(tc.registry_path(), "w", encoding="utf-8") as f:
            f.write("[1,2,3]")
        self.assertEqual(tc.budget_usage(datetime.now(timezone.utc), _cfg()), (0.0, 0.0))

    def test_config_missing_corrupt(self):
        """budget 段缺失/非 dict(标量/列表)→ 不启用,add 长任务不拒(E1)。"""
        for b in (None, [1, 2], "oops", 42):
            cfg = _cfg()
            if b is None:
                del cfg["budget"]
            else:
                cfg["budget"] = b
            self.assertEqual(tc.budget_exceeded(cfg)[0], False, b)
            self.assertEqual(tc.budget_limits(cfg)["month_cny"], 0.0, b)

    def test_budget_pause_invalid(self):
        """pause_long_task_s 缺失/非数值/负数 → 回退 600(E11)。"""
        for bad in (None, "abc", -1, "nan"):
            kw = {} if bad is None else {"pause_long_task_s": bad}
            L = tc.budget_limits(_cfg(budget=kw))
            self.assertEqual(L["pause_long_task_s"], 600.0, bad)
        bres = (True, "month", 100.0, 50.0)
        self.assertTrue(tc.budget_blocks(600, "P2", bres, _cfg(budget={"pause_long_task_s": "abc"})))
        self.assertFalse(tc.budget_blocks(599, "P2", bres, _cfg()))

    def test_budget_cooldown_invalid(self):
        """alert_cooldown_h 缺失/非数值/负数 → 回退 24(E12)。"""
        for bad in (None, "abc", -1, "nan"):
            kw = {} if bad is None else {"alert_cooldown_h": bad}
            L = tc.budget_limits(_cfg(budget=kw))
            self.assertEqual(L["alert_cooldown_h"], 24.0, bad)

    def test_budget_expected_missing(self):
        """expected_seconds 缺失/非数值(老条目)→ 0,按短任务放行(E13)。"""
        bres = (True, "month", 100.0, 50.0)
        self.assertFalse(tc.budget_blocks(None, "P2", bres, _cfg()))
        self.assertFalse(tc.budget_blocks("oops", "P2", bres, _cfg()))
        self.assertFalse(tc.budget_blocks("", "P2", bres, _cfg()))


# ---------------------------------------------------------------------------
# 门禁(§1.2):add 拒绝/P0 放行/短任务放行/锁内二次校验
# ---------------------------------------------------------------------------

_ADD_ARGS = ["--command", "FLOW_WORKDIR=/projects/x sleep 0.01", "--workitem", "w1",
             "--kind", "design", "--why", "test", "--force", "--force-reason", "test"]


class GateTests(_IsoBase):
    def _over_budget_cfg(self, **kw):
        _seed([_spend_task("t-000000000001", 15.0)])  # 108 ≥ 100 超限
        return _cfg(budget={"month_cny": 100}, max_parallel=0, **kw)

    def test_add_reject_long_non_p0(self):
        """超限 + expected=600 + P2 → GateReject("budget_exceeded"),exit 2。"""
        cfg = self._over_budget_cfg()
        with self.assertRaises(tc.GateReject) as cm:
            tc.gate_validate(
                {"command": "FLOW_WORKDIR=/projects/x sleep 0.01", "workitem": "w1",
                 "kind": "design", "priority": "P2", "expected-seconds": 600,
                 "why": "test", "force": True}, cfg)
        self.assertEqual(cm.exception.args[0], "budget_exceeded")
        self.assertIn("预算超限", cm.exception.args[1])
        # CLI 层:exit 2 + 文案
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            rc = tc.cmd_add(_ADD_ARGS + ["--priority", "P2", "--expected-seconds", "600"], cfg)
        self.assertEqual(rc, 2)
        self.assertIn("预算超限", buf.getvalue())

    def test_add_pass_p0(self):
        """超限 + P0 → 放行(P0 永远放行)。"""
        cfg = self._over_budget_cfg()
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = tc.cmd_add(_ADD_ARGS + ["--priority", "P0", "--expected-seconds", "600", "--json"], cfg)
        self.assertEqual(rc, 0)
        out = json.loads(buf.getvalue().strip().splitlines()[-1])
        self.assertEqual(out["status"], "ok")
        self.assertIn(out["id"], tc.load_registry()["tasks"])

    def test_add_pass_short(self):
        """超限 + expected=599(<600)→ 放行(短任务不误杀)。"""
        cfg = self._over_budget_cfg()
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = tc.cmd_add(_ADD_ARGS + ["--priority", "P2", "--expected-seconds", "599"], cfg)
        self.assertEqual(rc, 0)

    def test_add_pass_budget_disabled(self):
        """未启用(week=0,月不超)→ 长任务照常入队。"""
        _seed([_spend_task("t-000000000001", 5.0)])  # 36 < 600 不超限
        cfg = _cfg(max_parallel=0)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = tc.cmd_add(_ADD_ARGS + ["--priority", "P2", "--expected-seconds", "600"], cfg)
        self.assertEqual(rc, 0)

    def test_add_task_budget_paused(self):
        """程序化直调 add_task(绕过 CLI gate)→ 锁内 BudgetPaused 兜底(§1.2 双保险)。"""
        cfg = self._over_budget_cfg()
        with self.assertRaises(tc.BudgetPaused) as cm:
            tc.add_task(cfg, "sleep 0.01", workitem="w2", priority="P2",
                        expected_seconds=600, kind="design", why="test")
        self.assertEqual(cm.exception.args[0], "month")
        # 短任务程序化直调 → 放行
        tid = tc.add_task(cfg, "sleep 0.01", workitem="w3", priority="P2",
                          expected_seconds=10, kind="design", why="test")
        self.assertIn(tid, tc.load_registry()["tasks"])


# ---------------------------------------------------------------------------
# pump 门禁(§1.2):超限长任务留在 scheduled,恢复后自然续跑
# ---------------------------------------------------------------------------

class PumpTests(_IsoBase):
    def _seed_scheduled(self):
        _seed([
            _spend_task("t-000000000001", 15.0),  # 108 ≥ 100 超限
            _task(tid="t-000000000002", state="scheduled", cost_usd=None,
                  expected_seconds=600, priority="P2",
                  scheduled_at="2020-01-01T00:00:00+00:00"),
            # 短任务用不同 workdir,避免同仓串行门控(_select_gated)先行挡住
            _task(tid="t-000000000003", state="scheduled", cost_usd=None,
                  expected_seconds=10, priority="P2",
                  scheduled_at="2020-01-01T00:00:00+00:00", workdir="/projects/y"),
        ])
        return _cfg(budget={"month_cny": 100})

    def test_pump_pause_scheduled(self):
        """超限长任务到期 → pump 后仍 scheduled、无 started_at;短任务照常 running。"""
        cfg = self._seed_scheduled()
        with mock.patch.object(tc, "run_stale_gate", return_value=[]), \
                mock.patch.object(tc, "spawn_runner", return_value=None):
            rc = tc.cmd_pump([], cfg)
        self.assertEqual(rc, 0)
        reg = tc.load_registry()["tasks"]
        self.assertEqual(reg["t-000000000002"]["state"], "scheduled")   # 暂停
        self.assertIsNone(reg["t-000000000002"]["started_at"])          # 不删、不动
        self.assertEqual(reg["t-000000000003"]["state"], "running")     # 短任务照常

    def test_pump_resume_after_restore(self):
        """阈值恢复(不超限)→ 下一轮 pump 提升被暂停任务。"""
        cfg = self._seed_scheduled()
        with mock.patch.object(tc, "run_stale_gate", return_value=[]), \
                mock.patch.object(tc, "spawn_runner", return_value=None):
            self.assertEqual(tc.cmd_pump([], cfg), 0)
        self.assertEqual(tc.load_registry()["tasks"]["t-000000000002"]["state"], "scheduled")
        ok_cfg = _cfg(budget={"month_cny": 1000})  # 预算恢复
        with mock.patch.object(tc, "run_stale_gate", return_value=[]), \
                mock.patch.object(tc, "spawn_runner", return_value=None):
            rc = tc.cmd_pump([], ok_cfg)
        self.assertEqual(rc, 0)
        self.assertEqual(tc.load_registry()["tasks"]["t-000000000002"]["state"], "running")
        self.assertIsNotNone(tc.load_registry()["tasks"]["t-000000000002"]["started_at"])

    def test_pump_slot_refill(self):
        """暂停长任务不占槽(ocr M):单槽下长任务在前被暂停 → 短任务本轮补选提升。"""
        _seed([
            _spend_task("t-000000000001", 15.0),  # 108 ≥ 100 超限
            _task(tid="t-000000000002", state="scheduled", cost_usd=None,
                  expected_seconds=600, priority="P2",
                  scheduled_at="2020-01-01T00:00:00+00:00"),
            _task(tid="t-000000000003", state="scheduled", cost_usd=None,
                  expected_seconds=10, priority="P2",
                  scheduled_at="2020-01-01T00:00:00+00:00", workdir="/projects/y"),
        ])
        cfg = _cfg(budget={"month_cny": 100})
        cfg["task"]["max_parallel"] = 1  # 单槽:长任务排前会先占位
        with mock.patch.object(tc, "run_stale_gate", return_value=[]), \
                mock.patch.object(tc, "spawn_runner", return_value=None):
            rc = tc.cmd_pump([], cfg)
        self.assertEqual(rc, 0)
        reg = tc.load_registry()["tasks"]
        self.assertEqual(reg["t-000000000002"]["state"], "scheduled")   # 长任务仍暂停
        self.assertEqual(reg["t-000000000003"]["state"], "running")     # 短任务补选提升
        self.assertIsNotNone(reg["t-000000000003"]["started_at"])

    def test_pump_slot_refill_no_overpromote(self):
        """补选不超槽(ocr H):两长任务暂停 + 两短任务争单槽 → 只提升 1 个短任务。"""
        _seed([
            _spend_task("t-000000000001", 15.0),  # 108 ≥ 100 超限
            _task(tid="t-000000000002", state="scheduled", cost_usd=None,
                  expected_seconds=600, priority="P2",
                  scheduled_at="2020-01-01T00:00:00+00:00"),
            _task(tid="t-000000000003", state="scheduled", cost_usd=None,
                  expected_seconds=600, priority="P2",
                  scheduled_at="2020-01-01T00:00:00+00:00", workdir="/projects/y"),
            _task(tid="t-000000000004", state="scheduled", cost_usd=None,
                  expected_seconds=10, priority="P2",
                  scheduled_at="2020-01-01T00:00:00+00:00", workdir="/projects/z"),
            _task(tid="t-000000000005", state="scheduled", cost_usd=None,
                  expected_seconds=10, priority="P2",
                  scheduled_at="2020-01-01T00:00:00+00:00", workdir="/projects/w"),
        ])
        cfg = _cfg(budget={"month_cny": 100})
        cfg["task"]["max_parallel"] = 1  # 单槽
        with mock.patch.object(tc, "run_stale_gate", return_value=[]), \
                mock.patch.object(tc, "spawn_runner", return_value=None):
            rc = tc.cmd_pump([], cfg)
        self.assertEqual(rc, 0)
        reg = tc.load_registry()["tasks"]
        self.assertEqual(reg["t-000000000002"]["state"], "scheduled")   # 两长任务均暂停
        self.assertEqual(reg["t-000000000003"]["state"], "scheduled")
        running = [t for t in reg.values() if t["state"] == "running"]
        self.assertEqual(len(running), 1)                                  # 单槽只提 1 个
        self.assertIn(running[0]["id"], ("t-000000000004", "t-000000000005"))

    def test_pause_long_due_pure(self):
        """纯函数:promotable/paused 分流;P0 恒 promotable。"""
        bres = (True, "month", 100.0, 50.0)
        due = [
            _task(tid="t-000000000001", expected_seconds=600, priority="P2"),
            _task(tid="t-000000000002", expected_seconds=10, priority="P2"),
            _task(tid="t-000000000003", expected_seconds=600, priority="P0"),
            _task(tid="t-000000000004", expected_seconds=None, priority="P2"),  # E13
        ]
        promotable, paused = tc.pause_long_due(due, bres, _cfg())
        self.assertEqual([t["id"] for t in paused], ["t-000000000001"])
        self.assertEqual([t["id"] for t in promotable],
                         ["t-000000000002", "t-000000000003", "t-000000000004"])


# ---------------------------------------------------------------------------
# 告警状态机(§1.3):进入/冷却/换级/重置/损坏/写失败/notify 链路
# ---------------------------------------------------------------------------

class AlertTests(_IsoBase):
    def _over_cfg(self, **kw):
        _seed([_spend_task("t-000000000001", 15.0)])  # 108 ≥ 100 超限
        return _cfg(budget={"month_cny": 100}, **kw)

    def test_alert_enter(self):
        """首次进入超限 → _pipe_notify 1 次 + 状态文件写入(level/ts)。"""
        cfg = self._over_cfg()
        with mock.patch.object(tc, "_pipe_notify") as m:
            tc.run_budget_alert(cfg)
        m.assert_called_once()
        payload = m.call_args.args[1]
        self.assertEqual(payload["kind"], "budget_alert")
        self.assertEqual(payload["level"], "month")
        self.assertEqual(payload["used_cny"], 108.0)
        self.assertEqual(payload["limit_cny"], 100.0)
        self.assertEqual(payload["usage_ratio"], 1.08)
        self.assertEqual(payload["pause_long_task_s"], 600)
        st = tc._read_budget_state(tc._budget_state_path())
        self.assertEqual(st["last_level"], "month")
        self.assertIsNotNone(st["last_alert_ts"])

    def test_alert_cooldown(self):
        """冷却窗口内不重复;超 alert_cooldown_h 再推;换 level 立即推。"""
        cfg = self._over_cfg()
        with mock.patch.object(tc, "_pipe_notify") as m:
            tc.run_budget_alert(cfg)          # 1) 进入 → 告警
            tc.run_budget_alert(cfg)          # 2) 同级冷却内 → 静默
        self.assertEqual(m.call_count, 1)
        # 3) 手动把 last_alert_ts 拨到 25 小时前 → 冷却过期 → 周期刷新告警
        past = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat(timespec="seconds")
        tc._write_budget_state(tc._budget_state_path(),
                               {"last_alert_ts": past, "last_level": "month"})
        with mock.patch.object(tc, "_pipe_notify") as m:
            tc.run_budget_alert(cfg)
        self.assertEqual(m.call_count, 1)
        # 4) 换 level(周超限)→ 立即告警(不冷却)
        _seed([_spend_task("t-000000000001", 15.0)])
        week_cfg = _cfg(budget={"month_cny": 0, "week_cny": 50})
        with mock.patch.object(tc, "_pipe_notify") as m:
            tc.run_budget_alert(week_cfg)
        self.assertEqual(m.call_count, 1)
        self.assertEqual(m.call_args.args[1]["level"], "week")

    def test_alert_reset(self):
        """回到正常 → 状态重置 {None,None},不推送。"""
        cfg = self._over_cfg()
        with mock.patch.object(tc, "_pipe_notify"):
            tc.run_budget_alert(cfg)
        ok_cfg = _cfg(budget={"month_cny": 1000})
        with mock.patch.object(tc, "_pipe_notify") as m:
            tc.run_budget_alert(ok_cfg)
        m.assert_not_called()
        st = tc._read_budget_state(tc._budget_state_path())
        self.assertEqual(st, {"last_alert_ts": None, "last_level": None})

    def test_alert_state_corrupt(self):
        """budget-alert.json 损坏/非 JSON → 视为无历史,本次进入超限重发(E7)。"""
        cfg = self._over_cfg()
        with open(tc._budget_state_path(), "w", encoding="utf-8") as f:
            f.write("{oops")
        self.assertIsNone(tc._read_budget_state(tc._budget_state_path()))
        with mock.patch.object(tc, "_pipe_notify") as m:
            tc.run_budget_alert(cfg)
        self.assertEqual(m.call_count, 1)

    def test_alert_write_fail(self):
        """状态写失败(OSError)→ 告警仍发送,写失败静默不抛(E8)。"""
        cfg = self._over_cfg()
        with mock.patch.object(tc, "_atomic_write_local", side_effect=OSError("disk full")), \
                mock.patch.object(tc, "_pipe_notify") as m:
            tc.run_budget_alert(cfg)          # 不抛
        self.assertEqual(m.call_count, 1)

    def test_budget_notify_unconfigured(self):
        """host.notify 未配置 → 告警 no-op(E9);模板含控制字符 → 拒绝执行。"""
        cfg = self._over_cfg()
        with mock.patch.object(tc.subprocess, "run") as m:
            tc.run_budget_alert(cfg)
        m.assert_not_called()
        bad = self._over_cfg()
        bad["host"] = {"notify": "echo \x07", "wake_template": None}
        with mock.patch.object(tc.subprocess, "run") as m:
            tc.run_budget_alert(bad)          # 控制字符 → 拒绝,不调 run
        m.assert_not_called()

    def _reset_alert_state(self):
        """清掉 budget-alert.json:冷却期内再次调用会被 should_alert 静默跳过,
        本测试要验证的是 notify 失败路径本身,故每次调用前重置进入态。"""
        p = tc._budget_state_path()
        if os.path.exists(p):
            os.remove(p)

    def test_budget_notify_fail(self):
        """feishu 推送非零退出/OSError → 仅 stderr 告警,不阻断(E10)。"""
        cfg = self._over_cfg()
        cfg["host"] = {"notify": "false", "wake_template": None}   # exit 1
        with contextlib.redirect_stderr(io.StringIO()) as err:
            tc.run_budget_alert(cfg)
        self.assertIn("exit=1", err.getvalue())
        # 命令不存在:sh -c 返回 127(非零退出路径);OSError 路径单独 mock 验证
        cfg["host"] = {"notify": "/nonexistent/bin/notify", "wake_template": None}
        self._reset_alert_state()
        with contextlib.redirect_stderr(io.StringIO()) as err:
            tc.run_budget_alert(cfg)
        self.assertIn("exit=", err.getvalue())
        with mock.patch.object(tc.subprocess, "run",
                               side_effect=OSError("no such file")), \
                contextlib.redirect_stderr(io.StringIO()) as err:
            self._reset_alert_state()        # OSError 前同样重置,保证走到 notify
            tc.run_budget_alert(cfg)          # OSError 捕获,不抛
        self.assertIn("notify 调用失败", err.getvalue())

    def test_taskdir_isolation(self):
        """所有读写(含 budget-alert.json)均落在 FLOW_TASK_DIR 临时目录。"""
        cfg = self._over_cfg()
        with mock.patch.object(tc, "_pipe_notify"):
            tc.run_budget_alert(cfg)
        self.assertTrue(os.path.isfile(tc._budget_state_path()))
        self.assertTrue(os.path.abspath(tc._budget_state_path()).startswith(
            os.path.abspath(self.tmp)))
        self.assertTrue(os.path.isfile(tc.registry_path()))
        self.assertTrue(os.path.abspath(tc.registry_path()).startswith(os.path.abspath(self.tmp)))


# ---------------------------------------------------------------------------
# CLI:flow task budget(§2.4)
# ---------------------------------------------------------------------------

class BudgetCliTests(_IsoBase):
    def test_cmd_budget_json(self):
        _seed([_spend_task("t-000000000001", 1.0)])  # 7.2 CNY
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = tc.cmd_budget(["--json"], _cfg())
        self.assertEqual(rc, 0)
        out = json.loads(buf.getvalue().strip().splitlines()[-1])
        self.assertEqual(out["status"], "ok")
        self.assertEqual(out["month"]["used_cny"], 7.2)
        self.assertEqual(out["month"]["limit_cny"], 600.0)
        self.assertFalse(out["month"]["exceeded"])
        # 当前时刻任务同时归当月当周 → week 用量同为 7.2;week_cny=0 不启用
        self.assertEqual(out["week"]["used_cny"], 7.2)
        self.assertEqual(out["week"]["limit_cny"], 0.0)
        self.assertFalse(out["week"]["exceeded"])
        self.assertFalse(out["overall_exceeded"])
        self.assertIsNone(out["level"])
        self.assertEqual(out["pause_long_task_s"], 600)

    def test_cmd_budget_readable(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = tc.cmd_budget([], _cfg())
        self.assertEqual(rc, 0)
        text = buf.getvalue()
        self.assertIn("月用量", text)
        self.assertIn("周用量", text)
        self.assertIn("P0", text)

    def test_cmd_budget_tasks_missing(self):
        """tasks.json 读不到 → 用量 0,仍 exit 0(fail-closed §2.4)。"""
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = tc.cmd_budget(["--json"], _cfg())
        self.assertEqual(rc, 0)
        out = json.loads(buf.getvalue().strip().splitlines()[-1])
        self.assertEqual(out["month"]["used_cny"], 0.0)

    def test_cmd_budget_subcommand_registered(self):
        """SUBCOMMANDS 注册 + USAGE 文案。"""
        self.assertIn("budget", tc.SUBCOMMANDS)
        self.assertEqual(tc.SUBCOMMANDS["budget"], tc.cmd_budget)
        self.assertIn("flow task budget", tc.USAGE)


if __name__ == "__main__":
    unittest.main()
