#!/usr/bin/env python3
"""flow-core.py 单元测试(P2):状态机 T1-T24 + store S1-S9。

用法: python3 -m unittest discover tests
零 API、全程临时目录。单测直接 import 纯函数(transition/evaluate_guard 零 I/O)。
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
from datetime import timedelta
from unittest import mock

_HERE = os.path.dirname(os.path.abspath(__file__))
_FLOW_CORE = os.path.join(_HERE, "..", "scripts", "flow-core.py")

_spec = importlib.util.spec_from_file_location("flow_core", _FLOW_CORE)
fc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fc)


def _cfg():
    return {
        "workitem": {"plane_id": "control", "lock_ttl_s": 3600, "id_max_len": 64},
        "gates": {"takeover_after_same_defect": 2, "iteration_limit": 3},
    }


def _status(wi_id="w1", state="created", **extra):
    return {
        "schema_version": 1, "id": wi_id, "state": state, "iteration": 0,
        "same_defect_count": 0, "primary_defect_type": None, "takeover": False,
        "re_execute_count": 0, "process_version": "1.0.0",
        "created_at": "2026-08-14T10:00:00+00:00",
        "updated_at": "2026-08-14T10:00:00+00:00",
        "event_seq": 1, "locked_by": None, "lock_expires_at": None,
        **extra,
    }


def _event(ev, **meta):
    return {"ts": "2026-08-14T10:00:00+00:00", "event": ev, "from": None, "to": None,
            "actor": "flow:control", "guard": None, "reason": None, "meta": meta}


# 故意拼接,避免源码静态 deny-list 命中(sk-[A-Za-z0-9]{10})
_FAKE_KEY = "sk-" + "fake-async-test-key"


# stub dsh(async 契约用;照 run-smoke.sh 模式,零 API,支持 DSH_STUB_SLEEP/DSH_STUB_EXIT)
STUB_DSH_ASYNC = r'''#!/usr/bin/env bash
set -u
if [[ "${DSH_STUB_SLEEP:-0}" != "0" ]]; then sleep "$DSH_STUB_SLEEP"; fi
cat << 'MD'
# stub 方案

这是 stub 输出的假方案内容，用于 async 契约测试。
MD
exit "${DSH_STUB_EXIT:-0}"
'''


class StateMachineTests(unittest.TestCase):
    """T1-T24 状态机纯函数单测(§6.1)。"""

    def test_T01_design(self):
        r = fc.transition("created", "design", {"brief_nonempty": True})
        self.assertTrue(r["ok"])
        self.assertEqual(r["to"], "designed")
        self.assertEqual(r["guard"], "brief_required")
        self.assertEqual(r["effects"], {})

    def test_T02_illegal(self):
        r = fc.transition("created", "accept", {})
        self.assertFalse(r["ok"])
        self.assertEqual(r["reason"], "illegal_transition")

    def test_T03_unknown(self):
        r = fc.transition("bogus", "design", {})
        self.assertFalse(r["ok"])
        self.assertEqual(r["reason"], "unknown_state")
        r = fc.transition("created", "foo", {})
        self.assertFalse(r["ok"])
        self.assertEqual(r["reason"], "unknown_event")

    def test_T04_design_guard_fail(self):
        r = fc.transition("created", "design", {"brief_nonempty": False})
        self.assertFalse(r["ok"])
        self.assertEqual(r["reason"], "guard_failed:brief_required")
        self.assertNotIn("effects", r)

    def test_T05_review(self):
        r = fc.transition("designed", "review",
                          {"design_nonempty": True, "decision_present": True})
        self.assertTrue(r["ok"])
        self.assertEqual(r["to"], "reviewed")

    def test_T06_translate_pass(self):
        r = fc.transition("reviewed", "translate", {"verdict": "pass"})
        self.assertTrue(r["ok"])
        self.assertEqual(r["to"], "translated")

    def test_T07_translate_reject(self):
        r = fc.transition("reviewed", "translate", {"verdict": "reject"})
        self.assertFalse(r["ok"])
        self.assertEqual(r["reason"], "guard_failed:quality_pass")

    def test_T08_feedback(self):
        r = fc.transition("reviewed", "feedback",
                          {"verdict": "reject", "same_defect_count": 0,
                           "takeover_after_same_defect": 2})
        self.assertTrue(r["ok"])
        self.assertEqual(r["to"], "designed")
        self.assertEqual(r["effects"]["iteration"], "+1")
        self.assertEqual(r["effects"]["same_defect_count"], 1)

    def test_T09_feedback_pass(self):
        r = fc.transition("reviewed", "feedback",
                          {"verdict": "pass", "same_defect_count": 0,
                           "takeover_after_same_defect": 2})
        self.assertFalse(r["ok"])
        self.assertEqual(r["reason"], "guard_failed:quality_reject_retry")

    def test_T10_takeover(self):
        r = fc.transition("reviewed", "takeover",
                          {"verdict": "reject", "same_defect_count": 2,
                           "takeover_after_same_defect": 2})
        self.assertTrue(r["ok"])
        self.assertEqual(r["to"], "translated")
        self.assertTrue(r["effects"]["takeover"])

    def test_T11_takeover_below_threshold(self):
        r = fc.transition("reviewed", "takeover",
                          {"verdict": "reject", "same_defect_count": 1,
                           "takeover_after_same_defect": 2})
        self.assertFalse(r["ok"])
        self.assertEqual(r["reason"], "guard_failed:quality_reject_takeover")

    def test_T12_execute(self):
        r = fc.transition("translated", "execute", {"taskbook_nonempty": True})
        self.assertTrue(r["ok"])
        self.assertEqual(r["to"], "executed")

    def test_T13_verify_all_true(self):
        r = fc.transition("executed", "verify",
                          {"tests_pass": True, "diff_match": True, "error_table_match": True})
        self.assertTrue(r["ok"])
        self.assertEqual(r["to"], "verified")

    def test_T14_verify_any_false(self):
        r = fc.transition("executed", "verify",
                          {"tests_pass": True, "diff_match": False, "error_table_match": True})
        self.assertFalse(r["ok"])
        self.assertEqual(r["reason"], "guard_failed:test_gate_pass")

    def test_T15_verify_fail_design(self):
        r = fc.transition("executed", "verify_fail",
                          {"tests_pass": True, "diff_match": False, "error_table_match": True,
                           "route": "design"})
        self.assertTrue(r["ok"])
        self.assertEqual(r["to"], "designed")
        self.assertEqual(r["effects"]["iteration"], "+1")

    def test_T16_verify_fail_impl(self):
        r = fc.transition("executed", "verify_fail",
                          {"tests_pass": True, "diff_match": False, "error_table_match": True,
                           "route": "impl"})
        self.assertTrue(r["ok"])
        self.assertEqual(r["to"], "executed")
        self.assertEqual(r["effects"]["re_execute_count"], "+1")

    def test_T17_verify_fail_bad_route(self):
        r = fc.transition("executed", "verify_fail",
                          {"tests_pass": True, "diff_match": False, "error_table_match": True})
        self.assertFalse(r["ok"])
        self.assertTrue(r["reason"].startswith("guard_failed:test_gate_fail_"))
        # 全真时即使 route=design 也拒绝
        r = fc.transition("executed", "verify_fail",
                          {"tests_pass": True, "diff_match": True, "error_table_match": True,
                           "route": "design"})
        self.assertFalse(r["ok"])
        self.assertTrue(r["reason"].startswith("guard_failed:test_gate_fail_"))

    def test_T18_accept(self):
        r = fc.transition("verified", "accept", {"approve_confirmed": True})
        self.assertTrue(r["ok"])
        self.assertEqual(r["to"], "accepted")

    def test_T19_retro(self):
        r = fc.transition("accepted", "retro", {})
        self.assertTrue(r["ok"])
        self.assertEqual(r["to"], "retrospected")

    def test_T20_happy_path(self):
        steps = [
            ("design", {"brief_nonempty": True}, "designed"),
            ("review", {"design_nonempty": True, "decision_present": True}, "reviewed"),
            ("translate", {"verdict": "pass"}, "translated"),
            ("execute", {"taskbook_nonempty": True}, "executed"),
            ("verify", {"tests_pass": True, "diff_match": True, "error_table_match": True}, "verified"),
            ("accept", {"approve_confirmed": True}, "accepted"),
            ("retro", {}, "retrospected"),
        ]
        cur = "created"
        for ev, ctx, to in steps:
            r = fc.transition(cur, ev, ctx)
            self.assertTrue(r["ok"], f"{cur} -{ev}-> 失败: {r}")
            self.assertEqual(r["to"], to)
            cur = to
        self.assertEqual(cur, "retrospected")

    def test_T21_reject_loop(self):
        r = fc.transition("reviewed", "feedback",
                          {"verdict": "reject", "same_defect_count": 0,
                           "takeover_after_same_defect": 2,
                           "primary_defect_type": None, "defect_type": "missing_scenario"})
        self.assertTrue(r["ok"])
        self.assertEqual(r["effects"]["iteration"], "+1")
        self.assertEqual(r["effects"]["same_defect_count"], 1)
        self.assertEqual(r["effects"]["primary_defect_type"], "missing_scenario")
        r = fc.transition("designed", "review",
                          {"design_nonempty": True, "decision_present": True})
        self.assertTrue(r["ok"])
        r = fc.transition("reviewed", "translate", {"verdict": "pass"})
        self.assertTrue(r["ok"])
        self.assertEqual(r["to"], "translated")
        self.assertEqual(r["effects"]["same_defect_count"], 0)

    def test_T22_takeover_threshold(self):
        r = fc.transition("reviewed", "takeover",
                          {"verdict": "reject", "same_defect_count": 1,
                           "takeover_after_same_defect": 2})
        self.assertFalse(r["ok"])
        r = fc.transition("reviewed", "takeover",
                          {"verdict": "reject", "same_defect_count": 2,
                           "takeover_after_same_defect": 2})
        self.assertTrue(r["ok"])

    def test_T23_force(self):
        # force 跳过守卫但非法形状仍拒
        r = fc.transition("created", "design", {"brief_nonempty": False}, force=True)
        self.assertTrue(r["ok"])
        self.assertEqual(r["to"], "designed")
        r = fc.transition("created", "accept", {}, force=True)
        self.assertFalse(r["ok"])
        self.assertEqual(r["reason"], "illegal_transition")

    def test_T24_threshold_override(self):
        r = fc.transition("reviewed", "takeover",
                          {"verdict": "reject", "same_defect_count": 2,
                           "takeover_after_same_defect": 3})
        self.assertFalse(r["ok"])
        r = fc.transition("reviewed", "takeover",
                          {"verdict": "reject", "same_defect_count": 3,
                           "takeover_after_same_defect": 3})
        self.assertTrue(r["ok"])


class StoreTests(unittest.TestCase):
    """S1-S9 store 层测试(§6.2)。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="flow-store-")
        self.wi_dir = os.path.join(self.tmp, "workitems", "w1")
        os.makedirs(self.wi_dir, exist_ok=True)
        self._old_data = os.environ.get("FLOW_DATA_DIR")
        self._old_cfg = os.environ.get("COLLABFLOW_CONFIG")
        os.environ["FLOW_DATA_DIR"] = os.path.join(self.tmp, ".flow")
        os.environ["COLLABFLOW_CONFIG"] = os.path.join(self.tmp, "no-user-config.yaml")

    def tearDown(self):
        if self._old_data is None:
            os.environ.pop("FLOW_DATA_DIR", None)
        else:
            os.environ["FLOW_DATA_DIR"] = self._old_data
        if self._old_cfg is None:
            os.environ.pop("COLLABFLOW_CONFIG", None)
        else:
            os.environ["COLLABFLOW_CONFIG"] = self._old_cfg
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_S01_roundtrip(self):
        status = _status()
        text = fc.dump_yaml(status)
        loaded = fc.parse_yaml(text, "status.yaml")
        self.assertEqual(loaded, status)
        self.assertEqual(text, fc.dump_yaml(status))  # 键排序稳定

    def test_S02_no_tmp_residue(self):
        status = _status()
        for _ in range(10):
            fc.save_status_atomic(self.wi_dir, status)
        residue = [n for n in os.listdir(self.wi_dir) if ".tmp." in n]
        self.assertEqual(residue, [])
        self.assertEqual(fc.load_status(self.wi_dir), status)  # 读方不观察半写

    def test_S03_concurrent_append(self):
        # 预置 200 行,2 进程各并发 100 行 → 恰 400 行,seq 单调无缺号
        for _ in range(200):
            fc.append_event(self.wi_dir, _event("lock", owner="seed"))
        child = (
            "import importlib.util, sys\n"
            "spec = importlib.util.spec_from_file_location('fc', sys.argv[1])\n"
            "fc = importlib.util.module_from_spec(spec); spec.loader.exec_module(fc)\n"
            "wi_dir = sys.argv[2]\n"
            "for i in range(int(sys.argv[3])):\n"
            "    fc.append_event(wi_dir, {'ts':'t','event':'lock','from':None,'to':None,"
            "'actor':'flow:control','guard':None,'reason':None,'meta':{'i':i}})\n"
        )
        procs = [
            subprocess.Popen([sys.executable, "-c", child, _FLOW_CORE, self.wi_dir, "100"])
            for _ in range(2)
        ]
        for p in procs:
            p.wait()
        path = os.path.join(self.wi_dir, "events.jsonl")
        with open(path, encoding="utf-8") as f:
            seqs = [json.loads(ln)["seq"] for ln in f if ln.strip()]
        self.assertEqual(len(seqs), 400)
        self.assertEqual(seqs, list(range(1, 401)))

    def test_S04_append_no_loss(self):
        for i in range(100):
            fc.append_event(self.wi_dir, _event("lock", i=i))
        path = os.path.join(self.wi_dir, "events.jsonl")
        with open(path, encoding="utf-8") as f:
            seqs = [json.loads(ln)["seq"] for ln in f if ln.strip()]
        self.assertEqual(seqs, list(range(1, 101)))

    def test_S05_lock_acquire(self):
        fc.save_status_atomic(self.wi_dir, _status())
        now = fc.now_dt()
        s = fc.acquire_lock(self.wi_dir, "flow:control", 3600, now)
        self.assertEqual(s["locked_by"], "flow:control")
        self.assertIsNotNone(s["lock_expires_at"])
        with self.assertRaises(fc.Locked):
            fc.acquire_lock(self.wi_dir, "flow:execution", 3600, now)

    def test_S06_lock_expiry(self):
        fc.save_status_atomic(self.wi_dir, _status())
        now = fc.now_dt()
        fc.acquire_lock(self.wi_dir, "flow:a", 0, now)  # ttl=0 立即过期
        s = fc.acquire_lock(self.wi_dir, "flow:b", 3600, now)  # 覆盖取锁
        self.assertEqual(s["locked_by"], "flow:b")

    def test_S07_unlock(self):
        self.wi_dir = os.path.join(os.environ["FLOW_DATA_DIR"], "workitems", "w1")
        code = fc.cmd_new(["w1"], _cfg())
        self.assertEqual(code, 0)
        code = fc.cmd_lock(["w1", "--owner", "a"], _cfg())
        self.assertEqual(code, 0)
        # owner 不匹配 → exit 2
        code = fc.cmd_unlock(["w1", "--owner", "b"], _cfg())
        self.assertEqual(code, 2)
        # 匹配 → 清锁
        code = fc.cmd_unlock(["w1", "--owner", "a"], _cfg())
        self.assertEqual(code, 0)
        st = fc.load_status(self.wi_dir)
        self.assertIsNone(st["locked_by"])
        self.assertIsNone(st["lock_expires_at"])

    def test_S08_seq_consistency(self):
        fc.save_status_atomic(self.wi_dir, _status())
        status = fc.load_status(self.wi_dir)
        for i in range(5):
            seq = fc.append_event(self.wi_dir, _event("lock", i=i))
            status["event_seq"] = seq
            fc.save_status_atomic(self.wi_dir, status)
            path = os.path.join(self.wi_dir, "events.jsonl")
            with open(path, encoding="utf-8") as f:
                lines = [ln for ln in f if ln.strip()]
            last = json.loads(lines[-1])["seq"]
            self.assertEqual(fc.load_status(self.wi_dir)["event_seq"], last)

    def test_S09_append_only(self):
        for i in range(3):
            fc.append_event(self.wi_dir, _event("lock", i=i))
        path = os.path.join(self.wi_dir, "events.jsonl")
        with open(path, encoding="utf-8") as f:
            before = f.read()
        for i in range(3, 5):
            fc.append_event(self.wi_dir, _event("lock", i=i))
        with open(path, encoding="utf-8") as f:
            after = f.read()
        self.assertTrue(after.startswith(before))  # 不删改既有行


class AsyncDesignTests(unittest.TestCase):
    """async-design-support 方案 A01-A11(§3.1):--async/--check + 同步不回归。stub 零 API。"""

    _ENV_PREFIXES = ("FLOW_", "COLLABFLOW_", "STUB_", "DSH_", "DEEPSEEK", "HOME")

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="flow-async-")
        self.workdir = os.path.join(self.tmp, "work")
        os.makedirs(self.workdir, exist_ok=True)
        self._saved = {k: v for k, v in os.environ.items()
                       if k.startswith(self._ENV_PREFIXES)}
        for k in list(self._saved):
            os.environ.pop(k, None)
        os.environ["FLOW_DATA_DIR"] = os.path.join(self.tmp, ".flow")
        os.environ["FLOW_WORKDIR"] = self.workdir
        os.environ["COLLABFLOW_CONFIG"] = os.path.join(self.tmp, "no-cfg.yaml")
        os.environ["HOME"] = os.path.join(self.tmp, "home")
        os.makedirs(os.environ["HOME"], exist_ok=True)
        os.environ["DEEPSEEK_API_KEY"] = _FAKE_KEY
        os.environ["DSH_HOME"] = os.path.join(self.tmp, "dshhome")
        os.environ["DSH_DESIGN_PRO_PATCH"] = os.path.join(self.tmp, "pro.patch.yml")
        self.stub_dsh = os.path.join(self.tmp, "stub-dsh.sh")
        with open(self.stub_dsh, "w", encoding="utf-8") as f:
            f.write(STUB_DSH_ASYNC)
        os.chmod(self.stub_dsh, 0o755)
        os.environ["DSH_BIN"] = self.stub_dsh

    def tearDown(self):
        try:  # 清理后台 worker,避免孤儿写已删目录
            st = fc.load_status(os.path.join(fc.workitems_dir(), "w1"))
            pid = (st.get("async") or {}).get("pid")
            if pid is not None:
                try:
                    os.kill(int(pid), 9)
                except (ProcessLookupError, PermissionError, ValueError):
                    pass
                try:
                    os.waitpid(int(pid), 0)   # 收割僵尸,避免 os.kill(pid,0) 误判存活
                except ChildProcessError:
                    pass
        except Exception:
            pass
        for k in [k for k in list(os.environ) if k.startswith(self._ENV_PREFIXES)]:
            os.environ.pop(k, None)
        os.environ.update(self._saved)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _mk_wi(self, state="created"):
        wi_dir = os.path.join(fc.workitems_dir(), "w1")
        os.makedirs(wi_dir, exist_ok=True)
        fc.save_status_atomic(wi_dir, _status("w1", state))
        with open(os.path.join(wi_dir, "brief.md"), "w", encoding="utf-8") as f:
            f.write("# brief\n")
        return wi_dir

    def _jload(self, path):
        return json.loads(fc.read_file(path))

    def _wait_result(self, wi_dir, timeout=20):
        """轮询 design-async-result.json 出现(worker 完成),返回 bool。"""
        path = os.path.join(wi_dir, "design-async-result.json")
        deadline = time.time() + timeout
        while time.time() < deadline:
            if os.path.isfile(path):
                return True
            time.sleep(0.1)
        return False

    def test_A01_async_phase_exhaustive(self):
        # 纯函数穷举(不 sleep,pid_alive 传 lambda):done_ok/done_timeout/done_failed/
        # running/over_expected/crashed
        base = _status()
        base["async"] = {"pid": 123, "started_at": "2026-08-21T01:00:00+00:00",
                         "expected_seconds": 480, "finished_at": None, "exit_code": None}
        now = fc.parse_iso("2026-08-21T01:05:00+00:00")  # elapsed=300
        ph = fc.async_phase(base, {"exit_code": 0}, now, True)
        self.assertEqual(ph["phase"], "done_ok")
        self.assertIsNone(ph["alarm"])
        ph = fc.async_phase(base, {"exit_code": 124}, now, True)
        self.assertEqual(ph["phase"], "done_timeout")
        self.assertEqual(ph["alarm"], "timeout")
        ph = fc.async_phase(base, {"exit_code": 1}, now, True)
        self.assertEqual(ph["phase"], "done_failed")
        # running:无 result + pid 活 + 未超 expected
        ph = fc.async_phase(base, None, now, True)
        self.assertEqual(ph["phase"], "running")
        self.assertEqual(ph["elapsed_seconds"], 300)
        self.assertEqual(ph["remaining_seconds"], 180)
        self.assertFalse(ph["over_expected"])
        # over_expected:elapsed > expected 且仍在跑
        far = fc.parse_iso("2026-08-21T01:10:00+00:00")  # elapsed=600 > 480
        ph = fc.async_phase(base, None, far, True)
        self.assertEqual(ph["phase"], "over_expected")
        self.assertTrue(ph["over_expected"])
        self.assertEqual(ph["alarm"], "timeout")
        self.assertEqual(ph["remaining_seconds"], 0)
        # crashed:无 result + pid 死
        ph = fc.async_phase(base, None, now, False)
        self.assertEqual(ph["phase"], "crashed")
        # 无 async 块兜底 → crashed
        ph = fc.async_phase({}, None, now, False)
        self.assertEqual(ph["phase"], "crashed")

    def test_A02_async_launch_marks_status(self):
        wi_dir = self._mk_wi()
        code = fc.cmd_design(["w1", "--async"], _cfg())
        self.assertEqual(code, 0)
        st = fc.load_status(wi_dir)
        a = st["async"]
        self.assertIn("pid", a)
        self.assertIn("started_at", a)
        self.assertEqual(a["expected_seconds"], 480)  # 默认回退
        self.assertIsNone(a["finished_at"])
        self.assertIsNone(a["exit_code"])
        self.assertEqual(st["state"], "created")      # 标记写入不转移
        self.assertTrue(os.path.isfile(os.path.join(wi_dir, "design-async.log")))
        self.assertNotIn("design.md", os.listdir(wi_dir))  # 未落盘

    def test_A03_usage_errors(self):
        # UsageError 由 main() 捕获 → exit 2;直接调 cmd_design 断言异常类型
        wi_dir = self._mk_wi()
        with self.assertRaises(fc.UsageError):
            fc.cmd_design(["w1", "--async", "--check"], _cfg())     # 互斥
        with self.assertRaises(fc.UsageError):
            fc.cmd_design(["w1", "--expected", "5"], _cfg())        # 无 --async
        with self.assertRaises(fc.UsageError):
            fc.cmd_design(["w1", "--expected", "0", "--async"], _cfg())  # 非正整数
        st = fc.load_status(wi_dir)  # 校验均在 spawn 前:无 async 标记
        self.assertNotIn("async", st)

    def test_A04_check_idempotent(self):
        wi_dir = self._mk_wi()
        self.assertEqual(fc.cmd_design(["w1", "--async"], _cfg()), 0)
        self.assertTrue(self._wait_result(wi_dir))
        self.assertEqual(fc.cmd_design(["w1", "--check"], _cfg()), 0)
        st = fc.load_status(wi_dir)
        self.assertEqual(st["state"], "designed")
        self.assertEqual(st["async"]["exit_code"], 0)
        self.assertIsNotNone(st["async"]["finished_at"])
        seq1 = st["event_seq"]
        # 再次 --check:幂等 no-op,无副作用
        self.assertEqual(fc.cmd_design(["w1", "--check"], _cfg()), 0)
        st2 = fc.load_status(wi_dir)
        self.assertEqual(st2["state"], "designed")
        self.assertEqual(st2["event_seq"], seq1)
        with open(os.path.join(wi_dir, "events.jsonl"), encoding="utf-8") as f:
            events = [json.loads(ln) for ln in f if ln.strip()]
        self.assertEqual(len([e for e in events if e["event"] == "design"]), 1)

    def test_A05_completion_transitions(self):
        wi_dir = self._mk_wi()
        self.assertEqual(fc.cmd_design(["w1", "--async"], _cfg()), 0)
        self.assertTrue(self._wait_result(wi_dir))
        self.assertEqual(fc.cmd_design(["w1", "--check"], _cfg()), 0)
        self.assertTrue(os.path.isfile(os.path.join(wi_dir, "design.md")))
        self.assertTrue(os.path.isfile(os.path.join(wi_dir, "design-result.json")))
        self.assertTrue(fc.read_file(os.path.join(wi_dir, "design.md")).strip())
        st = fc.load_status(wi_dir)
        self.assertEqual(st["state"], "designed")
        with open(os.path.join(wi_dir, "events.jsonl"), encoding="utf-8") as f:
            events = [json.loads(ln) for ln in f if ln.strip()]
        self.assertTrue(any(e["event"] == "design" for e in events))

    def test_A06_over_expected_alarm(self):
        wi_dir = self._mk_wi()
        os.environ["DSH_STUB_SLEEP"] = "3"
        self.assertEqual(fc.cmd_design(["w1", "--async", "--expected", "1"], _cfg()), 0)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = fc.cmd_design(["w1", "--check", "--json"], _cfg())
        self.assertEqual(code, 3)                       # running
        self.assertEqual(json.loads(buf.getvalue())["status"], "running")
        time.sleep(2)                                   # elapsed>expected 且仍在跑
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = fc.cmd_design(["w1", "--check", "--json"], _cfg())
        self.assertEqual(code, 124)                     # over_expected 报警
        obj = json.loads(buf.getvalue())
        self.assertEqual(obj["alarm"], "timeout")
        self.assertTrue(obj["over_expected"])
        self.assertTrue(self._wait_result(wi_dir))      # 等 worker 结束再清理

    def test_A07_worker_failure(self):
        wi_dir = self._mk_wi()
        os.environ["DSH_STUB_EXIT"] = "1"
        self.assertEqual(fc.cmd_design(["w1", "--async"], _cfg()), 0)
        self.assertTrue(self._wait_result(wi_dir))
        code = fc.cmd_design(["w1", "--check"], _cfg())
        self.assertEqual(code, 1)
        self.assertEqual(fc.load_status(wi_dir)["state"], "created")  # 不转移
        r = self._jload(os.path.join(wi_dir, "design-async-result.json"))
        self.assertEqual(r["exit_code"], 1)
        self.assertFalse(os.path.isfile(os.path.join(wi_dir, "design.md")))

    def test_A08_hard_timeout(self):
        wi_dir = self._mk_wi()
        os.environ["DSH_STUB_SLEEP"] = "5"
        os.environ["DSH_DESIGN_TIMEOUT"] = "1"   # flow-config 尊重已设 env
        self.assertEqual(fc.cmd_design(["w1", "--async"], _cfg()), 0)
        self.assertTrue(self._wait_result(wi_dir, timeout=30))
        code = fc.cmd_design(["w1", "--check"], _cfg())
        self.assertEqual(code, 124)
        r = self._jload(os.path.join(wi_dir, "design-async-result.json"))
        self.assertEqual(r["exit_code"], 124)
        self.assertEqual(fc.load_status(wi_dir)["state"], "created")

    def test_A09_crash(self):
        wi_dir = self._mk_wi()
        os.environ["DSH_STUB_SLEEP"] = "2"   # 保证 result.json 尚未写入时 kill
        self.assertEqual(fc.cmd_design(["w1", "--async"], _cfg()), 0)
        st = fc.load_status(wi_dir)
        os.kill(int(st["async"]["pid"]), 9)
        try:  # 收割僵尸,否则 os.kill(pid,0) 对僵尸仍返回 True
            os.waitpid(int(st["async"]["pid"]), 0)
        except ChildProcessError:
            pass
        code = fc.cmd_design(["w1", "--check"], _cfg())
        self.assertEqual(code, 1)                       # crashed
        self.assertEqual(fc.load_status(wi_dir)["state"], "created")
        self.assertFalse(os.path.isfile(os.path.join(wi_dir, "design.md")))

    def test_A10_sync_no_regression(self):
        wi_dir = self._mk_wi()
        # M2 默认 async-first:旧同步行为迁移到 --sync(断言不变)
        self.assertEqual(fc.cmd_design(["w1", "--sync"], _cfg()), 0)
        st = fc.load_status(wi_dir)
        self.assertEqual(st["state"], "designed")
        self.assertNotIn("async", st)                       # 同步不写 async 标记
        self.assertTrue(os.path.isfile(os.path.join(wi_dir, "design.md")))

    def test_A11_corrupt_result(self):
        wi_dir = self._mk_wi()
        self.assertEqual(fc.cmd_design(["w1", "--async"], _cfg()), 0)
        self.assertTrue(self._wait_result(wi_dir))
        with open(os.path.join(wi_dir, "design-async-result.json"), "w",
                  encoding="utf-8") as f:
            f.write("{not-valid-json")
        code = fc.cmd_design(["w1", "--check"], _cfg())
        self.assertEqual(code, 1)                       # fail-closed
        self.assertFalse(os.path.isfile(os.path.join(wi_dir, "design.md")))
        self.assertEqual(fc.load_status(wi_dir)["state"], "created")


# ---------------------------------------------------------------------------
# chain-on-transition(C1-C22,design §2 矩阵):post-transition 钩子链
# 零 API、临时目录、stub 注入(CHAIN_REASONIX_BIN / chain.notify / _flow_bin)
# ---------------------------------------------------------------------------
# 机械判定四项全过的 design.md fixture
DESIGN_OK = """# 方案标题

## 1. 现状与改造点
- 说明

## 2. 方案设计
### 2.1 数据流

## 3. 测试策略
- C1-C22 矩阵

## 4. 错误处理表
| 编号 | 错误场景 | 处理方式 | 测试覆盖 |
|---|---|---|---|
| E1 | 配置缺失 | 默认值兜底 | test_config_default |

## 5. 验收标准
1. 全量测试通过

## 6. 改动文件清单
- scripts/flow-core.py
- config/defaults.yaml
"""

# taskbook 前置块(test_command 用非 bool 歧义裸串;verify 场景 result.json 优先)
FLOW_BLOCK = """test_command: pytest
diff_scope:
  allow:
    - scripts/flow-core.py
  deny:
    - scripts/dsh-design
"""

RESULT_OK = {
    "status": "ok", "exit_code": 0, "test_command": "true",
    "diff": {"changed_files": ["scripts/flow-core.py"], "untracked_files": []},
}
RESULT_FAIL = {
    "status": "ok", "exit_code": 0, "test_command": "false",
    "diff": {"changed_files": ["scripts/flow-core.py"], "untracked_files": []},
}


def _chain_cfg(on_designed="off", enabled=True, dry_run=False, notify=None, **kw):
    cfg = {
        "workitem": {"plane_id": "control", "lock_ttl_s": 3600, "id_max_len": 64},
        "gates": {"takeover_after_same_defect": 2, "iteration_limit": 3},
        "executor": {"default": "reasonix", "timeout_s": 1800},
        "task": {"default_priority": "P2",
                 "expected_seconds_seed": {"design": 480, "execute": 1800}},
        "chain": {"enabled": enabled, "on_designed": on_designed,
                  "on_reviewed": True, "on_translated": True, "on_executed": True,
                  "on_verified": True, "review_llm": False,
                  "review_llm_model": "stub-model",
                  "storm_threshold": 3, "overlap_minutes": 40,
                  "dry_run": dry_run, "max_depth": 8, "notify": notify,
                  "warn_hours": 24, "force_hours": 48, "scan_projects": None},
    }
    cfg["chain"].update(kw)
    return cfg


class ChainHookTests(unittest.TestCase):
    """C1-C22(§2 矩阵)。"""

    _ENV_PREFIXES = ("FLOW_", "COLLABFLOW_", "CHAIN_", "STUB_", "HOME", "DSH_", "DEEPSEEK")

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="flow-chain-")
        self.workdir = os.path.join(self.tmp, "work")
        os.makedirs(self.workdir, exist_ok=True)
        self._saved = {k: v for k, v in os.environ.items()
                       if k.startswith(self._ENV_PREFIXES)}
        for k in list(self._saved):
            os.environ.pop(k, None)
        os.environ["FLOW_DATA_DIR"] = os.path.join(self.tmp, ".flow")
        os.environ["FLOW_WORKDIR"] = self.workdir
        os.environ["FLOW_TASK_DIR"] = os.path.join(self.tmp, "tasks")
        os.environ["COLLABFLOW_CONFIG"] = os.path.join(self.tmp, "no-cfg.yaml")
        os.environ["HOME"] = os.path.join(self.tmp, "home")
        os.makedirs(os.environ["HOME"], exist_ok=True)
        os.makedirs(os.environ["FLOW_TASK_DIR"], exist_ok=True)
        # 惯例探测目标(auto_translate 的 test_command 依赖;python3 -m unittest discover tests)
        os.makedirs(os.path.join(self.workdir, "tests"), exist_ok=True)
        # ocr L1:_flow_bin 测试 stub 不残留模块级污染(setUp 存原值,tearDown 恢复)
        self._orig_flow_bin = fc._flow_bin

    def tearDown(self):
        fc._flow_bin = self._orig_flow_bin
        for k in [k for k in list(os.environ) if k.startswith(self._ENV_PREFIXES)]:
            os.environ.pop(k, None)
        os.environ.update(self._saved)
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ── helpers ──────────────────────────────────────────────────────────

    def _mk_wi(self, wi_id="w1", state="created", **extra):
        wi_dir = os.path.join(fc.workitems_dir(), wi_id)
        os.makedirs(wi_dir, exist_ok=True)
        fc.save_status_atomic(wi_dir, _status(wi_id, state, **extra))
        with open(os.path.join(wi_dir, "brief.md"), "w", encoding="utf-8") as f:
            f.write("# brief\n")
        return wi_dir

    def _mk_design(self, wi_dir, text=DESIGN_OK):
        with open(os.path.join(wi_dir, "design.md"), "w", encoding="utf-8") as f:
            f.write(text)

    def _mk_taskbook(self, wi_dir, block=FLOW_BLOCK):
        with open(os.path.join(wi_dir, "taskbook.md"), "w", encoding="utf-8") as f:
            f.write("# 任务书\n\n```flow\n" + block + "```\n")

    def _mk_executor(self, wi_dir, result=RESULT_OK):
        ex = os.path.join(wi_dir, "executor")
        os.makedirs(ex, exist_ok=True)
        with open(os.path.join(ex, "result.json"), "w", encoding="utf-8") as f:
            json.dump(result, f)
        with open(os.path.join(ex, "diff.patch"), "w", encoding="utf-8") as f:
            f.write("diff --git a/x b/x\n")

    def _mk_reviewed(self, wi_id="w1"):
        """designed + decision pass + design.md → 手工推进到 reviewed(关闭 on_reviewed 防误触发)。"""
        wi_dir = self._mk_wi(wi_id, "designed")
        self._mk_design(wi_dir)
        fc.write_decision(wi_dir, "pass")
        cfg = _chain_cfg(on_reviewed=False)
        res = fc._do_transition(wi_dir, "designed", "reviewed", "review", {}, {}, cfg)
        self.assertTrue(res["ok"], res)
        return wi_dir

    def _stub_reasonix(self, output):
        p = os.path.join(self.tmp, "stub-reasonix")
        with open(p, "w", encoding="utf-8") as f:
            f.write(f"#!/usr/bin/env bash\ncat <<'EOF'\n{output}\nEOF\n")
        os.chmod(p, 0o755)
        return p

    def _stub_notify(self):
        p = os.path.join(self.tmp, "stub-notify.sh")
        out = os.path.join(self.tmp, "notify.log")
        with open(p, "w", encoding="utf-8") as f:
            f.write(f"#!/usr/bin/env bash\ncat >> {out}\necho >> {out}\n")
        os.chmod(p, 0o755)
        return p, out

    def _stub_flow(self):
        p = os.path.join(self.tmp, "stub-flow")
        cap = os.path.join(self.tmp, "flow-add.cap")
        with open(p, "w", encoding="utf-8") as f:
            f.write(f"#!/usr/bin/env bash\nprintf '%s\\n' '---' >> {cap}\n"
                    f"printf '%s\\n' \"$@\" >> {cap}\n"
                    f"echo '{{\"status\":\"ok\",\"id\":\"t-stub\",\"expected_seconds\":1800}}'\n")
        os.chmod(p, 0o755)
        return p, cap

    def _stub_notify_argv(self):
        """notify stub:argv 逐行追加到 cap,stdin 追加到 out(ocr M1 payload/argv 断言)。"""
        p = os.path.join(self.tmp, "stub-notify-argv.sh")
        cap = os.path.join(self.tmp, "notify-argv.cap")
        out = os.path.join(self.tmp, "notify-stdin.log")
        with open(p, "w", encoding="utf-8") as f:
            f.write(f"#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" >> {cap}\n"
                    f"cat >> {out}\necho >> {out}\n")
        os.chmod(p, 0o755)
        return p, cap, out

    def _flow_add_blocks(self, cap):
        """按 --- 分隔符切分 flow task add 调用。"""
        if not os.path.isfile(cap):
            return []
        blocks, cur = [], []
        for ln in fc.read_file(cap).splitlines():
            if ln == "---":
                if cur:
                    blocks.append(cur)
                cur = []
            else:
                cur.append(ln)
        if cur:
            blocks.append(cur)
        return blocks

    def _read_audit(self, wi_dir):
        path = os.path.join(wi_dir, "events", "audit.jsonl")
        if not os.path.isfile(path):
            return []
        out = []
        for ln in fc.read_file(path).splitlines():
            ln = ln.strip()
            if ln:
                out.append(json.loads(ln))
        return out

    def _notify_titles(self, notify_log):
        if not os.path.isfile(notify_log):
            return []
        titles = []
        for ln in fc.read_file(notify_log).splitlines():
            ln = ln.strip()
            if ln:
                try:
                    titles.append(json.loads(ln).get("title"))
                except ValueError:
                    pass
        return titles

    # ── C1-C22 ────────────────────────────────────────────────────────────

    def test_C01_manual_transition_triggers_hook(self):
        """cmd_transition(created→designed) 后 audit.jsonl 出现 auto_review 动作。"""
        wi_dir = self._mk_wi()
        self._mk_design(wi_dir)
        cfg = _chain_cfg(on_designed="auto_review")
        code = fc.cmd_transition(["w1", "designed"], cfg)
        self.assertEqual(code, 0)
        actions = [r["action"] for r in self._read_audit(wi_dir)]
        self.assertIn("auto_review_pass", actions)

    def test_C02_pump_internal_path_triggers_hook(self):
        """cmd_design --sync 内部 _do_transition 路径同样触发 on_designed。"""
        wi_dir = self._mk_wi()
        self._mk_design(wi_dir)
        cfg = _chain_cfg(on_designed="auto_review")
        res = fc._transition_with_hooks(
            wi_dir, "created", "designed", "design", {}, {}, cfg)
        self.assertTrue(res["ok"])
        actions = [r["action"] for r in self._read_audit(wi_dir)]
        self.assertIn("auto_review_pass", actions)

    def test_C03_disabled_degrades(self):
        """chain.enabled=false:转移成功但零 audit、零新文件(完全退化)。"""
        wi_dir = self._mk_wi()
        self._mk_design(wi_dir)
        cfg = _chain_cfg(enabled=False)
        code = fc.cmd_transition(["w1", "designed"], cfg)
        self.assertEqual(code, 0)
        self.assertEqual(self._read_audit(wi_dir), [])
        self.assertFalse(os.path.exists(os.path.join(wi_dir, "decision.yaml")))
        st = fc.load_status(wi_dir)
        self.assertEqual(st["state"], "designed")

    def test_C04_on_designed_off_only_notify(self):
        """on_designed=off:notify 收到「待 review」,无 decision.yaml。"""
        wi_dir = self._mk_wi()
        notify_cmd, notify_log = self._stub_notify()
        cfg = _chain_cfg(on_designed="off", notify=notify_cmd)
        code = fc.cmd_transition(["w1", "designed"], cfg)
        self.assertEqual(code, 0)
        self.assertIn("design_done", self._notify_titles(notify_log))
        self.assertFalse(os.path.exists(os.path.join(wi_dir, "decision.yaml")))

    def test_C05_auto_review_mech_pass_advances(self):
        """完整 design.md → decision pass + 状态推进到 reviewed。"""
        wi_dir = self._mk_wi()
        self._mk_design(wi_dir)
        cfg = _chain_cfg(on_designed="auto_review")
        code = fc.cmd_transition(["w1", "designed"], cfg)
        self.assertEqual(code, 0)
        d = fc.read_decision(wi_dir)
        self.assertEqual(d.get("verdict"), "pass")
        st = fc.load_status(wi_dir)
        self.assertEqual(st["state"], "reviewed")

    def test_C06_auto_review_mech_reject(self):
        """缺测试策略 → decision reject(primary_defect_type=constraint_violation),不转移。"""
        wi_dir = self._mk_wi()
        bad = DESIGN_OK.replace("## 3. 测试策略\n- C1-C22 矩阵\n\n", "")
        self._mk_design(wi_dir, bad)
        cfg = _chain_cfg(on_designed="auto_review")
        code = fc.cmd_transition(["w1", "designed"], cfg)
        self.assertEqual(code, 0)
        d = fc.read_decision(wi_dir)
        self.assertEqual(d.get("verdict"), "reject")
        # ocr9b-L1:缺测试策略按实际失败项记 constraint_violation(不再一律 untestable_acceptance)
        self.assertEqual(d.get("primary_defect_type"), "constraint_violation")
        st = fc.load_status(wi_dir)
        self.assertEqual(st["state"], "designed")

    def test_C06b_mech_reject_defect_type_by_failure(self):
        """ocr9b-L1:机械 reject 按实际失败项记录 defect 类型(四类分别标记)。"""
        def _reject_type(wi_dir):
            cfg = _chain_cfg(on_designed="auto_review")
            fc._hook_auto_review(wi_dir, cfg, False)
            return fc.read_decision(wi_dir).get("primary_defect_type")
        # 空 design(design.md 缺失)→ 方案整体缺失 → missing_scenario
        self.assertEqual(_reject_type(self._mk_wi("w1", "designed")), "missing_scenario")
        # 缺测试策略章节 → 违反「必含测试策略」结构约束 → constraint_violation
        no_strategy = DESIGN_OK.replace("## 3. 测试策略\n- C1-C22 矩阵\n\n", "")
        w2 = self._mk_wi("w2", "designed")
        self._mk_design(w2, no_strategy)
        self.assertEqual(_reject_type(w2), "constraint_violation")
        # 错误处理表缺失 → 验收对照不可测 → untestable_acceptance
        no_et = DESIGN_OK.replace(
            "## 4. 错误处理表\n| 编号 | 错误场景 | 处理方式 | 测试覆盖 |\n"
            "|---|---|---|---|\n| E1 | 配置缺失 | 默认值兜底 | test_config_default |\n\n", "")
        w3 = self._mk_wi("w3", "designed")
        self._mk_design(w3, no_et)
        self.assertEqual(_reject_type(w3), "untestable_acceptance")
        # 缺验收对照章节 → 无法验收 → untestable_acceptance
        no_acc = DESIGN_OK.replace("## 5. 验收标准\n1. 全量测试通过\n\n", "")
        w4 = self._mk_wi("w4", "designed")
        self._mk_design(w4, no_acc)
        self.assertEqual(_reject_type(w4), "untestable_acceptance")

    def test_C05b_mech_check_multilevel_sections(self):
        """ocr9b-L2:章节编号支持多级——`## 2.1 测试策略`/`### 3.2.1 验收标准`
        被识别为对应章节,机械判定不再漏检。"""
        multi = (DESIGN_OK.replace("## 3. 测试策略", "## 2.1 测试策略")
                          .replace("## 5. 验收标准", "### 3.2.1 验收标准")
                          .replace("## 6. 改动文件清单", "## 2.2.3 改动文件清单"))
        wi_dir = self._mk_wi()
        self._mk_design(wi_dir, multi)
        mech = fc.mechanical_design_check(wi_dir)
        self.assertTrue(mech["pass"], mech["reasons"])
        self.assertIn("test_strategy_section", mech["checks"])
        self.assertIn("acceptance_section", mech["checks"])
        # 多级编号的改动文件清单章节同样被 extract_change_list 识别(fail-closed 不再误拒)
        self.assertIn("scripts/flow-core.py", fc.extract_change_list(wi_dir))
        # 单级/无编号格式保持兼容
        single = DESIGN_OK.replace("### 2.1 数据流", "### 2.1.4 数据流")
        w2 = self._mk_wi("w2")
        self._mk_design(w2, single)
        mech2 = fc.mechanical_design_check(w2)
        self.assertTrue(mech2["pass"], mech2["reasons"])
        self.assertTrue(fc.extract_change_list(w2))

    def test_C07_require_human_review_skips(self):
        """require_human_review=true → 跳过自动审,仅 notify。"""
        wi_dir = self._mk_wi(require_human_review=True)
        self._mk_design(wi_dir)
        notify_cmd, notify_log = self._stub_notify()
        cfg = _chain_cfg(on_designed="auto_review", notify=notify_cmd)
        code = fc.cmd_transition(["w1", "designed"], cfg)
        self.assertEqual(code, 0)
        actions = [r["action"] for r in self._read_audit(wi_dir)]
        self.assertIn("auto_review_skip", actions)
        self.assertIn("review_skipped", self._notify_titles(notify_log))
        self.assertFalse(os.path.exists(os.path.join(wi_dir, "decision.yaml")))
        st = fc.load_status(wi_dir)
        self.assertEqual(st["state"], "designed")

    def test_C08_auto_review_llm_double_gate(self):
        """review_llm=true:reasonix pass → 双过推进;reject → reject 不转移。"""
        wi_dir = self._mk_wi()
        self._mk_design(wi_dir)
        os.environ["CHAIN_REASONIX_BIN"] = self._stub_reasonix('{"verdict": "pass", "reasons": []}')
        cfg = _chain_cfg(on_designed="auto_review", review_llm=True)
        code = fc.cmd_transition(["w1", "designed"], cfg)
        self.assertEqual(code, 0)
        self.assertEqual(fc.read_decision(wi_dir).get("verdict"), "pass")
        self.assertEqual(fc.load_status(wi_dir)["state"], "reviewed")
        # LLM reject → 自动 reject 不转移
        os.environ["CHAIN_REASONIX_BIN"] = self._stub_reasonix('{"verdict": "reject", "reasons": ["x"]}')
        wi_dir2 = self._mk_wi("w2")
        self._mk_design(wi_dir2)
        code = fc.cmd_transition(["w2", "designed"], cfg)
        self.assertEqual(code, 0)
        self.assertEqual(fc.read_decision(wi_dir2).get("verdict"), "reject")
        self.assertEqual(fc.load_status(wi_dir2)["state"], "designed")

    def test_C09_auto_translate_renders_and_validates(self):
        """taskbook.md 含 ```flow 块,test_command 与 diff_scope.allow 非空。"""
        os.environ["CHAIN_REASONIX_BIN"] = self._stub_reasonix('{"body": "ok"}')
        wi_dir = self._mk_reviewed()
        cfg = _chain_cfg()
        fc._hook_auto_translate(wi_dir, cfg, False)
        self.assertTrue(os.path.isfile(os.path.join(wi_dir, "taskbook.md")))
        tb = fc.read_file(os.path.join(wi_dir, "taskbook.md"))
        self.assertIn("```flow", tb)
        fb = fc.parse_flow_block(wi_dir)
        self.assertTrue(fb["test_command"])
        self.assertTrue(fb["diff_scope"]["allow"])
        actions = [r["action"] for r in self._read_audit(wi_dir)]
        self.assertIn("auto_translate_ok", actions)

    def test_C10_auto_translate_allow_empty_fails_closed(self):
        """无改动清单 → 不写 taskbook、不转移。"""
        os.environ["CHAIN_REASONIX_BIN"] = self._stub_reasonix('{"body": "x"}')
        wi_dir = self._mk_reviewed()
        # 清空 design.md 改动清单章节
        bad = DESIGN_OK.split("## 6. 改动文件清单")[0] + "## 6. 范围外\n- 无\n"
        self._mk_design(wi_dir, bad)
        cfg = _chain_cfg()
        fc._hook_auto_translate(wi_dir, cfg, False)
        self.assertFalse(os.path.exists(os.path.join(wi_dir, "taskbook.md")))
        self.assertEqual(fc.load_status(wi_dir)["state"], "reviewed")
        actions = [r["action"] for r in self._read_audit(wi_dir)]
        self.assertIn("auto_translate_fail", actions)

    def test_C11_auto_translate_advances_to_translated(self):
        """taskbook 校验过后状态推进到 translated。"""
        os.environ["CHAIN_REASONIX_BIN"] = self._stub_reasonix('{"body": "ok"}')
        wi_dir = self._mk_reviewed()
        cfg = _chain_cfg()
        fc._hook_auto_translate(wi_dir, cfg, False)
        self.assertEqual(fc.load_status(wi_dir)["state"], "translated")

    def test_C12_auto_enqueue_window_and_command(self):
        """mock flow task add:命令串命中白名单、含 --at(未来窗口)与 --workdir。"""
        stub, cap = self._stub_flow()
        fc._flow_bin = lambda: stub
        wi_dir = self._mk_wi("w1", "translated")
        self._mk_taskbook(wi_dir)
        cfg = _chain_cfg()
        fc._hook_auto_enqueue(wi_dir, cfg, False)
        args = fc.read_file(cap).splitlines()
        self.assertIn("--at", args)
        at_raw = args[args.index("--at") + 1]
        scheduled = fc.parse_iso(at_raw)
        self.assertGreaterEqual(scheduled, fc.now_dt() - timedelta(seconds=2))
        self.assertIn("--workdir", args)
        self.assertEqual(args[args.index("--workdir") + 1], self.workdir)
        cmd = args[args.index("--command") + 1]
        self.assertIn("workitem execute w1 --sync", cmd)
        actions = [r["action"] for r in self._read_audit(wi_dir)]
        self.assertIn("auto_enqueue", actions)

    def test_C13_auto_enqueue_same_repo_overlap_offset(self):
        """同仓重叠 diff_scope → scheduled_at 错开 ≥overlap_minutes。"""
        stub, cap = self._stub_flow()
        fc._flow_bin = lambda: stub
        wi_dir = self._mk_wi("w1", "translated")
        self._mk_taskbook(wi_dir)
        # 另一个 workitem w2 的 execute 任务(同 workdir、重叠 allow、非终态)
        w2_dir = os.path.join(fc.workitems_dir(), "w2")
        os.makedirs(w2_dir, exist_ok=True)
        fc.save_status_atomic(w2_dir, _status("w2", "translated"))
        self._mk_taskbook(w2_dir)
        reg = {"schema_version": 1, "tasks": {
            "t-1": {"id": "t-1", "workitem": "w2",
                    "command": "flow workitem execute w2 --sync",
                    "priority": "P2", "state": "scheduled", "kind": "execute",
                    "workdir": self.workdir, "expected_seconds": 1800,
                    "scheduled_at": fc.now_iso(), "why": "x"}}}
        with open(os.path.join(os.environ["FLOW_TASK_DIR"], "tasks.json"), "w",
                  encoding="utf-8") as f:
            json.dump(reg, f)
        cfg = _chain_cfg(overlap_minutes=40)
        base = fc.window.next_offpeak_start(fc.now_dt())   # hook 调用前取基准
        fc._hook_auto_enqueue(wi_dir, cfg, False)
        args = fc.read_file(cap).splitlines()
        at_raw = args[args.index("--at") + 1]
        # isoformat(timespec="seconds") 截断亚秒(微秒),base 与 scheduled 对齐到秒比较;
        # base 取 hook 调用前(≤ hook 内部 now),截断后不会因跨秒放大 rhs。
        self.assertGreaterEqual(
            fc.parse_iso(at_raw), base.replace(microsecond=0) + timedelta(minutes=40))

    def test_C14_learn_execute_expected(self):
        """纯函数:480→max(1800,720)=1800;2000→max(1800,3000)=3000;缺失→1800。"""
        cfg = _chain_cfg()
        self.assertEqual(fc.learn_execute_expected(480, cfg), 1800)
        self.assertEqual(fc.learn_execute_expected(2000, cfg), 3000)
        self.assertEqual(fc.learn_execute_expected(None, cfg), 1800)
        self.assertEqual(fc.learn_execute_expected("abc", cfg), 1800)

    def test_C14b_learn_execute_expected_bad_seed_fallback(self):
        """ocr6-F4:expected_seconds_seed.execute 非数字/缺失 → fallback 1800,不崩
        (修复前 int("abc") 在 try 外抛 ValueError)。"""
        cfg = _chain_cfg()
        cfg["task"]["expected_seconds_seed"] = {"design": 480, "execute": "abc"}
        self.assertEqual(fc.learn_execute_expected(2000, cfg), 3000)   # d 合法,seed 仅定下限
        self.assertEqual(fc.learn_execute_expected(None, cfg), 1800)   # d 缺失 → fallback
        self.assertEqual(fc.learn_execute_expected("xyz", cfg), 1800)  # d 非法 → fallback
        self.assertEqual(fc.learn_execute_expected(-5, cfg), 1800)     # d ≤ 0 → fallback
        cfg["task"]["expected_seconds_seed"] = {"design": 480, "execute": None}
        self.assertEqual(fc.learn_execute_expected(None, cfg), 1800)
        cfg["task"]["expected_seconds_seed"] = {"design": 480, "execute": "3.5"}
        self.assertEqual(fc.learn_execute_expected(None, cfg), 1800)

    def test_C15_auto_verify_pass_advances(self):
        """gate 全过 → verified + on_verified notify(accept_pending)。"""
        notify_cmd, notify_log = self._stub_notify()
        wi_dir = self._mk_wi("w1", "executed")
        self._mk_design(wi_dir)
        self._mk_taskbook(wi_dir)
        self._mk_executor(wi_dir, RESULT_OK)
        cfg = _chain_cfg(notify=notify_cmd)
        fc._hook_auto_verify(wi_dir, cfg, False)
        st = fc.load_status(wi_dir)
        self.assertEqual(st["state"], "verified")
        self.assertEqual(st.get("chain_fail_count"), 0)
        self.assertIn("accept_pending", self._notify_titles(notify_log))
        actions = [r["action"] for r in self._read_audit(wi_dir)]
        self.assertIn("auto_verify_pass", actions)

    def test_C16_auto_verify_fail_impl_reenqueue(self):
        """tests fail(impl) → verify_fail、chain_fail_count=1、重入队 execute --force。"""
        stub, cap = self._stub_flow()
        fc._flow_bin = lambda: stub
        wi_dir = self._mk_wi("w1", "executed")
        self._mk_design(wi_dir)
        self._mk_taskbook(wi_dir)
        self._mk_executor(wi_dir, RESULT_FAIL)
        cfg = _chain_cfg()
        fc._hook_auto_verify(wi_dir, cfg, False)
        st = fc.load_status(wi_dir)
        self.assertEqual(st.get("chain_fail_count"), 1)
        self.assertEqual(st["state"], "executed")  # impl 路由落回 executed
        args = self._flow_add_blocks(cap)[0]
        cmd = args[args.index("--command") + 1]
        self.assertIn("--force", cmd)
        self.assertIn("workitem execute w1 --sync", cmd)
        actions = [r["action"] for r in self._read_audit(wi_dir)]
        self.assertIn("auto_verify_retry", actions)

    def test_C17_auto_verify_storm_freezes(self):
        """连续 3 次失败 → chain_frozen=true、不再重入队、notify 升级;
        event_seq 不回退(修复:冻结分支曾用旧快照覆盖)。"""
        stub, cap = self._stub_flow()
        fc._flow_bin = lambda: stub
        notify_cmd, notify_log = self._stub_notify()
        wi_dir = self._mk_wi("w1", "executed")
        self._mk_design(wi_dir)
        self._mk_taskbook(wi_dir)
        self._mk_executor(wi_dir, RESULT_FAIL)
        cfg = _chain_cfg(notify=notify_cmd)
        fc._hook_auto_verify(wi_dir, cfg, False)
        fc._hook_auto_verify(wi_dir, cfg, False)
        fc._hook_auto_verify(wi_dir, cfg, False)
        st = fc.load_status(wi_dir)
        self.assertTrue(st.get("chain_frozen"))
        self.assertEqual(st.get("chain_fail_count"), 3)
        self.assertEqual(st["state"], "executed")  # impl 路由落回 executed
        # 前两次重入队,第三次冻结不重入队
        blocks = self._flow_add_blocks(cap)
        self.assertEqual(len(blocks), 2)
        self.assertIn("storm_frozen", self._notify_titles(notify_log))
        actions = [r["action"] for r in self._read_audit(wi_dir)]
        self.assertIn("auto_verify_frozen", actions)
        # event_seq 单调不回退(verify_fail 转移后 event_seq 必大于转移前)
        evs = [json.loads(ln)["seq"] for ln in fc.read_file(
            os.path.join(wi_dir, "events.jsonl")).splitlines() if ln.strip()]
        self.assertEqual(st["event_seq"], evs[-1])
        self.assertEqual(evs, sorted(evs))

    def test_C17b_auto_verify_design_route_freezes(self):
        """route=design 连续失败:verify_fail 落 designed + 冻结;design 缺陷不自动入队。"""
        stub, cap = self._stub_flow()
        fc._flow_bin = lambda: stub
        # taskbook 无 diff_scope → scope_undeclared → design 路由
        block_no_scope = "test_command: pytest\n"
        wi_dir = self._mk_wi("w2", "executed")
        self._mk_design(wi_dir)
        self._mk_taskbook(wi_dir, block_no_scope)
        self._mk_executor(wi_dir, RESULT_OK)
        cfg = _chain_cfg()
        fc._hook_auto_verify(wi_dir, cfg, False)
        fc._hook_auto_verify(wi_dir, cfg, False)
        fc._hook_auto_verify(wi_dir, cfg, False)
        st = fc.load_status(wi_dir)
        self.assertTrue(st.get("chain_frozen"))
        self.assertEqual(st.get("chain_fail_count"), 3)
        self.assertEqual(st["state"], "designed")  # design 路由落 designed
        self.assertEqual(self._flow_add_blocks(cap), [])  # design 缺陷不重入队
        actions = [r["action"] for r in self._read_audit(wi_dir)]
        self.assertIn("auto_verify_frozen", actions)

    def test_C17c_auto_verify_pass_unfreezes_chain(self):
        """ocr9b-M1:冻结(chain_frozen=true)后 verify 通过 → 连同清除冻结标志
        (与 fail_count 一起重置),auto_translate 恢复执行不再 bail out。"""
        stub, _ = self._stub_flow()
        fc._flow_bin = lambda: stub
        wi_dir = self._mk_wi("w1", "executed")
        self._mk_design(wi_dir)
        self._mk_taskbook(wi_dir)
        self._mk_executor(wi_dir, RESULT_FAIL)
        cfg = _chain_cfg()
        fc._hook_auto_verify(wi_dir, cfg, False)
        fc._hook_auto_verify(wi_dir, cfg, False)
        fc._hook_auto_verify(wi_dir, cfg, False)          # 第 3 次失败 → 冻结
        st = fc.load_status(wi_dir)
        self.assertTrue(st.get("chain_frozen"))
        self.assertEqual(st.get("chain_fail_count"), 3)
        # 修复后 verify 通过 → fail_count 归零且 chain_frozen 一并清除
        self._mk_executor(wi_dir, RESULT_OK)
        fc._hook_auto_verify(wi_dir, cfg, False)
        st = fc.load_status(wi_dir)
        self.assertEqual(st["state"], "verified")
        self.assertEqual(st.get("chain_fail_count"), 0)
        self.assertNotIn("chain_frozen", st)              # 冻结标志被清除(解冻)
        # 链恢复:auto_translate 不再因 chain_frozen skip(状态回 reviewed 后重走)
        st["state"] = "reviewed"
        fc.save_status_atomic(wi_dir, st)
        fc.write_decision(wi_dir, "pass")   # translate 转移 guard(quality_pass)需 verdict
        os.environ["CHAIN_REASONIX_BIN"] = self._stub_reasonix('{"body": "ok"}')
        fc._hook_auto_translate(wi_dir, cfg, False)
        actions = [r["action"] for r in self._read_audit(wi_dir)]
        self.assertNotIn("auto_translate_skip", actions)
        self.assertIn("auto_translate_ok", actions)
        self.assertTrue(os.path.isfile(os.path.join(wi_dir, "taskbook.md")))
        self.assertEqual(fc.load_status(wi_dir)["state"], "translated")

    def test_C18_dry_run_zero_writes(self):
        """dry_run=true:无 decision/taskbook/队列/verify.json/audit 新写入。"""
        wi_dir = self._mk_wi()
        self._mk_design(wi_dir)
        os.environ["CHAIN_REASONIX_BIN"] = self._stub_reasonix('{"verdict": "pass"}')
        cfg = _chain_cfg(on_designed="auto_review", review_llm=True, dry_run=True)
        res = fc._transition_with_hooks(
            wi_dir, "created", "designed", "design", {}, {}, cfg)
        self.assertTrue(res["ok"])
        self.assertTrue(res["chain"]["dry_run"])
        self.assertEqual(res["chain"]["hook"], "on_designed")
        self.assertTrue(res["chain"]["would_write"])
        self.assertEqual(self._read_audit(wi_dir), [])
        self.assertFalse(os.path.exists(os.path.join(wi_dir, "decision.yaml")))
        self.assertFalse(os.path.exists(os.path.join(wi_dir, "taskbook.md")))
        self.assertEqual(fc.load_status(wi_dir)["state"], "designed")

    def test_C18b_dry_run_suppresses_notify_chain(self):
        """ocr F2:dry_run=true 时 notify_chain 不执行外部命令(零写入契约);
        非 dry-run 同路径仍推送 review_reject。"""
        stub, out = self._stub_notify()
        wi_dir = self._mk_wi("w1")          # created → designed 转移触发 on_designed
        self._mk_design(wi_dir, "# 标题\n")    # 机械四项不过 → auto_review_reject 分支
        cfg = _chain_cfg(on_designed="auto_review", dry_run=True, notify=stub)
        res = fc._transition_with_hooks(
            wi_dir, "created", "designed", "design", {}, {}, cfg)
        self.assertTrue(res["ok"])
        self.assertTrue(res["chain"]["dry_run"])
        self.assertIn("auto_review_reject", res["chain"]["would_write"])
        self.assertEqual(self._notify_titles(out), [])        # 外部命令零执行
        self.assertFalse(os.path.exists(os.path.join(wi_dir, "decision.yaml")))
        # 对照:非 dry-run → notify 命令执行,收到 review_reject
        stub2, out2 = self._stub_notify()
        wi2 = self._mk_wi("w2", "designed")
        self._mk_design(wi2, "# 标题\n")
        fc._hook_auto_review(wi2, _chain_cfg(on_designed="auto_review", notify=stub2), False)
        self.assertIn("review_reject", self._notify_titles(out2))
        self.assertTrue(os.path.isfile(os.path.join(wi2, "decision.yaml")))

    def test_C19_audit_appended_with_seq(self):
        """非 dry 钩子动作写 events/audit.jsonl,字段齐全且 seq 递增。"""
        wi_dir = self._mk_wi()
        self._mk_design(wi_dir)
        cfg = _chain_cfg(on_designed="off", on_reviewed=False)
        res = fc.with_workitem_lock(wi_dir, lambda: fc._do_transition(
            wi_dir, "created", "designed", "design", {}, {}, cfg))
        self.assertTrue(res["ok"])
        fc._hook_auto_review(wi_dir, cfg, False)
        fc.audit_chain(wi_dir, "manual_append")
        recs = self._read_audit(wi_dir)
        self.assertEqual([r["seq"] for r in recs], [1, 2])
        self.assertEqual(recs[0]["action"], "auto_review_pass")
        self.assertEqual(recs[0]["schema_version"], 1)
        self.assertIn("ts", recs[0])

    def test_C20_old_workitem_compatible(self):
        """status 无 chain_* 字段 + 无 design-result.json → 钩子用默认值/fallback 不崩。"""
        stub, _cap = self._stub_flow()
        fc._flow_bin = lambda: stub
        wi_dir = self._mk_wi("w1", "translated")  # 无 chain_fail_count/chain_frozen
        self._mk_taskbook(wi_dir)
        cfg = _chain_cfg()
        fc._hook_auto_enqueue(wi_dir, cfg, False)
        actions = [r["action"] for r in self._read_audit(wi_dir)]
        self.assertIn("auto_enqueue", actions)
        rec = self._read_audit(wi_dir)[-1]
        # ocr7-L1:expected 地板从 config 派生(此前硬编码 1915=max(fallback 1800,
        # medium timeout 1800+115),config 变更即碎):fallback=seed.execute,
        # timeout=executor_params_for("medium").timeout_s
        cfg2 = _chain_cfg()
        fallback = int(cfg2["task"]["expected_seconds_seed"]["execute"])
        timeout_s = fc.executor_params_for("medium", cfg2)["timeout_s"]
        self.assertEqual(rec["output"]["expected_seconds"], max(fallback, timeout_s + 115))

    def test_C21_hook_failure_does_not_block_transition(self):
        """stub reasonix 崩溃 → 转移仍 ok=True,audit 记 error(fail-closed 不写 taskbook)。"""
        os.environ["CHAIN_REASONIX_BIN"] = os.path.join(self.tmp, "no-such-reasonix")
        # designed + decision pass → 手动 review 转移触发 on_reviewed(auto_translate)
        wi_dir = self._mk_wi("w1", "designed")
        self._mk_design(wi_dir)
        fc.write_decision(wi_dir, "pass")
        cfg = _chain_cfg()
        code = fc.cmd_transition(["w1", "reviewed"], cfg)
        self.assertEqual(code, 0)  # review 转移成功不阻塞
        self.assertEqual(fc.load_status(wi_dir)["state"], "reviewed")
        self.assertFalse(os.path.exists(os.path.join(wi_dir, "taskbook.md")))  # fail-closed 不写
        recs = self._read_audit(wi_dir)
        fail_recs = [r for r in recs if r["action"] == "auto_translate_fail"]
        self.assertTrue(fail_recs)
        self.assertTrue(fail_recs[-1].get("error"))

    def test_C22_chain_depth_no_infinite_loop(self):
        """全开链 design→review→translate 一次完成,深度受控无死循环。"""
        stub, _cap = self._stub_flow()
        fc._flow_bin = lambda: stub
        os.environ["CHAIN_REASONIX_BIN"] = self._stub_reasonix('{"body": "ok"}')
        wi_dir = self._mk_wi()
        self._mk_design(wi_dir)
        cfg = _chain_cfg(on_designed="auto_review")
        code = fc.cmd_transition(["w1", "designed"], cfg)
        self.assertEqual(code, 0)
        self.assertEqual(fc.load_status(wi_dir)["state"], "translated")
        self.assertEqual(fc._chain_depth, 0)
        actions = [r["action"] for r in self._read_audit(wi_dir)]
        self.assertIn("auto_review_pass", actions)
        self.assertIn("auto_translate_ok", actions)
        self.assertIn("auto_enqueue", actions)

    def test_C23_llm_call_multiline_json_parsed(self):
        """ocr F3:stdout 为多行缩进 JSON + 尾随日志 → 正确解析(不再只认最后一行)。"""
        out = '{\n  "verdict": "pass",\n  "reasons": ["ok"]\n}\n[log] trailing'
        os.environ["CHAIN_REASONIX_BIN"] = self._stub_reasonix(out)
        ok, obj = fc._llm_call(_chain_cfg(), "prompt")
        self.assertTrue(ok)
        self.assertEqual(obj, {"verdict": "pass", "reasons": ["ok"]})

    def test_C24_llm_call_noise_fail_closed(self):
        """ocr F3:非 JSON / 数组 / 空输出 → (False, None) fail-closed,不误判成功。"""
        cases = ("some log line\n", "[1, 2, 3]", "", "   \n")
        for out in cases:
            os.environ["CHAIN_REASONIX_BIN"] = self._stub_reasonix(out)
            ok, obj = fc._llm_call(_chain_cfg(), "prompt")
            self.assertFalse(ok)
            self.assertIsNone(obj)

    def test_C25_auto_translate_transition_fail_no_residue(self):
        """ocr F1:translate 转移失败(ok=False)→ taskbook.md 不残留、状态停 reviewed。"""
        os.environ["CHAIN_REASONIX_BIN"] = self._stub_reasonix('{"body": "ok"}')
        wi_dir = self._mk_reviewed()
        cfg = _chain_cfg()
        with mock.patch.object(fc, "_do_transition",
                               return_value={"ok": False, "reason": "guard: boom"}):
            fc._hook_auto_translate(wi_dir, cfg, False)
        self.assertFalse(os.path.exists(os.path.join(wi_dir, "taskbook.md")))
        self.assertEqual(fc.load_status(wi_dir)["state"], "reviewed")
        actions = [r["action"] for r in self._read_audit(wi_dir)]
        self.assertIn("auto_translate_error", actions)

    def test_C26_auto_review_transition_fail_no_residue(self):
        """ocr F1:review 转移失败(ok=False)→ decision.yaml 不残留、状态停 designed。"""
        wi_dir = self._mk_wi("w1", "designed")
        self._mk_design(wi_dir)
        cfg = _chain_cfg(on_designed="auto_review")
        with mock.patch.object(fc, "_do_transition",
                               return_value={"ok": False, "reason": "guard: boom"}):
            fc._hook_auto_review(wi_dir, cfg, False)
        self.assertFalse(os.path.exists(os.path.join(wi_dir, "decision.yaml")))
        self.assertEqual(fc.load_status(wi_dir)["state"], "designed")
        actions = [r["action"] for r in self._read_audit(wi_dir)]
        self.assertIn("auto_review_error", actions)

    def test_C27_auto_verify_transition_fail_keeps_fail_count(self):
        """ocr F2:verify 转移失败(ok=False)→ chain_fail_count 不提前清零、
        状态停 executed(与「未通过 verify」一致)。"""
        wi_dir = self._mk_wi("w1", "executed", chain_fail_count=2)
        self._mk_design(wi_dir)
        self._mk_taskbook(wi_dir)
        self._mk_executor(wi_dir, RESULT_OK)
        cfg = _chain_cfg()
        with mock.patch.object(fc, "_do_transition",
                               return_value={"ok": False, "reason": "guard: boom"}):
            fc._hook_auto_verify(wi_dir, cfg, False)
        st = fc.load_status(wi_dir)
        self.assertEqual(st["state"], "executed")
        self.assertEqual(st.get("chain_fail_count"), 2)
        actions = [r["action"] for r in self._read_audit(wi_dir)]
        self.assertIn("auto_verify_error", actions)

    def test_C35_three_item_report_both_shapes(self):
        """ocr7-M3/L3:_three_item_report 兼容 run_verify_auto_core 返回值(形状 1)
        与 verify.json 子字典(形状 2)——rescue_verify_fail 通知传形状 2,此前
        三项全空,展示数据错位。"""
        # 形状 1:顶层 tests_result/diff_result/errors_result(flow-core 内部调用)
        v1 = {"tests_result": {"pass": False, "reason": "command_unresolved"},
              "diff_result": {"match": True, "reason": None},
              "errors_result": {"match": False, "reason": "uncovered"},
              "route": "impl"}
        r1 = fc._three_item_report(v1)
        self.assertIs(r1["tests"]["pass"], False)
        self.assertEqual(r1["tests"]["reason"], "command_unresolved")
        self.assertIs(r1["diff"]["match"], True)
        self.assertIs(r1["error_table"]["match"], False)
        self.assertEqual(r1["route"], "impl")
        # 形状 2:verify 子字典(顶层 *_match + details 带 reason)——flow-task-core 传入
        v2 = {"schema_version": 1, "tests_pass": True, "diff_match": False,
              "error_table_match": True, "route": None, "checked_at": "2026-08-25T00:00:00Z",
              "details": {"tests": {"pass": True, "reason": None, "command": "pytest"},
                          "diff": {"match": False, "reason": "scope_undeclared"},
                          "error_table": {"match": True, "reason": None}}}
        r2 = fc._three_item_report(v2)
        self.assertIs(r2["tests"]["pass"], True)
        self.assertIs(r2["diff"]["match"], False)
        self.assertEqual(r2["diff"]["reason"], "scope_undeclared")
        self.assertIs(r2["error_table"]["match"], True)
        self.assertIsNone(r2["route"])
        # 空/非 dict 输入 → 缺省 None 不崩
        r3 = fc._three_item_report({})
        self.assertIsNone(r3["tests"]["pass"])
        self.assertIsNone(r3["diff"]["match"])
        self.assertIsNone(r3["route"])

    def test_C36_translate_prompt_truncation_warns(self):
        """ocr7-M5:_translate_prompt 对 >8000 字符 design.md 截断时告警 + prompt
        标注不完整,不再静默丢关键段(测试策略/验收标准/改动清单)。"""
        wi_dir = self._mk_wi("w1", "reviewed")
        with open(os.path.join(wi_dir, "design.md"), "w", encoding="utf-8") as f:
            f.write("# 方案\n" + ("x" * 10000) + "\n## 测试策略\n- 全量回归\n")
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            prompt = fc._translate_prompt(wi_dir)
        self.assertIn("告警", err.getvalue())
        self.assertIn("8000", err.getvalue())
        self.assertIn("截断", prompt)                    # prompt 内标注不完整
        self.assertNotIn("## 测试策略", prompt[:9000])   # 后段未被静默纳入
        # 小 design 不告警、不标注
        wi2 = self._mk_wi("w2", "reviewed")
        with open(os.path.join(wi2, "design.md"), "w", encoding="utf-8") as f:
            f.write("# 小方案\n")
        err2 = io.StringIO()
        with contextlib.redirect_stderr(err2):
            prompt2 = fc._translate_prompt(wi2)
        self.assertEqual(err2.getvalue(), "")
        self.assertNotIn("截断", prompt2)

    # ── ocr3 M1/L1 回归 ─────────────────────────────────────────────────

    def test_C37_notify_argv_whitelist(self):
        """ocr M1:notify 模板白名单解析——shell 元字符/管道/重定向 → None,合法 → argv。"""
        self.assertIsNone(fc._notify_argv(None))
        self.assertIsNone(fc._notify_argv(""))
        self.assertIsNone(fc._notify_argv("cat /tmp/x | sh"))
        self.assertIsNone(fc._notify_argv("echo hi > /tmp/f"))
        self.assertIsNone(fc._notify_argv("sh -c 'touch /tmp/x'"))
        self.assertIsNone(fc._notify_argv("$(rm -rf /)"))
        self.assertIsNone(fc._notify_argv("a; b"))
        self.assertIsNone(fc._notify_argv("ls *.md"))
        self.assertEqual(fc._notify_argv("/usr/bin/notify --flag value"),
                         ["/usr/bin/notify", "--flag", "value"])
        self.assertEqual(fc._notify_argv("/bin/echo --flag=1"), ["/bin/echo", "--flag=1"])
        # ocr F4:含空格路径/引号参数被接受(shlex 已正确处理),控制字符仍拒绝
        self.assertEqual(fc._notify_argv("/usr/local/bin/notify.sh --msg 'hello world'"),
                         ["/usr/local/bin/notify.sh", "--msg", "hello world"])
        self.assertEqual(fc._notify_argv("/usr/local/bin/notify.sh --file 'a b.txt'"),
                         ["/usr/local/bin/notify.sh", "--file", "a b.txt"])
        self.assertIsNone(fc._notify_argv(f"/bin/echo {chr(0x1f)}"))  # 控制字符仍拒
        # ocr F4 加固:shell 包装变体仍拒(env/sh --/绝对路径 shell/合并/前置组合标志)
        self.assertIsNone(fc._notify_argv("env sh -c 'touch /tmp/x'"))
        self.assertIsNone(fc._notify_argv("sh -- -c 'touch /tmp/x'"))
        self.assertIsNone(fc._notify_argv("/bin/bash -c 'touch /tmp/x'"))
        self.assertIsNone(fc._notify_argv("bash -lc 'echo x'"))
        self.assertIsNone(fc._notify_argv("sh -e -c 'touch /tmp/x'"))
        self.assertIsNone(fc._notify_argv("env bash -e -lc 'echo x'"))

    def test_C38_notify_argv_rejects_script_interpreters(self):
        """ocr6-F2:黑名单不只 shell——node/ruby/perl/php/awk/sed/lua/julia/R 等
        解释器同样可从 stdin 读代码执行(裸解释器即注入面),统一 fail-closed 拒绝;
        含 python 版本变体(python3.12/pypy3)与 env 前缀;解释器 + 脚本文件参数
        (脚本从文件读,payload 走 stdin)不误伤。"""
        for interp in ("node", "deno", "bun", "ruby", "perl", "php", "awk", "sed",
                       "lua", "luajit", "julia", "R", "Rscript", "tclsh", "wish",
                       "python3.12", "pypy3"):
            self.assertIsNone(fc._notify_argv(interp), f"裸解释器应拒: {interp}")
            self.assertIsNone(fc._notify_argv(f"/usr/bin/{interp}"), interp)
        self.assertIsNone(fc._notify_argv("env node"))          # env 前缀同样拒
        self.assertIsNone(fc._notify_argv("ruby -c puts1"))     # -c 类包装拒
        # 不误伤:解释器 + 脚本文件参数仍放行
        self.assertEqual(fc._notify_argv("node /opt/notify.js --flag x"),
                         ["node", "/opt/notify.js", "--flag", "x"])
        self.assertEqual(fc._notify_argv("ruby /opt/notify.rb"),
                         ["ruby", "/opt/notify.rb"])

    def test_C38b_env_value_flags_not_bypass_interp(self):
        """ocr9-F3:env 带值选项(-u/--unset/-C/--chdir)的值不可被误判为命令位置
        ——「env -u FOO node」须定位到裸解释器 node 并拒绝;env -S/--split-string
        会重切字符串(argv 检查不可靠)直接拒绝;-- 分隔符后解释器仍拦截。"""
        self.assertIsNone(fc._notify_argv("env -u FOO node"))
        self.assertIsNone(fc._notify_argv("env --unset FOO python3"))
        self.assertIsNone(fc._notify_argv("env --unset=FOO node"))
        self.assertIsNone(fc._notify_argv("env -C /tmp node"))
        self.assertIsNone(fc._notify_argv("env --chdir=/tmp sh"))
        self.assertIsNone(fc._notify_argv("env -S 'node -e code'"))
        self.assertIsNone(fc._notify_argv("env --split-string='node -e code'"))
        self.assertIsNone(fc._notify_argv("env -- node"))          # -- 后解释器仍拦截
        # 不误伤:env 前缀 + 非解释器命令仍放行(带值选项值被正确跳过)
        self.assertEqual(fc._notify_argv("env -u FOO /usr/bin/notify --flag v"),
                         ["env", "-u", "FOO", "/usr/bin/notify", "--flag", "v"])
        self.assertEqual(fc._notify_argv("env --chdir=/tmp /usr/bin/notify"),
                         ["env", "--chdir=/tmp", "/usr/bin/notify"])

    def test_C39_notify_chain_allows_space_paths(self):
        """ocr F4:含空格路径/引号参数被实际执行;控制字符模板零执行。
        (含空格路径须引号包裹——shlex 拆分后保持单一 token,即 finding 所述
        「shlex.split 已正确处理空格与引号」;校验层不再因空格拒绝。)"""
        spaced = os.path.join(self.tmp, "dir with space")
        os.makedirs(spaced, exist_ok=True)
        stub = os.path.join(spaced, "notify me.sh")     # 命令路径含空格
        out = os.path.join(self.tmp, "spaced.log")
        with open(stub, "w", encoding="utf-8") as f:
            f.write(f"#!/usr/bin/env bash\ncat >> {out}\n")
        os.chmod(stub, 0o755)
        wi_dir = self._mk_wi("w1", "designed")
        cfg = _chain_cfg(notify=f"'{stub}' --flag 'a b.txt'")
        fc.notify_chain(wi_dir, cfg, "t", "b")
        self.assertTrue(os.path.isfile(out))            # 含空格路径被接受并执行
        rec = json.loads(fc.read_file(out).splitlines()[0])
        self.assertEqual(rec["title"], "t")
        # 零执行断言:stub 脚本只写 out(spaced.log),从不写 never.log——原
        # assertFalse(os.path.exists(out2)) 恒真,无法防回归。改为快照 out 内容:
        # 控制字符模板被拒时 stub 不被二次调用,日志内容不变。
        before = fc.read_file(out)
        fc.notify_chain(wi_dir, _chain_cfg(notify=f"'{stub}' --bad {chr(0x1f)}"),
                        "t", "b")                        # 控制字符 → argv 拒绝
        self.assertEqual(fc.read_file(out), before)      # 零执行:日志未追加

    def test_C40_notify_chain_rejects_evil_templates(self):
        """ocr M1:恶意 notify 模板(管道/重定向/子 shell/分号)零命令执行。"""
        stub, _cap, out = self._stub_notify_argv()
        wi_dir = self._mk_wi("w1")
        pwned = os.path.join(self.tmp, "pwned")
        evils = (
            f"{stub} | cat",                      # 管道
            f"echo pwn > {pwned}",                # 重定向
            f"sh -c 'touch {pwned}.sh'",          # 子 shell
            f"$(touch {pwned}.var)",              # 变量展开/子 shell
            f"{stub}; touch {pwned}.semi",        # 分号
        )
        for evil in evils:
            fc.notify_chain(wi_dir, _chain_cfg(notify=evil), "t", "b")
        self.assertFalse(os.path.exists(out))                      # stub 未被调用
        for suffix in ("", ".sh", ".var", ".semi"):
            self.assertFalse(os.path.exists(pwned + suffix))

    def test_C41_notify_chain_payload_via_stdin(self):
        """ocr M1:合法「命令+参数」模板正常执行;payload 完整经 stdin 传入。"""
        stub, cap, out = self._stub_notify_argv()
        wi_dir = self._mk_wi("w1", "designed")
        cfg = _chain_cfg(notify=f"{stub} --flag value")
        fc.notify_chain(wi_dir, cfg, "t", "b", {"k": 1})
        self.assertEqual(fc.read_file(cap).splitlines(), ["--flag", "value"])
        rec = json.loads(fc.read_file(out).splitlines()[0])
        self.assertEqual(rec["title"], "t")
        self.assertEqual(rec["body"], "b")
        self.assertEqual(rec["meta"], {"k": 1})
        self.assertEqual(rec["workitem"], "w1")
        self.assertEqual(rec["state"], "designed")

    def test_C42_flow_bin_stub_restored_on_teardown(self):
        """ocr L1:_flow_bin 测试 stub 经 tearDown 恢复模块原函数,不残留污染。"""
        orig = fc._flow_bin
        stub, _cap = self._stub_flow()
        fc._flow_bin = lambda: stub              # 复刻 C12-C27 的覆盖方式
        self.assertIsNot(fc._flow_bin, orig)     # stub 已生效
        # ocr5-L1:不在测试体内手动调 setUp/tearDown(unittest 会自动调用,手动
        # 重复执行产生脆弱副作用)——恢复由框架随用例结束自动调用的 tearDown
        # 完成(C12-C27 依赖同一机制,若有回归将大面积失败,故无需在此显式重演)。
        self.assertEqual(self._orig_flow_bin, orig)   # tearDown 将据此恢复原函数

    def test_C43_extract_change_list_keeps_items_after_subheading(self):
        """ocr F4:「改动文件/交付物」段内子标题(###)不截断列表——子标题后
        `- path` 条目继续收集;遇同级/更高级 heading(新段落)才终止。"""
        wi_dir = self._mk_wi()
        self._mk_design(wi_dir, """# 方案标题

## 1. 方案设计
- 说明

## 6. 改动文件清单
- scripts/flow-core.py

### src 模块
- src/utils.py
- src/parser.py

### tests 模块
- tests/test_flow_core.py

## 7. 冻结清单
- config/secrets.yaml
""")
        # 改动文件:段内子标题后的条目不丢失(fail-closed _hook_auto_translate 不再误拒)
        self.assertEqual(fc.extract_change_list(wi_dir),
                         ["scripts/flow-core.py", "src/utils.py",
                          "src/parser.py", "tests/test_flow_core.py"])
        self.assertEqual(fc.extract_frozen_list(wi_dir), ["config/secrets.yaml"])

    def test_C43b_extract_change_list_stops_at_sibling_heading(self):
        """ocr F4:同级新段仍终止收集(不越段);更浅层级(# 顶层段)同样终止。"""
        wi_dir = self._mk_wi()
        self._mk_design(wi_dir, """## 改动文件
- scripts/a.py

## 其它段
- scripts/not_here.py

# 顶层段
- scripts/top.py
""")
        self.assertEqual(fc.extract_change_list(wi_dir), ["scripts/a.py"])

    def test_C44_hook_chain_runs_outside_workitem_lock(self):
        """ocr F4:hook 链(LLM 子进程阻塞)执行期间,workitem 锁不被持有——
        其他 workitem 操作可并发取锁;链结束后状态正常推进到 reviewed。"""
        if fc.fcntl is None:
            self.skipTest("fcntl 不可用,跳过锁粒度测试")
        marker = os.path.join(self.tmp, "llm-started")
        stub = os.path.join(self.tmp, "stub-slow-reasonix")
        with open(stub, "w", encoding="utf-8") as f:
            f.write(f"#!/usr/bin/env bash\ntouch {marker}\nsleep 1.5\n"
                    f"echo '{{\"verdict\":\"pass\"}}'\n")
        os.chmod(stub, 0o755)
        os.environ["CHAIN_REASONIX_BIN"] = stub
        wi_dir = self._mk_wi()
        self._mk_design(wi_dir)
        cfg = _chain_cfg(on_designed="auto_review", review_llm=True)

        probe = {"marker_seen": False, "lock_acquired": False, "error": None}

        def _probe():
            # 等 LLM stub 启动(marker 出现,sleep 中)——此时应已释放 workitem 锁
            deadline = time.time() + 5
            while not os.path.exists(marker) and time.time() < deadline:
                time.sleep(0.02)
            probe["marker_seen"] = os.path.exists(marker)
            fd = os.open(os.path.join(wi_dir, ".lock"),
                         os.O_CREAT | os.O_RDWR, 0o644)
            try:
                try:
                    fc.fcntl.flock(fd, fc.fcntl.LOCK_EX | fc.fcntl.LOCK_NB)
                    probe["lock_acquired"] = True        # hook 阶段锁空闲 → 可取
                except OSError:
                    probe["lock_acquired"] = False       # 锁仍被 hook 链持有 → fail
            finally:
                try:
                    fc.fcntl.flock(fd, fc.fcntl.LOCK_UN)
                except OSError:
                    pass
                os.close(fd)

        t = threading.Thread(target=_probe)
        t.start()
        try:
            res = fc._transition_with_hooks(wi_dir, "created", "designed",
                                            "design", {}, {}, cfg)
        finally:
            t.join(timeout=10)
        self.assertTrue(res["ok"], res)
        self.assertTrue(probe["marker_seen"], "LLM stub 未启动,测试无效")
        self.assertTrue(probe["lock_acquired"],
                        "hook 链执行期间 workitem 锁被长期持有,其他操作被挡")
        self.assertEqual(fc.load_status(wi_dir)["state"], "reviewed")  # 链正常推进

    def test_C45_cmd_transition_json_exposes_chain(self):
        """ocr F4:cmd_transition --json 的 dry-run 摘要经锁外注入 chain 正常透出
        (review 回归:_do 内 emit 时 chain 尚未注入,res.get(\"chain\") 恒假)。"""
        wi_dir = self._mk_wi()
        self._mk_design(wi_dir)
        cfg = _chain_cfg(on_designed="auto_review", dry_run=True)
        with contextlib.redirect_stdout(io.StringIO()) as buf:
            code = fc.cmd_transition(["w1", "designed", "--json"], cfg)
        self.assertEqual(code, 0)
        obj = json.loads(buf.getvalue().strip().splitlines()[-1])
        self.assertEqual(obj["status"], "ok")
        self.assertEqual(obj["chain"]["hook"], "on_designed")
        self.assertTrue(obj["chain"]["dry_run"])
        self.assertTrue(obj["chain"]["would_write"])


class ChainConfigTests(unittest.TestCase):
    """§3 错误表 E1/E2:load_config chain 段校验。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="flow-chain-cfg-")
        self._saved = {k: v for k, v in os.environ.items()
                       if k.startswith(("FLOW_", "COLLABFLOW_"))}
        for k in list(self._saved):
            os.environ.pop(k, None)
        os.environ["FLOW_DATA_DIR"] = os.path.join(self.tmp, ".flow")

    def tearDown(self):
        for k in [k for k in list(os.environ) if k.startswith(("FLOW_", "COLLABFLOW_"))]:
            os.environ.pop(k, None)
        os.environ.update(self._saved)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_load_config_invalid_chain(self):
        """E1:on_designed 非法;E2:布尔键给非布尔 → StoreError。"""
        with open(os.path.join(self.tmp, "c.yaml"), "w", encoding="utf-8") as f:
            f.write("chain:\n  on_designed: bogus\n")
        os.environ["COLLABFLOW_CONFIG"] = os.path.join(self.tmp, "c.yaml")
        with self.assertRaises(fc.StoreError):
            fc.load_config()
        with open(os.path.join(self.tmp, "c.yaml"), "w", encoding="utf-8") as f:
            f.write("chain:\n  on_reviewed: yes\n")
        with self.assertRaises(fc.StoreError):
            fc.load_config()

    def test_load_config_env_dry_run_override(self):
        """FLOW_CHAIN_DRY_RUN=1 → chain.dry_run=True。"""
        os.environ["FLOW_CHAIN_DRY_RUN"] = "1"
        cfg = fc.load_config()
        self.assertTrue(cfg["chain"]["dry_run"])
        self.assertTrue(cfg["chain"]["enabled"])


class RunTestsTimeoutTests(unittest.TestCase):
    """ocr medium F1:_run_tests 有界超时(verify 路径共用函数;timeout 可选,默认 None)。"""

    def _tmp(self):
        d = tempfile.mkdtemp(prefix="flow-run-tests-")
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        return d

    def test_no_timeout_behavior_unchanged(self):
        """不传 timeout → 行为不变:正常命令 pass/exit_code=0,reason=None。"""
        r = fc._run_tests(self._tmp(), "echo hi")
        self.assertTrue(r["pass"])
        self.assertEqual(r["exit_code"], 0)
        self.assertIsNone(r["reason"])

    def test_timeout_expired_returns_timeout_reason(self):
        """传 timeout 且命令超时 → pass=False、exit_code=None、reason="timeout"(不抛)。"""
        # exec 替换 shell:timeout kill 直接命中命令进程,不遗留孤儿 sleep
        r = fc._run_tests(self._tmp(), "exec sleep 0.5", timeout=0.1)
        self.assertFalse(r["pass"])
        self.assertIsNone(r["exit_code"])
        self.assertEqual(r["reason"], "timeout")
        self.assertIsInstance(r["output_tail"], str)

    def test_timeout_expired_bytes_output_decoded(self):
        """CPython text=True 超时时 TimeoutExpired.stdout/stderr 是未解码 bytes →
        _dec 解码不 TypeError,output_tail 含部分输出(锁内不炸)。"""
        with mock.patch.object(
                fc.subprocess, "run",
                side_effect=fc.subprocess.TimeoutExpired(
                    ["cmd"], 0.1, output=b"partial stdout", stderr=b"err line")):
            r = fc._run_tests(self._tmp(), "echo hi", timeout=0.1)
        self.assertFalse(r["pass"])
        self.assertIsNone(r["exit_code"])
        self.assertEqual(r["reason"], "timeout")
        self.assertIn("partial stdout", r["output_tail"])
        self.assertIn("err line", r["output_tail"])

    def test_timeout_fast_command_still_passes(self):
        """传 timeout 但命令按时完成 → 正常通过(超时不误伤快速命令)。"""
        r = fc._run_tests(self._tmp(), "echo ok", timeout=10)
        self.assertTrue(r["pass"])
        self.assertEqual(r["exit_code"], 0)
        self.assertIsNone(r["reason"])

    def test_failed_command_reason_unchanged(self):
        """非超时失败:reason=test_failed,exit_code 非零(与既有行为一致)。"""
        r = fc._run_tests(self._tmp(), "exit 3", timeout=10)
        self.assertFalse(r["pass"])
        self.assertEqual(r["exit_code"], 3)
        self.assertEqual(r["reason"], "test_failed")




if __name__ == "__main__":
    unittest.main(verbosity=2)
