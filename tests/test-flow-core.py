#!/usr/bin/env python3
"""flow-core.py 单元测试(P2):状态机 T1-T24 + store S1-S9。

用法: python3 -m unittest discover tests
零 API、全程临时目录。单测直接 import 纯函数(transition/evaluate_guard 零 I/O)。
"""

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest

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


def _status(wi_id="w1", state="created"):
    return {
        "schema_version": 1, "id": wi_id, "state": state, "iteration": 0,
        "same_defect_count": 0, "primary_defect_type": None, "takeover": False,
        "re_execute_count": 0, "process_version": "1.0.0",
        "created_at": "2026-08-14T10:00:00+00:00",
        "updated_at": "2026-08-14T10:00:00+00:00",
        "event_seq": 1, "locked_by": None, "lock_expires_at": None,
    }


def _event(ev, **meta):
    return {"ts": "2026-08-14T10:00:00+00:00", "event": ev, "from": None, "to": None,
            "actor": "flow:control", "guard": None, "reason": None, "meta": meta}


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


if __name__ == "__main__":
    unittest.main(verbosity=2)
