#!/usr/bin/env python3
# noqa: N999
# 注:文件名连字符为项目惯例(与 test-flow-core.py 等旧文件一致)
"""flow-task-core.py W-B 单测:任务终态钩子 + 超时收口(stale gate)
(design .flow/workitems/task-terminal-hooks/design.md §2 A-G 矩阵)。

用法: python3 -m unittest discover tests
零 API、全程 FLOW_TASK_DIR/FLOW_DATA_DIR/FLOW_STALE_SCAN_ROOT 临时目录隔离;
_fc(flow-core)钩子以 unittest.mock.patch 打桩;纯函数直接单测,动作层以临时目录 + mock 隔离。
"""

import importlib.util
import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

_HERE = os.path.dirname(os.path.abspath(__file__))
_FLOW_CORE = os.path.join(_HERE, "..", "scripts", "flow-core.py")
_TASK_CORE = os.path.join(_HERE, "..", "scripts", "flow-task-core.py")

_fc_spec = importlib.util.spec_from_file_location("flow_core_wb", _FLOW_CORE)
fc = importlib.util.module_from_spec(_fc_spec)
_fc_spec.loader.exec_module(fc)

_tc_spec = importlib.util.spec_from_file_location("flow_task_core_wb", _TASK_CORE)
tc = importlib.util.module_from_spec(_tc_spec)
_tc_spec.loader.exec_module(tc)

_NOW = datetime(2026, 8, 24, 12, 0, 0, tzinfo=timezone.utc)


def _utc(dt):
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def _ago(hours):
    return _utc(_NOW - timedelta(hours=hours))


class WbIsoBase(unittest.TestCase):
    """FLOW_TASK_DIR/FLOW_DATA_DIR/FLOW_STALE_SCAN_ROOT 临时目录隔离。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="flow-wb-")
        self.task_dir = os.path.join(self.tmp, "task")
        self.data_dir = os.path.join(self.tmp, "data")
        self.scan_root = os.path.join(self.tmp, "scan")
        for d in (self.task_dir, self.data_dir, self.scan_root):
            os.makedirs(d, exist_ok=True)
        os.environ["FLOW_TASK_DIR"] = self.task_dir
        os.environ["FLOW_DATA_DIR"] = self.data_dir
        os.environ["FLOW_STALE_SCAN_ROOT"] = self.scan_root

    def tearDown(self):
        for k in ("FLOW_TASK_DIR", "FLOW_DATA_DIR", "FLOW_STALE_SCAN_ROOT"):
            os.environ.pop(k, None)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _cfg(self, **chain_kw):
        chain = {"enabled": True}
        chain.update(chain_kw)
        return {"task": {"seed_history_len": 8}, "host": {"notify": None}, "chain": chain}

    def _full_cfg(self):
        return {
            "workitem": {"plane_id": "control", "lock_ttl_s": 3600, "id_max_len": 64},
            "gates": {"takeover_after_same_defect": 2, "iteration_limit": 3},
            "executor": {"default": "reasonix", "timeout_s": 1800, "diff_scope": None},
            "task": {"expected_seconds_seed": {"design": 480, "execute": 1800}},
            "chain": {"enabled": True},
        }

    def _make_wi(self, wi_id, state, events=None, scan_root=None):
        """在扫描根下构造 workitem 目录(status.yaml + 可选 events.jsonl)。"""
        root = scan_root or self.scan_root
        wi_dir = os.path.join(root, wi_id)
        os.makedirs(wi_dir, exist_ok=True)
        with open(os.path.join(wi_dir, "status.yaml"), "w", encoding="utf-8") as f:
            f.write(fc.dump_yaml({"schema_version": 1, "id": wi_id, "state": state}))
        if events is not None:
            with open(os.path.join(wi_dir, "events.jsonl"), "w", encoding="utf-8") as f:
                for e in events:  # noqa: FURB122
                    f.write(json.dumps(e, ensure_ascii=False) + "\n")
        return wi_dir

    def _status_side_effect(self, mapping):
        def _fn(wi_dir):
            return {"state": mapping[os.path.basename(wi_dir)], "locked_by": None}
        return _fn


# ---------------------------------------------------------------------------
# A:任务终态钩子(§1.3)
# ---------------------------------------------------------------------------

class TerminalHookTests(WbIsoBase):
    def test_terminal_design_done_calls_on_designed(self):
        wi_dir = self._make_wi("w1", "designed")
        t = {"id": "t-1", "kind": "design", "workitem": "w1", "state": "done"}
        with mock.patch.object(tc._fc, "resolve_wi_dir", return_value=wi_dir), \
             mock.patch.object(tc._fc, "load_status", return_value={"state": "designed"}), \
             mock.patch.object(tc._fc, "load_config", return_value=self._full_cfg()), \
             mock.patch.object(tc._fc, "_run_post_transition_hooks") as mh:
            res = tc.run_terminal_hooks(t, self._cfg(), "done")
        self.assertEqual(res["action"], "design_done")
        mh.assert_called_once()
        self.assertEqual(mh.call_args[0][0], wi_dir)
        self.assertEqual(mh.call_args[0][1], {"event": "design", "to": "designed"})

    def test_terminal_execute_done_calls_on_executed(self):
        wi_dir = self._make_wi("w1", "executed")
        t = {"id": "t-1", "kind": "execute", "workitem": "w1", "state": "done"}
        with mock.patch.object(tc._fc, "resolve_wi_dir", return_value=wi_dir), \
             mock.patch.object(tc._fc, "load_status", return_value={"state": "executed"}), \
             mock.patch.object(tc._fc, "load_config", return_value=self._full_cfg()), \
             mock.patch.object(tc._fc, "_run_post_transition_hooks") as mh:
            res = tc.run_terminal_hooks(t, self._cfg(), "done")
        self.assertEqual(res["action"], "execute_done")
        mh.assert_called_once()
        self.assertEqual(mh.call_args[0][1], {"event": "execute", "to": "executed"})

    def test_terminal_design_done_no_transition(self):
        # design done 但 workitem 未到 designed(force/raw 无转移)→ E17 不强制转移
        wi_dir = self._make_wi("w1", "created")
        t = {"id": "t-1", "kind": "design", "workitem": "w1", "state": "done"}
        with mock.patch.object(tc._fc, "resolve_wi_dir", return_value=wi_dir), \
             mock.patch.object(tc._fc, "load_status", return_value={"state": "created"}), \
             mock.patch.object(tc._fc, "_run_post_transition_hooks") as mh, \
             mock.patch.object(tc._fc, "_hook_auto_review") as mr:
            res = tc.run_terminal_hooks(t, self._cfg(), "done")
        self.assertEqual(res["action"], "design_done_no_transition")
        mh.assert_not_called()
        mr.assert_not_called()

    def test_wb_cfg_without_workitem_section_resolves_wi(self):
        """ocr7-H1:W-B 配置(task/host/chain,无 workitem 段)下 resolve_wi_dir 不再
        KeyError——_read_wi_state_safe 读到真实 state,_trigger_chain 正常触发钩子。"""
        wi_dir = os.path.join(self.data_dir, "workitems", "w1")
        os.makedirs(wi_dir, exist_ok=True)
        with open(os.path.join(wi_dir, "status.yaml"), "w", encoding="utf-8") as f:
            f.write(fc.dump_yaml({"schema_version": 1, "id": "w1", "state": "designed"}))
        wb_cfg = self._cfg()                       # 无 workitem 段(load_task_config 产物)
        self.assertNotIn("workitem", wb_cfg)
        # resolve_wi_dir 缺 workitem 段 → 默认 id_max_len,不再 KeyError
        self.assertEqual(tc._fc.resolve_wi_dir("w1", wb_cfg), wi_dir)
        # _read_wi_state_safe 读到真实 state(而非静默 None)
        self.assertEqual(tc._read_wi_state_safe("w1", wb_cfg), "designed")
        # _trigger_chain 不再 degrade 为 resolve_wi_dir error,正常触发钩子
        with mock.patch.object(tc._fc, "_run_post_transition_hooks") as mh:
            res = tc._trigger_chain("w1", wb_cfg, "design", "designed")
        self.assertEqual(res["result"], "executed")
        mh.assert_called_once()

    def test_terminal_execute_failed_notifies_failure_report(self):
        stub = os.path.join(self.tmp, "notify.sh")
        rec_path = os.path.join(self.tmp, "rec.json")
        with open(stub, "w", encoding="utf-8") as f:
            f.write("#!/usr/bin/env bash\ncat > \"$1\"\n")
        os.chmod(stub, 0o755)
        cfg = {"host": {"notify": f"{stub} {rec_path}"}, "task": {}, "chain": {"enabled": True}}
        t = {"id": "t-1", "kind": "execute", "workitem": "w1"}
        event_record = {"state": "failed", "exit_code": 2, "diagnostic": "boom tail"}
        with mock.patch.object(tc, "_notify_if_configured") as mn:
            tc._notify_terminal(cfg, t, event_record)
            mn.assert_not_called()                      # 无双发泛化通知
        with open(rec_path, encoding="utf-8") as f:
            rec = json.load(f)
        self.assertEqual(rec["kind"], "execute_failure")
        # ocr 修复(2026-08-25):exit_code/failure_tail 从 event_record 取
        # (t 是 runner 启动快照,不含终态字段;event_record 落账后构造,字段齐全)
        self.assertEqual(rec["exit_code"], 2)
        self.assertEqual(rec["failure_tail"], "boom tail")
        self.assertTrue(rec["guidance"])

    def test_terminal_execute_timeout_notifies_failure_report(self):
        stub = os.path.join(self.tmp, "notify-timeout.sh")
        rec_path = os.path.join(self.tmp, "rec-timeout.json")
        with open(stub, "w", encoding="utf-8") as f:
            f.write("#!/usr/bin/env bash\ncat > \"$1\"\n")
        os.chmod(stub, 0o755)
        cfg = {"host": {"notify": f"{stub} {rec_path}"}, "task": {}, "chain": {"enabled": True}}
        t = {"id": "t-1", "kind": "execute", "workitem": "w1"}
        event_record = {"state": "partial-complete", "exit_code": 124,
                        "diagnostic": "/tmp/redacted.log"}
        with mock.patch.object(tc, "_notify_if_configured") as mn:
            tc._notify_terminal(cfg, t, event_record)
            mn.assert_not_called()
        with open(rec_path, encoding="utf-8") as f:
            rec = json.load(f)
        self.assertEqual(rec["kind"], "execute_failure")
        self.assertEqual(rec["state"], "partial-complete")
        self.assertEqual(rec["exit_code"], 124)
        self.assertEqual(rec["failure_tail"], "/tmp/redacted.log")

    def test_terminal_no_kind_or_workitem_skips(self):
        with mock.patch.object(tc._fc, "_run_post_transition_hooks") as mh, \
             mock.patch.object(tc._fc, "_hook_auto_review") as mr, \
             mock.patch.object(tc._fc, "_hook_auto_verify") as mv:
            res = tc.run_terminal_hooks({"id": "t-1", "state": "done"}, self._cfg(), "done")
        self.assertEqual(res["action"], "skipped")
        mh.assert_not_called()
        mr.assert_not_called()
        mv.assert_not_called()

    def test_terminal_hook_exception_is_best_effort(self):
        # E19:resolve_wi_dir 抛异常 → 终态钩子记 error 不抛(审计 + 返回 result)
        t = {"id": "t-1", "kind": "execute", "workitem": "w1", "state": "done"}
        with mock.patch.object(tc._fc, "resolve_wi_dir", side_effect=RuntimeError("boom")):
            res = tc.run_terminal_hooks(t, self._cfg(), "done")
        self.assertEqual(res["action"], "execute_done")
        self.assertEqual(res["result"]["result"], "error")

    def test_terminal_audit_task_state_is_terminal(self):
        # ocr F1:终态审计 task_state 用真实终态(state 参数),非 runner 启动快照
        # (t 是启动时快照, state=="running";若写 t["state"] 每条终态审计都记 running)
        wi_dir = self._make_wi("w1", "designed")
        t = {"id": "t-1", "kind": "design", "workitem": "w1", "state": "running"}
        with mock.patch.object(tc._fc, "resolve_wi_dir", return_value=wi_dir), \
             mock.patch.object(tc._fc, "load_status", return_value={"state": "designed"}), \
             mock.patch.object(tc._fc, "load_config", return_value=self._full_cfg()), \
             mock.patch.object(tc._fc, "_run_post_transition_hooks") as mh:
            res = tc.run_terminal_hooks(t, self._cfg(), "done")
        self.assertEqual(res["action"], "design_done")
        mh.assert_called_once()
        path = os.path.join(tc.events_dir(), "audit.jsonl")
        self.assertTrue(os.path.isfile(path))
        with open(path, encoding="utf-8") as f:
            recs = [json.loads(ln) for ln in f.read().splitlines() if ln.strip()]
        tail = recs[-1]
        self.assertEqual(tail["stream"], tc.TERMINAL_AUDIT_STREAM)
        self.assertEqual(tail["task_id"], "t-1")
        self.assertEqual(tail["task_state"], "done")       # 真实终态
        self.assertNotEqual(tail["task_state"], "running")  # 快照 state 不被写入
        self.assertEqual(tail["action"], "design_done")


# ---------------------------------------------------------------------------
# B:超时演进(§1.4.2 分级 + 去重)
# ---------------------------------------------------------------------------

class StaleEvolveTests(WbIsoBase):
    def _gate(self, wi_id, state, hours):
        root = os.path.join(self.tmp, f"scan-{wi_id}")
        os.makedirs(root, exist_ok=True)
        wi_dir = self._make_wi(wi_id, state, events=[{"ts": _ago(hours)}], scan_root=root)
        return root, wi_dir

    def test_classify_stale_age_boundaries(self):
        cfg = self._cfg()
        self.assertEqual(tc.classify_stale_age(2 * 3600 - 1, cfg), "silent")
        self.assertEqual(tc.classify_stale_age(2 * 3600, cfg), "remind")
        self.assertEqual(tc.classify_stale_age(24 * 3600 - 1, cfg), "remind")
        self.assertEqual(tc.classify_stale_age(24 * 3600, cfg), "escalate")
        self.assertEqual(tc.classify_stale_age(48 * 3600 - 1, cfg), "escalate")
        self.assertEqual(tc.classify_stale_age(48 * 3600, cfg), "force")

    def test_stale_age_silent_under_2h(self):
        root, _wi_dir = self._gate("w-silent", "translated", 1)
        with mock.patch.object(tc._fc, "load_status", return_value={"state": "translated"}), \
             mock.patch.object(tc._fc, "is_locked", return_value=False), \
             mock.patch.object(tc._fc, "_hook_auto_enqueue") as me:
            res = tc.run_stale_gate(self._cfg(), now=_NOW, scan_roots=[root])
        self.assertEqual(res, [])
        me.assert_not_called()

    def test_stale_age_remind_2h_to_24h(self):
        root, _wi_dir = self._gate("w-remind", "translated", 10)
        with mock.patch.object(tc._fc, "load_status", return_value={"state": "translated"}), \
             mock.patch.object(tc._fc, "is_locked", return_value=False), \
             mock.patch.object(tc._fc, "_hook_auto_enqueue") as me:
            res = tc.run_stale_gate(self._cfg(), now=_NOW, scan_roots=[root])
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["result"], "notified")
        self.assertEqual(res[0]["level"], "remind")
        me.assert_not_called()                          # 仅提醒不 force

    def test_stale_age_escalate_24h_to_48h(self):
        root, _wi_dir = self._gate("w-escalate", "translated", 30)
        with mock.patch.object(tc._fc, "load_status", return_value={"state": "translated"}), \
             mock.patch.object(tc._fc, "is_locked", return_value=False), \
             mock.patch.object(tc._fc, "_hook_auto_enqueue") as me:
            res = tc.run_stale_gate(self._cfg(), now=_NOW, scan_roots=[root])
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["level"], "escalate")
        me.assert_not_called()

    def test_stale_remind_dedup_once_per_day(self):
        root, _wi_dir = self._gate("w-dedup", "translated", 10)
        with mock.patch.object(tc._fc, "load_status", return_value={"state": "translated"}), \
             mock.patch.object(tc._fc, "is_locked", return_value=False), \
             mock.patch.object(tc._fc, "_hook_auto_enqueue"):
            r1 = tc.run_stale_gate(self._cfg(), now=_NOW, scan_roots=[root])
            r2 = tc.run_stale_gate(self._cfg(), now=_NOW, scan_roots=[root])
        self.assertEqual(len(r1), 1)
        self.assertEqual(len(r2), 0)                    # 24h 内去重


# ---------------------------------------------------------------------------
# C:>48h 强制 R1-R5(§1.4.4)
# ---------------------------------------------------------------------------

class StaleForceTests(WbIsoBase):
    def _force_gate(self, wi_id, state):
        root = os.path.join(self.tmp, f"scan-force-{wi_id}")
        os.makedirs(root, exist_ok=True)
        wi_dir = self._make_wi(wi_id, state, events=[{"ts": _ago(50)}], scan_root=root)
        return root, wi_dir

    def test_force_r1_designed_auto_review_pass(self):
        root, wi_dir = self._force_gate("w-r1", "designed")
        with mock.patch.object(tc._fc, "load_status", return_value={"state": "designed"}), \
             mock.patch.object(tc._fc, "is_locked", return_value=False), \
             mock.patch.object(tc._fc, "load_config", return_value=self._full_cfg()), \
             mock.patch.object(tc._fc, "_hook_auto_review",
                               return_value={"action": "auto_review_pass",
                                             "result": {"pass": True}}) as mr:
            res = tc.run_stale_gate(self._cfg(), now=_NOW, scan_roots=[root])
        mr.assert_called_once()
        self.assertEqual(res[0]["result"], "pass")
        self.assertTrue(os.path.isdir(wi_dir))          # pass 不归档

    def test_force_r1_designed_mechanical_reject(self):
        root, wi_dir = self._force_gate("w-r1-rej", "designed")
        with mock.patch.object(tc._fc, "load_status", return_value={"state": "designed"}), \
             mock.patch.object(tc._fc, "is_locked", return_value=False), \
             mock.patch.object(tc._fc, "load_config", return_value=self._full_cfg()), \
             mock.patch.object(tc._fc, "_hook_auto_review",
                               return_value={"action": "auto_review_reject",
                                             "result": {"reasons": ["x"]}}):
            res = tc.run_stale_gate(self._cfg(), now=_NOW, scan_roots=[root])
        self.assertEqual(res[0]["result"], "reject")
        self.assertTrue(os.path.isdir(wi_dir))

    def test_force_r2_reviewed_auto_translate(self):
        root, _wi_dir = self._force_gate("w-r2", "reviewed")
        with mock.patch.object(tc._fc, "load_status", return_value={"state": "reviewed"}), \
             mock.patch.object(tc._fc, "is_locked", return_value=False), \
             mock.patch.object(tc._fc, "load_config", return_value=self._full_cfg()), \
             mock.patch.object(tc._fc, "_hook_auto_translate",
                               return_value={"result": "translated"}) as mt:
            res = tc.run_stale_gate(self._cfg(), now=_NOW, scan_roots=[root])
        mt.assert_called_once()
        self.assertEqual(res[0]["result"], "translated")

    def test_force_r3_translated_auto_enqueue(self):
        root, wi_dir = self._force_gate("w-r3", "translated")
        with open(os.path.join(wi_dir, "design-result.json"), "w", encoding="utf-8") as f:
            json.dump({"duration_s": 1200}, f)
        with mock.patch.object(tc._fc, "load_status", return_value={"state": "translated"}), \
             mock.patch.object(tc._fc, "is_locked", return_value=False), \
             mock.patch.object(tc._fc, "load_config", return_value=self._full_cfg()), \
             mock.patch.object(tc._fc, "_hook_auto_enqueue",
                               return_value={"result": "enqueued", "task_id": "t-x"}) as me:
            res = tc.run_stale_gate(self._cfg(), now=_NOW, scan_roots=[root])
        me.assert_called_once()
        self.assertEqual(res[0]["result"], "enqueued")
        with open(os.path.join(wi_dir, "design-result.json"), encoding="utf-8") as f:
            dr = json.load(f)
        # ocr7-L2:expected_seconds 为死字段(下游 _hook_auto_enqueue 自算,从不读)
        # → 已删注入;原字段保留
        self.assertNotIn("expected_seconds", dr)
        self.assertEqual(dr.get("duration_s"), 1200)         # 保留原字段

    def test_force_r4_r5_archive(self):
        for state in ("executed", "verified"):
            with self.subTest(state=state):
                root = os.path.join(self.tmp, f"scan-arch-{state}")
                os.makedirs(root, exist_ok=True)
                # ocr8-M2:force 级现走冷却去重;两个 subTest 若共用 wi_id "w1"
                # 会在共享的 events/audit.jsonl 上互相去重(第二个被跳过)。
                # 用不同 wi_id 隔离(真实场景不同 state 必有不同 id)。
                wi_dir = self._make_wi(f"w-{state}", state, events=[{"ts": _ago(50)}], scan_root=root)
                with mock.patch.object(tc._fc, "load_status", return_value={"state": state}), \
                     mock.patch.object(tc._fc, "is_locked", return_value=False), \
                     mock.patch.object(tc._fc, "load_config", return_value=self._full_cfg()):
                    res = tc.run_stale_gate(self._cfg(), now=_NOW, scan_roots=[root])
                self.assertEqual(res[0]["result"], "archived")
                self.assertFalse(os.path.isdir(wi_dir))     # 已移出

    def test_force_notify_only_dedup_when_stuck_persists(self):
        """ocr8-M2:force 级但未移出 stuck(chain 关闭 → notify_only)时,冷却期内
        不再重复触发——修复前 force 无条件放行,每个 pump 周期重复通知轰炸。"""
        root, wi_dir = self._force_gate("w-notify", "executed")
        cfg = self._cfg(enabled=False)               # chain 关闭 → notify_only
        with mock.patch.object(tc._fc, "load_status", return_value={"state": "executed"}), \
             mock.patch.object(tc._fc, "is_locked", return_value=False), \
             mock.patch.object(tc._fc, "load_config", return_value=self._full_cfg()), \
             mock.patch.object(tc, "_pipe_notify") as mn:
            r1 = tc.run_stale_gate(cfg, now=_NOW, scan_roots=[root])
            r2 = tc.run_stale_gate(cfg, now=_NOW, scan_roots=[root])
        self.assertEqual([x["result"] for x in r1], ["notified"])
        self.assertEqual(len(r2), 0)                 # 冷却期内去重:不重复通知
        self.assertEqual(mn.call_count, 1)           # 只通知了一次
        self.assertTrue(os.path.isdir(wi_dir))       # notify_only 不移出 stuck 集


# ---------------------------------------------------------------------------
# D:归档可恢复(§1.4.5)
# ---------------------------------------------------------------------------

class ArchiveTests(WbIsoBase):
    def test_archive_writes_marker_with_original_path(self):
        root = os.path.join(self.tmp, "scan-marker")
        os.makedirs(root, exist_ok=True)
        wi_dir = self._make_wi("w1", "verified", events=[{"ts": _ago(0)}], scan_root=root)
        res = tc.archive_stale_workitem("w1", wi_dir, "verified", "stale_>48h")
        self.assertEqual(res["result"], "archived")
        target = res["target"]
        marker_path = os.path.join(target, "stale.json")
        self.assertTrue(os.path.isfile(marker_path))
        with open(marker_path, encoding="utf-8") as f:
            marker = json.load(f)
        self.assertEqual(marker["id"], "w1")
        self.assertEqual(marker["state"], "verified")
        self.assertEqual(marker["reason"], "stale_>48h")
        self.assertEqual(marker["original_path"], wi_dir)
        self.assertIn("archived_at", marker)

    def test_archive_target_exists_suffix(self):
        root = os.path.join(self.tmp, "scan-suffix")
        os.makedirs(root, exist_ok=True)
        wi_dir = self._make_wi("w1", "executed", events=[{"ts": _ago(0)}], scan_root=root)
        stale_root = os.path.join(os.path.dirname(root), "stale")
        os.makedirs(os.path.join(stale_root, "w1"), exist_ok=True)   # E10 目标已存在
        res = tc.archive_stale_workitem("w1", wi_dir, "executed", "stale_>48h")
        self.assertEqual(res["result"], "archived")
        self.assertNotEqual(os.path.basename(res["target"]), "w1")   # 时间戳后缀避免覆盖

    def test_archive_recoverable_restore(self):
        root = os.path.join(self.tmp, "scan-restore")
        os.makedirs(root, exist_ok=True)
        wi_dir = self._make_wi("w1", "verified", events=[{"ts": _ago(0)}], scan_root=root)
        res = tc.archive_stale_workitem("w1", wi_dir, "verified", "stale_>48h")
        self.assertEqual(res["result"], "archived")
        target = res["target"]
        self.assertTrue(os.path.isfile(os.path.join(target, "status.yaml")))
        self.assertTrue(os.path.isfile(os.path.join(target, "events.jsonl")))
        # 手动 mv 回 workitems → 完整且再次可被扫描
        restored = os.path.join(root, "w1")
        shutil.move(target, restored)
        self.assertTrue(os.path.isfile(os.path.join(restored, "status.yaml")))
        self.assertEqual(tc._peek_state(restored), "verified")

    def test_archive_move_failure_keeps_in_place(self):
        root = os.path.join(self.tmp, "scan-movefail")
        os.makedirs(root, exist_ok=True)
        wi_dir = self._make_wi("w1", "executed", events=[{"ts": _ago(0)}], scan_root=root)
        with mock.patch.object(tc.shutil, "move", side_effect=OSError("cross-device")):
            res = tc.archive_stale_workitem("w1", wi_dir, "executed", "stale_>48h")
        self.assertEqual(res["result"], "error")
        self.assertTrue(os.path.isdir(wi_dir))          # E11 保留原地

    def test_archive_marker_failure_restores_in_place(self):
        """ocr6-F1:move 成功后 marker 写失败 → 尽力移回原处 + 失败通知,不留半归档。"""
        root = os.path.join(self.tmp, "scan-markerfail")
        os.makedirs(root, exist_ok=True)
        wi_dir = self._make_wi("w1", "executed", events=[{"ts": _ago(0)}], scan_root=root)
        stale_target = os.path.abspath(os.path.join(root, "..", "stale", "w1"))
        with mock.patch.object(tc, "_pipe_notify") as pn, \
             mock.patch.object(tc, "_atomic_write_local",
                               side_effect=OSError("disk full")):
            res = tc.archive_stale_workitem("w1", wi_dir, "executed", "stale_>48h",
                                            cfg=self._cfg())
        self.assertEqual(res["result"], "error")
        self.assertIn("restored", res["detail"])
        self.assertTrue(os.path.isdir(wi_dir))          # 已移回原处
        self.assertTrue(os.path.isfile(os.path.join(wi_dir, "status.yaml")))
        self.assertFalse(os.path.isdir(stale_target))   # stale 目录不留残影
        pn.assert_called_once()                         # 失败通知已发
        rec = pn.call_args[0][1]
        self.assertEqual(rec["kind"], "stale_archived")
        self.assertIsNone(rec["target"])
        self.assertIn("marker write failed", rec["error"])

    def test_archive_marker_failure_restore_fail_marks_recoverable(self):
        """ocr6-F1:marker 写失败且恢复也失败 → workitem 留在 stale 目录,通知携带
        target(可恢复位置),不留静默半归档状态。"""
        root = os.path.join(self.tmp, "scan-markerfail2")
        os.makedirs(root, exist_ok=True)
        wi_dir = self._make_wi("w1", "executed", events=[{"ts": _ago(0)}], scan_root=root)
        stale_target = os.path.abspath(os.path.join(root, "..", "stale", "w1"))
        real_move = tc.shutil.move
        calls = {"n": 0}

        def _move(src, dst):
            calls["n"] += 1
            if calls["n"] == 1:                        # 首次 move 正常(归档)
                return real_move(src, dst)
            raise OSError("restore blocked")           # 恢复 move 失败

        with mock.patch.object(tc.shutil, "move", side_effect=_move), \
             mock.patch.object(tc, "_atomic_write_local",
                               side_effect=OSError("disk full")), \
             mock.patch.object(tc, "_pipe_notify") as pn:
            res = tc.archive_stale_workitem("w1", wi_dir, "executed", "stale_>48h",
                                            cfg=self._cfg())
        self.assertEqual(res["result"], "error")
        self.assertIn("recover at", res["detail"])
        self.assertIn(stale_target, res["detail"])      # 可恢复位置明确
        self.assertFalse(os.path.isdir(wi_dir))         # 未恢复(留在 stale 目录)
        self.assertTrue(os.path.isdir(stale_target))    # 内容仍完整可恢复
        self.assertTrue(os.path.isfile(os.path.join(stale_target, "status.yaml")))
        pn.assert_called_once()
        rec = pn.call_args[0][1]
        self.assertEqual(rec["target"], stale_target)
        self.assertIn("restore failed", rec["error"])


# ---------------------------------------------------------------------------
# D2:扫描根解析(§1.4.7,ocr6-F3 去硬编码)
# ---------------------------------------------------------------------------

class ResolveScanRootsTests(WbIsoBase):
    def test_resolve_scan_roots_derives_base_from_data_dir(self):
        """ocr6-F3:无 FLOW_STALE_SCAN_ROOT/scan_projects 时,fallback 项目根由
        FLOW_DATA_DIR 派生(dirname(dirname(data_dir)) 下的兄弟项目),不再硬编码
        ~/.openclaw/workspace/projects——非默认数据目录部署时扫对根。"""
        os.environ.pop("FLOW_STALE_SCAN_ROOT", None)
        base = os.path.join(self.tmp, "projects")
        proj_a_flow = os.path.join(base, "proj-a", ".flow")
        os.makedirs(os.path.join(proj_a_flow, "workitems"), exist_ok=True)
        os.makedirs(os.path.join(base, "proj-b", ".flow", "workitems"), exist_ok=True)
        os.environ["FLOW_DATA_DIR"] = proj_a_flow        # 模拟 proj-a 的数据目录
        roots = tc.resolve_scan_roots(self._cfg())
        self.assertIn(os.path.join(proj_a_flow, "workitems"), roots)        # 恒含当前仓
        self.assertIn(os.path.join(base, "proj-b", ".flow", "workitems"),
                      roots)                            # 兄弟项目被扫到
        hard = os.path.expanduser("~/.openclaw/workspace/projects")
        self.assertFalse(any(r.startswith(hard) for r in roots))   # 硬编码根已消除


# ---------------------------------------------------------------------------
# E:expected_seconds 学习(§1.5)
# ---------------------------------------------------------------------------

class LearnExpectedTests(WbIsoBase):
    def test_learn_expected_seconds_from_registry(self):
        reg = {"schema_version": 1, "tasks": {
            "t-1": {"state": "done", "kind": "execute", "workdir": "/repo",
                    "started_at": "2026-08-24T00:00:00+00:00",
                    "finished_at": "2026-08-24T00:33:20+00:00"},   # 2000s
            "t-2": {"state": "done", "kind": "execute", "workdir": "/repo",
                    "started_at": "2026-08-24T00:00:00+00:00",
                    "finished_at": "2026-08-24T00:50:00+00:00"},   # 3000s
            "t-3": {"state": "done", "kind": "design", "workdir": "/repo",
                    "started_at": "2026-08-24T00:00:00+00:00",
                    "finished_at": "2026-08-24T01:00:00+00:00"},   # kind 不匹配
            "t-4": {"state": "queued", "kind": "execute", "workdir": "/repo",
                    "started_at": "2026-08-24T00:00:00+00:00",
                    "finished_at": "2026-08-24T01:00:00+00:00"},   # 非终态
        }}
        # samples=[2000,3000]; EMA=0.5*3000+0.5*2000=2500; ceil(2500*1.5)=3750
        self.assertEqual(tc.learn_expected_seconds(reg, "/repo", "execute", 1500), 3750)

    def test_learn_expected_seconds_fallback_no_history(self):
        self.assertEqual(tc.learn_expected_seconds({}, "/repo", "execute", 1500), 1500)
        self.assertEqual(tc.learn_expected_seconds({"tasks": {}}, None, "execute", 1500), 1500)
        self.assertEqual(tc.learn_expected_seconds({"tasks": {}}, "/repo", None, 1500), 1500)
        # 脏样本(时间戳非法)→ 忽略不 crash(E15)
        reg = {"tasks": {"t-1": {"state": "done", "kind": "execute", "workdir": "/repo",
                                 "started_at": "bogus",
                                 "finished_at": "2026-08-24T00:10:00+00:00"}}}
        self.assertEqual(tc.learn_expected_seconds(reg, "/repo", "execute", 1500), 1500)


# ---------------------------------------------------------------------------
# F:audit 落盘(§1.6)
# ---------------------------------------------------------------------------

class AuditTests(WbIsoBase):
    def test_stale_actions_append_audit_jsonl(self):
        root = os.path.join(self.tmp, "scan-audit")
        os.makedirs(root, exist_ok=True)
        self._make_wi("w-remind", "translated", events=[{"ts": _ago(10)}], scan_root=root)
        self._make_wi("w-escalate", "translated", events=[{"ts": _ago(30)}], scan_root=root)
        self._make_wi("w-force", "designed", events=[{"ts": _ago(50)}], scan_root=root)
        mapping = {"w-remind": "translated", "w-escalate": "translated", "w-force": "designed"}
        with mock.patch.object(tc._fc, "load_status",
                               side_effect=self._status_side_effect(mapping)), \
             mock.patch.object(tc._fc, "is_locked", return_value=False), \
             mock.patch.object(tc._fc, "load_config", return_value=self._full_cfg()), \
             mock.patch.object(tc._fc, "_hook_auto_review",
                               return_value={"action": "auto_review_pass",
                                             "result": {"pass": True}}):
            tc.run_stale_gate(self._cfg(), now=_NOW, scan_roots=[root])
        audit_path = os.path.join(self.task_dir, "events", "audit.jsonl")
        self.assertTrue(os.path.isfile(audit_path))
        with open(audit_path, encoding="utf-8") as f:
            rows = [json.loads(l) for l in f if l.strip()]
        levels = {r["workitem"]: r["level"] for r in rows}
        self.assertEqual(levels["w-remind"], "remind")
        self.assertEqual(levels["w-escalate"], "escalate")
        self.assertEqual(levels["w-force"], "force")
        for r in rows:
            self.assertEqual(r["stream"], "stale")
            self.assertIn("action", r)
            self.assertIn("result", r)
            self.assertIn("age_s", r)


# ---------------------------------------------------------------------------
# G:红线边界(§1.4/§3)
# ---------------------------------------------------------------------------

class RedlineTests(WbIsoBase):
    def test_chain_disabled_degrade_no_force(self):
        self.assertEqual(tc.stale_action_for_state("designed", False), "notify_only")
        self.assertEqual(tc.stale_action_for_state("executed", False), "notify_only")
        root = os.path.join(self.tmp, "scan-noforce")
        os.makedirs(root, exist_ok=True)
        self._make_wi("w1", "designed", events=[{"ts": _ago(50)}], scan_root=root)
        with mock.patch.object(tc._fc, "load_status", return_value={"state": "designed"}), \
             mock.patch.object(tc._fc, "is_locked", return_value=False), \
             mock.patch.object(tc._fc, "_hook_auto_review") as mr, \
             mock.patch.object(tc._fc, "_hook_auto_translate") as mt, \
             mock.patch.object(tc._fc, "_hook_auto_enqueue") as me:
            res = tc.run_stale_gate(self._cfg(enabled=False), now=_NOW, scan_roots=[root])
        mr.assert_not_called()
        mt.assert_not_called()
        me.assert_not_called()
        self.assertTrue(all(r["result"] == "notified" for r in res))

    def test_stale_skips_locked_workitem(self):
        root = os.path.join(self.tmp, "scan-locked")
        os.makedirs(root, exist_ok=True)
        self._make_wi("w1", "translated", events=[{"ts": _ago(50)}], scan_root=root)
        with mock.patch.object(tc._fc, "load_status",
                               return_value={"state": "translated", "locked_by": "someone"}), \
             mock.patch.object(tc._fc, "is_locked", return_value=True), \
             mock.patch.object(tc._fc, "_hook_auto_enqueue") as me:
            res = tc.run_stale_gate(self._cfg(), now=_NOW, scan_roots=[root])
        self.assertEqual(res, [])
        me.assert_not_called()

    def test_stale_skips_no_events(self):
        root = os.path.join(self.tmp, "scan-noevents")
        os.makedirs(root, exist_ok=True)
        self._make_wi("w1", "translated", events=None, scan_root=root)   # 无 events.jsonl
        with mock.patch.object(tc._fc, "load_status", return_value={"state": "translated"}), \
             mock.patch.object(tc._fc, "is_locked", return_value=False), \
             mock.patch.object(tc._fc, "_hook_auto_enqueue") as me:
            res = tc.run_stale_gate(self._cfg(), now=_NOW, scan_roots=[root])
        self.assertEqual(res, [])
        me.assert_not_called()

    def test_force_skips_require_human_review(self):
        # ocr10-M1:require_human_review 强制人工介入 → stale gate 不自动归档,
        # 保留原地 + 升级人工通知(不再 archived_skipped 归档绕过人工语义)
        root = os.path.join(self.tmp, "scan-human")
        os.makedirs(root, exist_ok=True)
        wi_dir = self._make_wi("w1", "designed", events=[{"ts": _ago(50)}], scan_root=root)
        with mock.patch.object(tc._fc, "load_status", return_value={"state": "designed"}), \
             mock.patch.object(tc._fc, "is_locked", return_value=False), \
             mock.patch.object(tc._fc, "load_config", return_value=self._full_cfg()), \
             mock.patch.object(tc._fc, "_hook_auto_review",
                               return_value={"action": "auto_review_skip",
                                             "output": {"reason": "require_human_review"}}), \
             mock.patch.object(tc, "_pipe_notify") as mp:
            res = tc.run_stale_gate(self._cfg(), now=_NOW, scan_roots=[root])
        self.assertEqual(res[0]["result"], "notified_skipped")
        self.assertTrue(os.path.isdir(wi_dir))        # 不归档,保留等人工处理
        records = [c.args[1] for c in mp.call_args_list]
        self.assertEqual([r["kind"] for r in records], ["stale_require_human"])
        self.assertEqual(records[0]["workitem"], "w1")

    def test_stale_gate_empty(self):
        # E18:扫描根不存在/无 workitems → 空结果幂等 exit 0
        res = tc.run_stale_gate(self._cfg(), now=_NOW, scan_roots=[os.path.join(self.tmp, "none")])
        self.assertEqual(res, [])


# ---------------------------------------------------------------------------
# ocr3 L2:last_event_ts 有界尾部读取
# ---------------------------------------------------------------------------

class LastEventTsTests(WbIsoBase):
    """ocr L2:last_event_ts 只读 events.jsonl 尾部 ≤STALE_TAIL_SCAN_BYTES,不逐行全量读。"""

    def test_last_event_ts_returns_last_line(self):
        """小文件:返回最后一条合法 ts;缺失/非法/naive → None(E1/E2 行为保持)。"""
        wi_dir = self._make_wi("w1", "designed", events=[
            {"ts": _ago(50), "event": "a"},
            {"ts": _ago(40), "event": "b"},
            {"ts": _ago(30), "event": "c"},
        ])
        self.assertEqual(tc.last_event_ts(wi_dir), datetime.fromisoformat(_ago(30)))
        # 缺失文件 → None(E1)
        self.assertIsNone(tc.last_event_ts(os.path.join(self.scan_root, "nope")))
        # 最后一行非法 → None(E1)
        bad = self._make_wi("w2", "designed", events=None)
        with open(os.path.join(bad, "events.jsonl"), "w", encoding="utf-8") as f:
            f.write(json.dumps({"ts": _ago(10), "event": "x"}) + "\n")
            f.write("not-json\n")
        self.assertIsNone(tc.last_event_ts(bad))
        # naive ts → None(E2)
        naive = self._make_wi("w3", "designed",
                              events=[{"ts": "2026-08-24T10:00:00", "event": "x"}])
        self.assertIsNone(tc.last_event_ts(naive))

    def test_last_event_ts_bounded_tail_read(self):
        """大文件(>STALE_TAIL_SCAN_BYTES):只读尾部有界字节,仍返回最后一条 ts。"""
        wi_dir = self._make_wi("w1", "designed", events=None)
        path = os.path.join(wi_dir, "events.jsonl")
        with open(path, "w", encoding="utf-8") as f:
            f.writelines("x" * 100 + "\n" for _ in range(2000))  # 200KB 填充,远超 64KB 上限
            f.write(json.dumps({"ts": _ago(30), "event": "last"}) + "\n")
        reads = []
        real_open = open

        class _SpyReader:
            """委托代理:包裹真实 BufferedReader,记录每次 read 的 n,转发
            read/seek/tell + 上下文管理器协议(CPython 文件对象无实例 __dict__,
            无法对 fobj.read 打属性补丁)。"""

            def __init__(self, fobj):
                self._fobj = fobj

            def read(self, n=-1):
                reads.append(n)
                return self._fobj.read(n)

            def seek(self, *args, **kwargs):
                return self._fobj.seek(*args, **kwargs)

            def tell(self):
                return self._fobj.tell()

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return self._fobj.__exit__(*args)

        def spy_open(p, mode="r", *a, **kw):
            fobj = real_open(p, mode, *a, **kw)
            return _SpyReader(fobj) if mode == "rb" else fobj

        with mock.patch.object(tc, "open", side_effect=spy_open, create=True):
            dt = tc.last_event_ts(wi_dir)
        self.assertEqual(dt, datetime.fromisoformat(_ago(30)))
        self.assertTrue(reads)                    # 走了 read(而非逐行迭代)
        self.assertTrue(all(n is not None and n <= tc.STALE_TAIL_SCAN_BYTES
                            for n in reads))      # 有界:单次读取 ≤ 上限字节


if __name__ == "__main__":
    unittest.main()
