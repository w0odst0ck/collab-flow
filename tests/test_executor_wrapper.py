#!/usr/bin/env python3
"""reasonix wrapper 单测(RX_BIN 注入 stub rx,零 API)+ flow-core partial-complete 分支。

用法: python3 -m unittest discover tests
覆盖(design reasonix-executor-robustness §4.1):
  E11 RX_TIMEOUT 透传 / E12 内层超时(124) / E13 外层 guard(143→125) /
  E14 partial-complete / E15 redact {10,} + 空回退 / E16 模型决策(阈值边界 + --model 覆盖)
  flow-core: partial-complete 分支(exit 124、不转移)+ run_executor --model 透传 + --model 空值拒绝
"""

import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_WRAPPER = os.path.join(_HERE, "..", "executors", "reasonix", "wrapper.sh")
_FLOW_CORE = os.path.join(_HERE, "..", "scripts", "flow-core.py")

_spec = importlib.util.spec_from_file_location("flow_core_rx", _FLOW_CORE)
fc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fc)

_ENV_PREFIXES = ("FLOW_", "COLLABFLOW_", "STUB_", "RX_", "DSH_", "DEEPSEEK")

# stub rx:记录 argv + env(RX_TIMEOUT/RX_MODEL)到 RX_STUB_RECORD;行为受 env 控制,
# 全程零真实调用、零 key;故意忽略 RX_TIMEOUT(模拟 rx 内层自管超时的场景,E13)。
STUB_RX = r'''#!/usr/bin/env bash
set -u
if [[ -n "${RX_STUB_RECORD:-}" ]]; then
  {
    printf 'argv:'
    printf ' <%s>' "$@"
    printf '\nRX_TIMEOUT=%s\nRX_MODEL=%s\n' "${RX_TIMEOUT:-}" "${RX_MODEL:-}"
  } > "$RX_STUB_RECORD"
fi
if [[ "${RX_STUB_DIFF:-0}" == "1" ]]; then
  echo "stub rx change $(date +%s)" >> README.md
fi
if [[ "${RX_STUB_SLEEP:-0}" != "0" ]]; then
  sleep "${RX_STUB_SLEEP}"
fi
if [[ "${RX_STUB_LEAK:-0}" == "1" ]]; then
  LEAK_PREFIX="sk-"
  LEAK_SUFFIX="abcdefghijklmnopqrstuvwxyz1234"
  echo "leak ${LEAK_PREFIX}${LEAK_SUFFIX}" >&2
fi
exit "${RX_STUB_EXIT:-0}"
'''


class ReasonixWrapperTests(unittest.TestCase):
    """wrapper 契约:退出码区分 / RX_TIMEOUT 透传 / 模型决策 / partial-complete / 诊断。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="rx-wrap-")
        self.workdir = os.path.join(self.tmp, "work")
        os.makedirs(self.workdir, exist_ok=True)
        subprocess.run(["git", "init", "-q", self.workdir], check=True)
        subprocess.run(["git", "-C", self.workdir, "config", "user.email", "t@example.com"], check=True)
        subprocess.run(["git", "-C", self.workdir, "config", "user.name", "tester"], check=True)
        with open(os.path.join(self.workdir, "README.md"), "w", encoding="utf-8") as f:
            f.write("# init\n")
        subprocess.run(["git", "-C", self.workdir, "add", "README.md"], check=True)
        subprocess.run(["git", "-C", self.workdir, "commit", "-q", "-m", "init"], check=True)
        self.out = os.path.join(self.tmp, "out")
        os.makedirs(self.out, exist_ok=True)
        self.taskbook = os.path.join(self.tmp, "taskbook.md")
        self._write_taskbook("# tb\n")
        self.stub_rx = os.path.join(self.tmp, "stub-rx.sh")
        with open(self.stub_rx, "w", encoding="utf-8") as f:
            f.write(STUB_RX)
        os.chmod(self.stub_rx, 0o755)
        self.record = os.path.join(self.tmp, "rx-record.txt")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write_taskbook(self, text):
        with open(self.taskbook, "w", encoding="utf-8") as f:
            f.write(text)

    def _run(self, timeout="7", model=None, extra=None):
        cmd = [_WRAPPER, "--taskbook", self.taskbook, "--workdir", self.workdir,
               "--out", self.out, "--timeout", timeout]
        if model is not None:
            cmd += ["--model", model]
        env = dict(os.environ)
        env.update({"RX_BIN": self.stub_rx, "RX_STUB_RECORD": self.record})
        if extra:
            env.update(extra)
        return subprocess.run(cmd, env=env, capture_output=True, text=True)

    def _result(self):
        with open(os.path.join(self.out, "result.json"), encoding="utf-8") as f:
            return json.load(f)

    def _record(self):
        with open(self.record, encoding="utf-8") as f:
            return f.read()

    def test_E11_rx_timeout_passthrough(self):
        """--timeout 7 → stub 收到 RX_TIMEOUT=7;rx_timeout_s=7;小任务书走 flash。"""
        p = self._run(timeout="7")
        self.assertEqual(p.returncode, 0)
        r = self._result()
        self.assertEqual(r["status"], "ok")
        self.assertEqual(r["rx_timeout_s"], 7)
        self.assertEqual(r["model"], "deepseek-v4-flash")
        self.assertIn("RX_TIMEOUT=7", self._record())
        self.assertIn("RX_MODEL=deepseek-v4-flash", self._record())
        self.assertTrue(r["duration_s"] >= 0)

    def test_E02_failed_rc1(self):
        """stub exit 1 → wrapper exit 1,status=failed,redacted_logs 非空。"""
        p = self._run(extra={"RX_STUB_EXIT": "1"})
        self.assertEqual(p.returncode, 1)
        r = self._result()
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["exit_code"], 1)
        self.assertTrue(r["redacted_logs"])

    def test_E12_timeout_inner_rx(self):
        """stub exit 124 且无 diff → status=timeout,timeout_source=rx,wrapper exit 124。"""
        p = self._run(extra={"RX_STUB_EXIT": "124"})
        self.assertEqual(p.returncode, 124)
        r = self._result()
        self.assertEqual(r["status"], "timeout")
        self.assertEqual(r["timeout_source"], "rx")
        self.assertEqual(r["exit_code"], 124)
        self.assertFalse(r["partial_complete"])
        self.assertTrue(r["redacted_logs"])

    def test_E13_timeout_wrapper_guard(self):
        """stub 长睡忽略 RX_TIMEOUT(timeout=1, GRACE=1 → GUARD=2,sleep 3) →
        外层 guard 杀 → exit 125,timeout_source=wrapper,不判 partial-complete。"""
        p = self._run(timeout="1", extra={"RX_STUB_SLEEP": "3", "RX_STUB_EXIT": "124",
                                          "RX_WRAPPER_GRACE_S": "1"})
        self.assertEqual(p.returncode, 125)
        r = self._result()
        self.assertEqual(r["status"], "timeout")
        self.assertEqual(r["timeout_source"], "wrapper")
        self.assertEqual(r["exit_code"], 125)
        self.assertFalse(r["partial_complete"])

    def test_E14_partial_complete(self):
        """stub exit 124 且先造 diff → status=partial-complete,partial_complete=true,exit 仍 124。"""
        p = self._run(extra={"RX_STUB_EXIT": "124", "RX_STUB_DIFF": "1"})
        self.assertEqual(p.returncode, 124)
        r = self._result()
        self.assertEqual(r["status"], "partial-complete")
        self.assertTrue(r["partial_complete"])
        self.assertEqual(r["timeout_source"], "rx")
        self.assertEqual(r["exit_code"], 124)
        self.assertTrue(r["redacted_logs"])

    def test_E15_redact_long_key(self):
        """stub 泄漏 30 字符 key → redacted_logs 无任何 sk- 残留、含 [REDACTED]。"""
        p = self._run(extra={"RX_STUB_EXIT": "1", "RX_STUB_LEAK": "1"})
        self.assertEqual(p.returncode, 1)
        r = self._result()
        logs = r["redacted_logs"]
        self.assertTrue(logs)
        self.assertIn("[REDACTED]", logs)
        self.assertNotRegex(logs, r"sk-[A-Za-z0-9]{10}")

    def test_E15b_no_output_fallback(self):
        """stub exit 1 且无任何输出 → redacted_logs 回退占位,恒非空。"""
        p = self._run(extra={"RX_STUB_EXIT": "1"})
        self.assertEqual(p.returncode, 1)
        self.assertEqual(self._result()["redacted_logs"], "(no rx output)")

    def test_E16a_threshold_pro(self):
        """任务书字节数 >= 阈值(env 缩到 10) → RX_MODEL=deepseek-v4-pro + rx --pro 标志。"""
        self._write_taskbook("# " + "x" * 20 + "\n")  # > 10 字节
        p = self._run(extra={"RX_MODEL_PRO_THRESHOLD_BYTES": "10"})
        self.assertEqual(p.returncode, 0)
        rec = self._record()
        self.assertIn("RX_MODEL=deepseek-v4-pro", rec)
        self.assertIn("<--pro>", rec)
        self.assertEqual(self._result()["model"], "deepseek-v4-pro")

    def test_E16b_cli_override_threshold(self):
        """--model 显式 flash 覆盖大任务书阈值 → flash,不加 --pro。"""
        self._write_taskbook("# " + "x" * 20 + "\n")
        p = self._run(model="deepseek-v4-flash", extra={"RX_MODEL_PRO_THRESHOLD_BYTES": "10"})
        self.assertEqual(p.returncode, 0)
        rec = self._record()
        self.assertIn("RX_MODEL=deepseek-v4-flash", rec)
        self.assertNotIn("--pro", rec)
        self.assertEqual(self._result()["model"], "deepseek-v4-flash")


# ---------------------------------------------------------------------------
# flow-core 增量(design §2.3.4):partial-complete 分支 + --model 解析透传
# ---------------------------------------------------------------------------

def _cfg():
    return {
        "workitem": {"plane_id": "control", "lock_ttl_s": 3600, "id_max_len": 64},
        "gates": {"takeover_after_same_defect": 2, "iteration_limit": 3},
        "executor": {"default": "reasonix", "timeout_s": 1800, "diff_scope": None},
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


class FlowCorePartialCompleteTests(unittest.TestCase):
    """flow-core 最小增量:partial-complete 分支(不转移、exit 124、提示)+ --model 解析/透传。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="fc-pc-")
        self.workdir = os.path.join(self.tmp, "work")
        os.makedirs(self.workdir, exist_ok=True)
        subprocess.run(["git", "init", "-q", self.workdir], check=True)
        subprocess.run(["git", "-C", self.workdir, "config", "user.email", "t@example.com"], check=True)
        subprocess.run(["git", "-C", self.workdir, "config", "user.name", "tester"], check=True)
        with open(os.path.join(self.workdir, "README.md"), "w", encoding="utf-8") as f:
            f.write("# init\n")
        subprocess.run(["git", "-C", self.workdir, "add", "README.md"], check=True)
        subprocess.run(["git", "-C", self.workdir, "commit", "-q", "-m", "init"], check=True)
        self._saved = {k: v for k, v in os.environ.items() if k.startswith(_ENV_PREFIXES)}
        for k in list(self._saved):
            os.environ.pop(k, None)
        os.environ["FLOW_DATA_DIR"] = os.path.join(self.tmp, ".flow")
        os.environ["FLOW_WORKDIR"] = self.workdir
        os.environ["COLLABFLOW_CONFIG"] = os.path.join(self.tmp, "no-cfg.yaml")

    def tearDown(self):
        for k in [k for k in list(os.environ) if k.startswith(_ENV_PREFIXES)]:
            os.environ.pop(k, None)
        os.environ.update(self._saved)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _mk_wi(self, state="translated"):
        wi_dir = os.path.join(fc.workitems_dir(), "w1")
        os.makedirs(wi_dir, exist_ok=True)
        fc.save_status_atomic(wi_dir, _status("w1", state))
        with open(os.path.join(wi_dir, "taskbook.md"), "w", encoding="utf-8") as f:
            f.write("# taskbook\n")
        return wi_dir

    def _write_result(self, wi_dir, status="partial-complete"):
        result = {"schema_version": 1, "executor": "stub", "status": status,
                  "exit_code": 124, "timeout_source": "rx", "model": "deepseek-v4-pro",
                  "rx_timeout_s": 1, "partial_complete": True, "duration_s": 1,
                  "diff": {"files_changed": 1, "insertions": 1, "deletions": 0,
                           "changed_files": ["README.md"], "untracked_files": [],
                           "patch": "executor/diff.patch"},
                  "test_command": None, "cost": None, "redacted_logs": "tail rx output",
                  "started_at": "", "finished_at": ""}
        os.makedirs(os.path.join(wi_dir, "executor"), exist_ok=True)
        with open(os.path.join(wi_dir, "executor", "result.json"), "w", encoding="utf-8") as f:
            json.dump(result, f)
    def test_PC01_partial_complete_branch(self):
        """partial-complete → exit 124、state 留 translated、stderr 含快速路提示。"""
        wi_dir = self._mk_wi("translated")
        os.environ["STUB_EXECUTOR_NO_RESULT"] = "1"
        os.environ["STUB_EXECUTOR_EXIT"] = "0"
        self._write_result(wi_dir)
        code = fc.cmd_execute(["w1", "--executor", "stub"], _cfg())
        self.assertEqual(code, 124)
        self.assertEqual(fc.load_status(wi_dir)["state"], "translated")

    def test_PC02_run_executor_model_passthrough(self):
        """model 非空 → cmd 追加 --model;缺省 → 不追加。"""
        captured = {}

        def fake_run(cmd, **kw):
            captured["cmd"] = list(cmd)
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        orig = fc.subprocess.run
        fc.subprocess.run = fake_run
        try:
            fc.run_executor("/w", "/t", "/w", "/o", 10, model="deepseek-v4-pro")
            self.assertIn("--model", captured["cmd"])
            self.assertIn("deepseek-v4-pro", captured["cmd"])
            fc.run_executor("/w", "/t", "/w", "/o", 10)
            self.assertNotIn("--model", captured["cmd"])
        finally:
            fc.subprocess.run = orig

    def test_PC03_model_empty_rejected(self):
        """--model 空串 → exit 2,不转移。"""
        wi_dir = self._mk_wi("translated")
        code = fc.cmd_execute(["w1", "--executor", "stub", "--model", ""], _cfg())
        self.assertEqual(code, 2)
        self.assertEqual(fc.load_status(wi_dir)["state"], "translated")


if __name__ == "__main__":
    unittest.main(verbosity=2)
