#!/usr/bin/env python3
"""flow-core.py P3 单测:执行器契约(E1-E10) + 设计器契约(D1-D3) + verify 门契约(V1-V9)。

用法: python3 -m unittest discover tests
零 API、全程临时目录 + stub。纯函数直接 import(load_executor_spec / resolve_test_command /
check_diff_scope / check_error_table 等)。
"""

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_FLOW_CORE = os.path.join(_HERE, "..", "scripts", "flow-core.py")

_spec = importlib.util.spec_from_file_location("flow_core_p3", _FLOW_CORE)
fc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fc)

_ENV_PREFIXES = ("FLOW_", "COLLABFLOW_", "STUB_", "DSH_", "DEEPSEEK")

# 故意构造的 key 用拼接,避免源码静态 deny-list 命中(sk-[A-Za-z0-9]{10})
FAKE_KEY = "sk-" + "fake-test-key"


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


def _jload(path):
    return json.loads(fc.read_file(path))


# stub dsh(复用 run-smoke 的 session 生成模式,零 API)
STUB_DSH = r'''#!/usr/bin/env bash
set -u
if [[ "${DSH_STUB_SESSION:-0}" == "1" ]]; then
  MODEL="${DSH_STUB_MODEL:-deepseek-v4-flash}"
  python3 - "$DSH_HOME" "$PWD" "$MODEL" << 'PY'
import json, os, subprocess, sys, time
home, cwd, model = sys.argv[1], sys.argv[2], sys.argv[3]
def project_key(cwd):
    readable=[]; run=False
    for ch in cwd:
        if ch in "/\\:":
            if not run: readable.append("-")
            run=True
        elif ch != "~" and ((ch.isascii() and ch.isalnum()) or ch in "._-"):
            readable.append(ch); run=False
        else:
            readable.append("~"+format(ord(ch),"04X")); run=False
    core=("".join(readable)).lstrip("-") or "root"
    return "--" + core[:251] + "--"
proj = os.path.join(home, "sessions", project_key(cwd))
os.makedirs(proj, exist_ok=True)
sid = "session-stub-" + str(int(time.time()))
d = os.path.join(proj, sid); os.makedirs(d, exist_ok=True)
events = [
  {"type":"session","version":0,"id":sid,"createdAt":int(time.time()*1000),"cwd":cwd,"delegationDepth":0},
  {"type":"request/header","data":{"header":{"config":{"model":model}}}},
  {"type":"assistant/message","data":{"usage":{"inputTokens":10,"outputTokens":8,"cacheReadTokens":5,"reasoningTokens":2}}},
]
raw = os.path.join(d,"session.jsonl")
with open(raw,"w") as f:
    for e in events: f.write(json.dumps(e)+"\n")
out = raw + ".zstd"
subprocess.run(["zstd","-f","-o",out,raw], check=True, capture_output=True)
os.remove(raw)
PY
fi
cat << 'MD'
# stub 方案

这是 stub 输出的假方案内容，用于 P3 契约测试。
MD
exit "${DSH_STUB_EXIT:-0}"
'''


class _Base(unittest.TestCase):
    def base_setup(self):
        self.tmp = tempfile.mkdtemp(prefix="flow-p3-")
        self.workdir = os.path.join(self.tmp, "work")
        os.makedirs(self.workdir, exist_ok=True)
        self._saved = {k: v for k, v in os.environ.items() if k.startswith(_ENV_PREFIXES)}
        for k in list(self._saved):
            os.environ.pop(k, None)
        os.environ["FLOW_DATA_DIR"] = os.path.join(self.tmp, ".flow")
        os.environ["FLOW_WORKDIR"] = self.workdir
        os.environ["COLLABFLOW_CONFIG"] = os.path.join(self.tmp, "no-cfg.yaml")

    def base_teardown(self):
        for k in [k for k in list(os.environ) if k.startswith(_ENV_PREFIXES)]:
            os.environ.pop(k, None)
        os.environ.update(self._saved)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _mk_wi(self, state):
        wi_dir = os.path.join(fc.workitems_dir(), "w1")
        os.makedirs(wi_dir, exist_ok=True)
        fc.save_status_atomic(wi_dir, _status("w1", state))
        return wi_dir

    def _write_taskbook(self, wi_dir, text="# taskbook\n"):
        with open(os.path.join(wi_dir, "taskbook.md"), "w", encoding="utf-8") as f:
            f.write(text)

    def _git_init(self):
        subprocess.run(["git", "init", "-q", self.workdir], check=True)
        subprocess.run(["git", "-C", self.workdir, "config", "user.email", "t@example.com"], check=True)
        subprocess.run(["git", "-C", self.workdir, "config", "user.name", "tester"], check=True)
        with open(os.path.join(self.workdir, "README.md"), "w", encoding="utf-8") as f:
            f.write("# init\n")
        subprocess.run(["git", "-C", self.workdir, "add", "README.md"], check=True)
        subprocess.run(["git", "-C", self.workdir, "commit", "-q", "-m", "init"], check=True)


class ExecutorContractTests(_Base):
    """E1-E10 执行器契约(§P3 §5.2)。"""

    def setUp(self):
        self.base_setup()

    def tearDown(self):
        self.base_teardown()

    def test_E01_stub_ok(self):
        self._git_init()
        wi_dir = self._mk_wi("translated")
        self._write_taskbook(wi_dir)
        os.environ["STUB_EXECUTOR_EXIT"] = "0"
        code = fc.cmd_execute(["w1", "--executor", "stub"], _cfg())
        self.assertEqual(code, 0)
        r = _jload(os.path.join(wi_dir, "executor", "result.json"))
        self.assertEqual(r["status"], "ok")
        self.assertGreater(os.path.getsize(os.path.join(wi_dir, "executor", "diff.patch")), 0)
        self.assertEqual(fc.load_status(wi_dir)["state"], "executed")

    def test_E02_stub_failed(self):
        self._git_init()
        wi_dir = self._mk_wi("translated")
        self._write_taskbook(wi_dir)
        os.environ["STUB_EXECUTOR_EXIT"] = "1"
        code = fc.cmd_execute(["w1", "--executor", "stub"], _cfg())
        self.assertEqual(code, 1)
        r = _jload(os.path.join(wi_dir, "executor", "result.json"))
        self.assertEqual(r["status"], "failed")
        self.assertEqual(fc.load_status(wi_dir)["state"], "translated")

    def test_E03_stub_timeout(self):
        self._git_init()
        wi_dir = self._mk_wi("translated")
        self._write_taskbook(wi_dir)
        os.environ["STUB_EXECUTOR_SLEEP"] = "5"
        code = fc.cmd_execute(["w1", "--executor", "stub", "--timeout", "1"], _cfg())
        self.assertEqual(code, 124)
        r = _jload(os.path.join(wi_dir, "executor", "result.json"))
        self.assertEqual(r["status"], "timeout")
        self.assertEqual(fc.load_status(wi_dir)["state"], "translated")

    def test_E04_missing_result(self):
        self._git_init()
        wi_dir = self._mk_wi("translated")
        self._write_taskbook(wi_dir)
        os.environ["STUB_EXECUTOR_NO_RESULT"] = "1"
        code = fc.cmd_execute(["w1", "--executor", "stub"], _cfg())
        self.assertEqual(code, 2)
        self.assertEqual(fc.load_status(wi_dir)["state"], "translated")

    def test_E05_corrupt_result(self):
        self._git_init()
        wi_dir = self._mk_wi("translated")
        self._write_taskbook(wi_dir)
        os.environ["STUB_EXECUTOR_NO_RESULT"] = "1"
        # stub 不写 result.json;手动写损坏 JSON 供 execute 读取
        os.makedirs(os.path.join(wi_dir, "executor"), exist_ok=True)
        with open(os.path.join(wi_dir, "executor", "result.json"), "w", encoding="utf-8") as f:
            f.write("{not-valid-json")
        code = fc.cmd_execute(["w1", "--executor", "stub"], _cfg())
        self.assertEqual(code, 2)
        self.assertEqual(fc.load_status(wi_dir)["state"], "translated")

    def test_E06_spec_readonly(self):
        base = os.path.join(self.tmp, "specs")
        os.makedirs(os.path.join(base, "ro"))
        with open(os.path.join(base, "ro", "spec.yaml"), "w", encoding="utf-8") as f:
            f.write("id: ro\nruntime:\n  os: linux\ninvoke:\n  binary: x\n  timeout_s: 1\n  sandbox: read-only\n")
        with self.assertRaises(fc.StoreError):
            fc.load_executor_spec("ro", exec_dir=base)

    def test_E07_spec_os_mismatch(self):
        base = os.path.join(self.tmp, "specs")
        os.makedirs(os.path.join(base, "mac"))
        with open(os.path.join(base, "mac", "spec.yaml"), "w", encoding="utf-8") as f:
            f.write("id: mac\nruntime:\n  os: darwin\ninvoke:\n  binary: x\n  timeout_s: 1\n  sandbox: workspace-write\n")
        with self.assertRaises(fc.StoreError):
            fc.load_executor_spec("mac", exec_dir=base)

    def test_E08_missing_diff(self):
        self._git_init()
        wi_dir = self._mk_wi("translated")
        self._write_taskbook(wi_dir)
        os.environ["STUB_EXECUTOR_NO_DIFF"] = "1"
        code = fc.cmd_execute(["w1", "--executor", "stub"], _cfg())
        self.assertEqual(code, 2)
        self.assertEqual(fc.load_status(wi_dir)["state"], "translated")

    def test_E09_result_sk_leak(self):
        self._git_init()
        wi_dir = self._mk_wi("translated")
        self._write_taskbook(wi_dir)
        os.environ["STUB_EXECUTOR_NO_RESULT"] = "1"
        leak = "sk-" + "abcdefghij12345"
        os.makedirs(os.path.join(wi_dir, "executor"), exist_ok=True)
        with open(os.path.join(wi_dir, "executor", "result.json"), "w", encoding="utf-8") as f:
            json.dump({"schema_version": 1, "executor": "stub", "status": "ok",
                       "exit_code": 0, "redacted_logs": leak}, f)
        code = fc.cmd_execute(["w1", "--executor", "stub"], _cfg())
        self.assertEqual(code, 2)
        self.assertEqual(fc.load_status(wi_dir)["state"], "translated")

    def test_E10_spec_flow_collection(self):
        base = os.path.join(self.tmp, "specs")
        os.makedirs(os.path.join(base, "flow"))
        with open(os.path.join(base, "flow", "spec.yaml"), "w", encoding="utf-8") as f:
            f.write("id: flow\nruntime:\n  os: linux\ninvoke:\n  binary: x\n  timeout_s: 1\n  sandbox: workspace-write\nargs: {a: 1}\n")
        with self.assertRaises(fc.StoreError):
            fc.load_executor_spec("flow", exec_dir=base)


class DesignerContractTests(_Base):
    """D1-D3 设计器契约(§P3 §5.2)。"""

    def setUp(self):
        self.base_setup()
        self._saved_home = os.environ.get("HOME")
        os.environ["HOME"] = os.path.join(self.tmp, "home")
        os.makedirs(os.environ["HOME"], exist_ok=True)
        os.environ["DEEPSEEK_API_KEY"] = FAKE_KEY
        os.environ["DSH_HOME"] = os.path.join(self.tmp, "dshhome")
        os.environ["DSH_DESIGN_PRO_PATCH"] = os.path.join(self.tmp, "pro.patch.yml")
        self.stub_dsh = os.path.join(self.tmp, "stub-dsh.sh")
        with open(self.stub_dsh, "w", encoding="utf-8") as f:
            f.write(STUB_DSH)
        os.chmod(self.stub_dsh, 0o755)
        os.environ["DSH_BIN"] = self.stub_dsh

    def tearDown(self):
        if self._saved_home is None:
            os.environ.pop("HOME", None)
        else:
            os.environ["HOME"] = self._saved_home
        self.base_teardown()

    def _mk_brief(self, wi_dir, text="# brief\n"):
        with open(os.path.join(wi_dir, "brief.md"), "w", encoding="utf-8") as f:
            f.write(text)

    def test_D01_design_ok(self):
        wi_dir = self._mk_wi("created")
        self._mk_brief(wi_dir)
        os.environ["DSH_STUB_SESSION"] = "1"
        os.environ["DSH_STUB_MODEL"] = "deepseek-v4-pro"
        code = fc.cmd_design(["w1"], _cfg())
        self.assertEqual(code, 0)
        self.assertTrue(os.path.isfile(os.path.join(wi_dir, "design.md")))
        dr = _jload(os.path.join(wi_dir, "design-result.json"))
        self.assertIn("model", dr)
        self.assertEqual(dr["model"], "deepseek-v4-pro")
        self.assertIsNotNone(dr.get("usage"))
        self.assertEqual(fc.load_status(wi_dir)["state"], "designed")

    def test_D02_model_flash_reject(self):
        wi_dir = self._mk_wi("created")
        self._mk_brief(wi_dir)
        os.environ["DSH_STUB_SESSION"] = "1"
        os.environ["DSH_STUB_MODEL"] = "deepseek-v4-flash"
        code = fc.cmd_design(["w1"], _cfg())
        self.assertEqual(code, 1)
        self.assertFalse(os.path.isfile(os.path.join(wi_dir, "design.md")))
        self.assertEqual(fc.load_status(wi_dir)["state"], "created")

    def test_D03_empty_brief(self):
        wi_dir = self._mk_wi("created")
        # brief.md 为空(不写内容)
        with open(os.path.join(wi_dir, "brief.md"), "w", encoding="utf-8") as f:
            f.write("")
        code = fc.cmd_design(["w1"], _cfg())
        self.assertEqual(code, 2)
        self.assertEqual(fc.load_status(wi_dir)["state"], "created")


class VerifyGateTests(_Base):
    """V1-V9 verify 门契约(§P3 §5.2)。"""

    def setUp(self):
        self.base_setup()

    def tearDown(self):
        self.base_teardown()

    def _mk_executed_wi(self, changed_files=None):
        wi_dir = os.path.join(fc.workitems_dir(), "w1")
        os.makedirs(os.path.join(wi_dir, "executor"), exist_ok=True)
        fc.save_status_atomic(wi_dir, _status("w1", "executed"))
        if changed_files is None:
            changed_files = ["scripts/flow-core.py"]
        result = {"schema_version": 1, "executor": "stub", "status": "ok", "exit_code": 0,
                  "duration_s": 0,
                  "diff": {"files_changed": len(changed_files), "insertions": 1, "deletions": 0,
                           "changed_files": changed_files, "untracked_files": [],
                           "patch": "executor/diff.patch"},
                  "test_command": None, "cost": None, "redacted_logs": ""}
        with open(os.path.join(wi_dir, "executor", "result.json"), "w", encoding="utf-8") as f:
            json.dump(result, f)
        with open(os.path.join(wi_dir, "executor", "diff.patch"), "w", encoding="utf-8") as f:
            f.write("diff --git a/x b/x\n+x\n")
        return wi_dir

    def _write_design_with_table(self, wi_dir, covered=True):
        test_col = "E2" if covered else ""
        content = (
            "# 设计\n\n"
            "| # | 错误场景 | 处理 | 测试覆盖 |\n"
            "|---|---|---|---|\n"
            "| E1 | a | 处理1 | E2 |\n"
            "| E2 | b | 处理2 | %s |\n" % test_col
        )
        with open(os.path.join(wi_dir, "design.md"), "w", encoding="utf-8") as f:
            f.write(content)

    def _write_taskbook_scope(self, wi_dir, allow, deny, test_command="true"):
        lines = ["# taskbook\n", "```flow\n", "test_command: %s\n" % test_command,
                 "diff_scope:\n"]
        if allow is not None:
            lines.append("  allow:\n")
            for a in allow:
                lines.append("    - %s\n" % a)
        if deny is not None:
            lines.append("  deny:\n")
            for d in deny:
                lines.append("    - %s\n" % d)
        lines.append("```\n")
        with open(os.path.join(wi_dir, "taskbook.md"), "w", encoding="utf-8") as f:
            f.write("".join(lines))

    def test_V01_all_pass(self):
        wi_dir = self._mk_executed_wi()
        self._write_taskbook_scope(wi_dir, ["scripts/flow-core.py"], None)
        self._write_design_with_table(wi_dir, covered=True)
        code = fc.cmd_verify(["w1", "--auto", "--test-command", "true"], _cfg())
        self.assertEqual(code, 0)
        v = _jload(os.path.join(wi_dir, "executor", "verify.json"))
        self.assertTrue(v["tests_pass"] and v["diff_match"] and v["error_table_match"])
        self.assertEqual(fc.load_status(wi_dir)["state"], "verified")
        self.assertTrue(os.path.isfile(os.path.join(wi_dir, "verify.md")))

    def test_V02_test_fail_impl(self):
        wi_dir = self._mk_executed_wi()
        self._write_taskbook_scope(wi_dir, ["scripts/flow-core.py"], None)
        self._write_design_with_table(wi_dir, covered=True)
        code = fc.cmd_verify(["w1", "--auto", "--test-command", "false"], _cfg())
        self.assertEqual(code, 1)
        v = _jload(os.path.join(wi_dir, "executor", "verify.json"))
        self.assertFalse(v["tests_pass"])
        self.assertEqual(v["route"], "impl")
        self.assertEqual(fc.load_status(wi_dir)["state"], "executed")

    def test_V03_out_of_scope_impl(self):
        wi_dir = self._mk_executed_wi(changed_files=["scripts/dsh-design"])
        self._write_taskbook_scope(wi_dir, None, ["scripts/dsh-design"])
        self._write_design_with_table(wi_dir, covered=True)
        code = fc.cmd_verify(["w1", "--auto", "--test-command", "true"], _cfg())
        self.assertEqual(code, 1)
        v = _jload(os.path.join(wi_dir, "executor", "verify.json"))
        self.assertFalse(v["diff_match"])
        self.assertEqual(v["route"], "impl")
        self.assertEqual(v["details"]["diff"]["out_of_scope"], ["scripts/dsh-design"])
        self.assertEqual(fc.load_status(wi_dir)["state"], "executed")

    def test_V04_scope_undeclared_design(self):
        wi_dir = self._mk_executed_wi()
        # 不写 diff_scope 前置块 → scope_undeclared
        with open(os.path.join(wi_dir, "taskbook.md"), "w", encoding="utf-8") as f:
            f.write("# taskbook\n")
        self._write_design_with_table(wi_dir, covered=True)
        code = fc.cmd_verify(["w1", "--auto", "--test-command", "true"], _cfg())
        self.assertEqual(code, 1)
        v = _jload(os.path.join(wi_dir, "executor", "verify.json"))
        self.assertFalse(v["diff_match"])
        self.assertEqual(v["details"]["diff"]["scope_verdict"], "undeclared")
        self.assertEqual(v["route"], "design")
        self.assertEqual(fc.load_status(wi_dir)["state"], "designed")

    def test_V05_error_table_missing_design(self):
        wi_dir = self._mk_executed_wi()
        self._write_taskbook_scope(wi_dir, ["scripts/flow-core.py"], None)
        # 不写错误表
        with open(os.path.join(wi_dir, "design.md"), "w", encoding="utf-8") as f:
            f.write("# 设计\n")
        code = fc.cmd_verify(["w1", "--auto", "--test-command", "true"], _cfg())
        self.assertEqual(code, 1)
        v = _jload(os.path.join(wi_dir, "executor", "verify.json"))
        self.assertFalse(v["error_table_match"])
        self.assertEqual(v["route"], "design")
        self.assertEqual(fc.load_status(wi_dir)["state"], "designed")

    def test_V06_error_table_uncovered_design(self):
        wi_dir = self._mk_executed_wi()
        self._write_taskbook_scope(wi_dir, ["scripts/flow-core.py"], None)
        self._write_design_with_table(wi_dir, covered=False)
        code = fc.cmd_verify(["w1", "--auto", "--test-command", "true"], _cfg())
        self.assertEqual(code, 1)
        v = _jload(os.path.join(wi_dir, "executor", "verify.json"))
        self.assertFalse(v["error_table_match"])
        self.assertIn("E2", v["details"]["error_table"]["uncovered"])
        self.assertEqual(v["route"], "design")
        self.assertEqual(fc.load_status(wi_dir)["state"], "designed")

    def test_V07_command_unresolved_design(self):
        wi_dir = self._mk_executed_wi()
        # 无 test_command 任何来源(无 --test-command/result.json.test_command/前置块/惯例)
        with open(os.path.join(wi_dir, "taskbook.md"), "w", encoding="utf-8") as f:
            f.write("# taskbook\n")
        self._write_design_with_table(wi_dir, covered=True)
        code = fc.cmd_verify(["w1", "--auto"], _cfg())
        self.assertEqual(code, 1)
        v = _jload(os.path.join(wi_dir, "executor", "verify.json"))
        self.assertFalse(v["tests_pass"])
        self.assertEqual(v["details"]["tests"]["reason"], "command_unresolved")
        self.assertEqual(v["route"], "design")
        self.assertEqual(fc.load_status(wi_dir)["state"], "designed")

    def test_V08_no_transition(self):
        wi_dir = self._mk_executed_wi()
        self._write_taskbook_scope(wi_dir, ["scripts/flow-core.py"], None)
        self._write_design_with_table(wi_dir, covered=True)
        code = fc.cmd_verify(["w1", "--auto", "--test-command", "true", "--no-transition"], _cfg())
        self.assertEqual(code, 0)
        self.assertTrue(os.path.isfile(os.path.join(wi_dir, "executor", "verify.json")))
        self.assertTrue(os.path.isfile(os.path.join(wi_dir, "verify.md")))
        self.assertEqual(fc.load_status(wi_dir)["state"], "executed")

    def test_V09_bare_verify_p2(self):
        wi_dir = self._mk_executed_wi()
        code = fc.cmd_verify(["w1"], _cfg())
        self.assertEqual(code, 0)
        v = _jload(os.path.join(wi_dir, "executor", "verify.json"))
        self.assertTrue(v["tests_pass"] and v["diff_match"] and v["error_table_match"])
        self.assertIsNone(v["route"])
        self.assertEqual(fc.load_status(wi_dir)["state"], "executed")


if __name__ == "__main__":
    unittest.main(verbosity=2)
