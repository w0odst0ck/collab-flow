"""script-executor 测试：默认路由 / size gating / run.sh 契约（真跑安全命令，零 LLM 零网络）。"""

import json
import os
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# flow-core.py 是连字符文件名：importlib 加载（沿用 test-local-executor 模式）
import importlib.util

_FC_PATH = os.path.join(ROOT, "scripts", "flow-core.py")
_fc_spec = importlib.util.spec_from_file_location("flow_core_script", _FC_PATH)
flow_core = importlib.util.module_from_spec(_fc_spec)
_fc_spec.loader.exec_module(flow_core)

RUN_SH = os.path.join(ROOT, "executors", "script", "run.sh")


def _write_taskbook(dirpath, content):
    path = os.path.join(dirpath, "taskbook.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path


class ScriptRunShContractTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def _run(self, *args, **kwargs):
        return subprocess.run(
            ["bash", RUN_SH, *args], capture_output=True, text=True, timeout=30,
            check=False, **kwargs
        )

    def test_ok_echo(self):
        tb = _write_taskbook(self.tmp, "command: echo 'hello script'\n")
        out = os.path.join(self.tmp, "out")
        r = self._run("--taskbook", tb, "--workdir", self.tmp, "--out", out)
        self.assertEqual(r.returncode, 0)
        with open(os.path.join(out, "result.md"), encoding="utf-8") as f:
            self.assertIn("hello script", f.read())
        with open(os.path.join(out, "result.json"), encoding="utf-8") as f:
            rec = json.load(f)
        self.assertEqual(rec["executor"], "script")
        self.assertEqual(rec["status"], "ok")

    def test_missing_command_fails(self):
        tb = _write_taskbook(self.tmp, "no command line\n")
        r = self._run("--taskbook", tb, "--workdir", self.tmp, "--out", os.path.join(self.tmp, "o"))
        self.assertEqual(r.returncode, 1)

    def test_blacklist_rejected(self):
        tb = _write_taskbook(self.tmp, "command: rm -rf /tmp/xxx\n")
        r = self._run("--taskbook", tb, "--workdir", self.tmp, "--out", os.path.join(self.tmp, "o"))
        self.assertEqual(r.returncode, 4)  # fail-closed 拒绝

    def test_timeout(self):
        tb = _write_taskbook(self.tmp, "command: sleep 5\n")
        out = os.path.join(self.tmp, "o")
        r = self._run("--taskbook", tb, "--workdir", self.tmp, "--out", out, "--timeout", "1")
        self.assertEqual(r.returncode, 2)  # 超时 → run.sh 层 failed
        with open(os.path.join(out, "result.json"), encoding="utf-8") as f:
            rec = json.load(f)
        self.assertEqual(rec["status"], "timeout")


class ScriptRouteTests(unittest.TestCase):
    """flow-core 路由：任务书 command: 行 → script；size=large 拒绝。"""

    def _wi(self, taskbook_md):
        tmp = tempfile.mkdtemp()
        _write_taskbook(tmp, taskbook_md)
        return tmp

    def test_default_route_command_to_script(self):
        wi = self._wi("command: echo hi\n")
        cfg = {"executor": {"default": "reasonix"}}
        executor, _ = flow_core._resolve_executor_decl(wi, cfg)
        self.assertEqual(executor, "script")

    def test_default_route_no_command_to_default(self):
        wi = self._wi("# 普通任务\nmodel: flash\n")
        cfg = {"executor": {"default": "reasonix"}}
        executor, _ = flow_core._resolve_executor_decl(wi, cfg)
        self.assertEqual(executor, "reasonix")

    def test_explicit_executor_wins(self):
        wi = self._wi("```flow\nexecutor: local\n```\ncommand: echo hi\n")
        cfg = {"executor": {"default": "reasonix"}}
        executor, _ = flow_core._resolve_executor_decl(wi, cfg)
        self.assertEqual(executor, "local")

    def test_script_rejects_large(self):
        wi = self._wi("command: echo hi\n")
        cfg = {"executor": {"script": {"timeout_s": 300}}}
        with self.assertRaises(flow_core.GateReject):
            flow_core.resolve_execute_params(
                wi, cfg, executor="script", cli_size="large")

    def test_script_accepts_small(self):
        wi = self._wi("command: echo hi\n")
        cfg = {"executor": {"script": {"timeout_s": 300}}}
        params = flow_core.resolve_execute_params(
            wi, cfg, executor="script", cli_size="small")
        self.assertEqual(params["timeout_s"], 300)
        self.assertIsNone(params["model"])


if __name__ == "__main__":
    unittest.main()
