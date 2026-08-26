"""local-executor(local 模型批处理线)单测:模型注册表 / local size gating / run.sh 契约。

用例: A models 注册表解析(defaults.yaml → resolve_model,未知名 fail-closed) /
B local size gating(large → GateReject,small/medium 放行,timeout 默认 1200 可配) /
C 任务书 model/executor 声明解析(flow 块 + 正文行锚定) /
D run.sh 契约(缺参 exit 1 / 假 curl 失败 exit 2,零真实 ollama)。
零 API、全程临时目录;直接 importlib 加载 flow-core.py(沿用 test-flow-core.py 模式)。
用法: python3 -m unittest discover tests
"""

import importlib.util
import json
import os
import shutil
import subprocess
import tempfile
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.join(_HERE, "..")
_FLOW_CORE = os.path.join(_ROOT, "scripts", "flow-core.py")
_RUN_SH = os.path.join(_ROOT, "executors", "local", "run.sh")

_spec = importlib.util.spec_from_file_location("flow_core_local", _FLOW_CORE)
fc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fc)

_ENV_PREFIXES = ("FLOW_", "COLLABFLOW_", "CHAIN_", "STUB_", "HOME", "DSH_",
                 "DEEPSEEK", "OLLAMA_", "LOCAL_")


def _status(wi_id="w1", state="translated"):
    return {
        "schema_version": 1, "id": wi_id, "state": state, "iteration": 0,
        "same_defect_count": 0, "primary_defect_type": None, "takeover": False,
        "re_execute_count": 0, "process_version": "1.0.0",
        "created_at": "2026-08-14T10:00:00+00:00",
        "updated_at": "2026-08-14T10:00:00+00:00",
        "event_seq": 1, "locked_by": None, "lock_expires_at": None,
    }


def _cfg(models=None, local_cfg=None, default="reasonix", **kw):
    """最小 cfg;models 段缺省 → 由测试注入;executor.local 可经 local_cfg 覆盖。"""
    cfg = {
        "workitem": {"plane_id": "control", "lock_ttl_s": 3600, "id_max_len": 64},
        "gates": {"takeover_after_same_defect": 2, "iteration_limit": 3},
        "executor": {"default": default, "timeout_s": 1800, "diff_scope": None},
        "task": {"default_priority": "P2",
                 "expected_seconds_seed": {"design": 480, "execute": 1800}},
        "chain": {"enabled": True, "on_designed": "off",
                  "on_reviewed": True, "on_translated": True, "on_executed": True,
                  "on_verified": True, "review_llm": False,
                  "review_llm_model": "stub-model", "storm_threshold": 3,
                  "overlap_minutes": 40, "dry_run": False, "max_depth": 8,
                  "notify": None, "warn_hours": 24, "force_hours": 48,
                  "scan_projects": None},
    }
    if models is not None:
        cfg["models"] = models
    if local_cfg is not None:
        cfg["executor"]["local"] = local_cfg
    cfg["executor"].update(kw)
    return cfg


def _default_models():
    """与 config/defaults.yaml models 段一致的注册表(测试最小集)。"""
    return {
        "qwen35-9b": {"type": "llm", "endpoint": "local",
                      "url": "http://127.0.0.1:11434", "model": "qwen3.5:9b",
                      "think": True, "speed": 52, "cost": 0,
                      "note": "本地 LLM(GPU,52 tok/s),思考可关"},
        "qwen3-8b": {"type": "llm", "endpoint": "local",
                     "url": "http://127.0.0.1:11434", "model": "qwen3:8b",
                     "think": True, "cost": 0, "note": "本地 LLM 备选"},
        "deepseek-flash": {"type": "llm", "endpoint": "api",
                           "provider": "deepseek", "model": "deepseek-v4-flash",
                           "cost": "low", "note": "云 LLM"},
        "deepseek-pro": {"type": "llm", "endpoint": "api",
                         "provider": "deepseek", "model": "deepseek-v4-pro",
                         "cost": "high", "note": "云 LLM(复杂任务)"},
    }


class ModelsRegistryTests(unittest.TestCase):
    """A: models 注册表解析(load_config 真实 defaults.yaml + resolve_model)。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="flow-local-")
        self._saved = {k: v for k, v in os.environ.items()
                       if k.startswith(_ENV_PREFIXES)}
        for k in list(self._saved):
            os.environ.pop(k, None)
        os.environ["COLLABFLOW_CONFIG"] = os.path.join(self.tmp, "no-cfg.yaml")
        os.environ["HOME"] = os.path.join(self.tmp, "home")
        os.makedirs(os.environ["HOME"], exist_ok=True)

    def tearDown(self):
        for k in [k for k in list(os.environ) if k.startswith(_ENV_PREFIXES)]:
            os.environ.pop(k, None)
        os.environ.update(self._saved)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_A1_defaults_yaml_models_loaded(self):
        """真实 defaults.yaml → load_config → models 段含 8 条目。"""
        cfg = fc.load_config()
        models = cfg.get("models") or {}
        for name in ("qwen35-9b", "qwen3-8b", "deepseek-flash", "deepseek-pro",
                     "wan21-1.3b", "ltx-2b", "bge-m3", "bge-reranker"):
            self.assertIn(name, models, f"注册表缺 {name}")
        # 红线:video 条目 path 必须是 ~ 占位,绝无真实路径
        for name in ("wan21-1.3b", "ltx-2b"):
            self.assertTrue(str(models[name]["path"]).startswith("~/"),
                            f"{name}.path 必须 ~ 占位: {models[name]['path']}")

    def test_A2_resolve_model_known(self):
        """resolve_model(qwen35-9b) 返回注册表条目(含 ollama 模型名/端点)。"""
        cfg = _cfg(models=_default_models())
        entry = fc.resolve_model("qwen35-9b", cfg)
        self.assertEqual(entry["type"], "llm")
        self.assertEqual(entry["endpoint"], "local")
        self.assertEqual(entry["model"], "qwen3.5:9b")
        self.assertEqual(entry["url"], "http://127.0.0.1:11434")
        self.assertTrue(entry["think"])
        # 返回副本:调用方改动不影响注册表
        entry["x"] = 1
        self.assertNotIn("x", cfg["models"]["qwen35-9b"])

    def test_A3_resolve_model_unknown_fail_closed(self):
        """注册表不存在 → UsageError,文案列出可用模型(fail-closed)。"""
        cfg = _cfg(models=_default_models())
        with self.assertRaises(fc.UsageError) as cm:
            fc.resolve_model("no-such-model", cfg)
        msg = str(cm.exception)
        self.assertIn("no-such-model", msg)
        self.assertIn("qwen35-9b", msg)  # 列出可用模型
        self.assertIn("deepseek-pro", msg)

    def test_A4_resolve_model_empty_and_bad_type(self):
        """空名/非字符串 → UsageError(fail-closed)。"""
        cfg = _cfg(models=_default_models())
        with self.assertRaises(fc.UsageError):
            fc.resolve_model("", cfg)
        with self.assertRaises(fc.UsageError):
            fc.resolve_model(None, cfg)
        with self.assertRaises(fc.UsageError):
            fc.resolve_model(42, cfg)

    def test_A5_resolve_model_empty_registry(self):
        """注册表空 → 报错并标注 (空)。"""
        cfg = _cfg(models={})
        with self.assertRaises(fc.UsageError) as cm:
            fc.resolve_model("x", cfg)
        self.assertIn("(空)", str(cm.exception))


class LocalSizeGatingTests(unittest.TestCase):
    """B: executor=local size gating(large → GateReject;small 放行)+ timeout 默认。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="flow-local-gate-")
        self.workdir = os.path.join(self.tmp, "work")
        os.makedirs(self.workdir, exist_ok=True)
        self._saved = {k: v for k, v in os.environ.items()
                       if k.startswith(_ENV_PREFIXES)}
        for k in list(self._saved):
            os.environ.pop(k, None)
        os.environ["FLOW_DATA_DIR"] = os.path.join(self.tmp, ".flow")
        os.environ["FLOW_WORKDIR"] = self.workdir
        os.environ["COLLABFLOW_CONFIG"] = os.path.join(self.tmp, "no-cfg.yaml")
        os.environ["HOME"] = os.path.join(self.tmp, "home")
        os.makedirs(os.environ["HOME"], exist_ok=True)
        os.makedirs(fc.workitems_dir(), exist_ok=True)

    def tearDown(self):
        for k in [k for k in list(os.environ) if k.startswith(_ENV_PREFIXES)]:
            os.environ.pop(k, None)
        os.environ.update(self._saved)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _mk_wi(self, wi_id="w1", flow_body=None):
        wi_dir = os.path.join(fc.workitems_dir(), wi_id)
        os.makedirs(wi_dir, exist_ok=True)
        fc.save_status_atomic(wi_dir, _status(wi_id, "translated"))
        body = flow_body or "test_command: pytest\n"
        with open(os.path.join(wi_dir, "taskbook.md"), "w", encoding="utf-8") as f:
            f.write("# 任务书\n\n```flow\n" + body + "```\n")
        return wi_dir

    def _cfg_with_models(self, local_cfg=None):
        return _cfg(models=_default_models(), local_cfg=local_cfg)

    def test_B1_local_large_gate_reject(self):
        """executor=local + size=large → GateReject(fail-closed)。"""
        wi_dir = self._mk_wi(flow_body="test_command: pytest\nsize: large\n")
        cfg = self._cfg_with_models()
        with self.assertRaises(fc.GateReject) as cm:
            fc.resolve_execute_params(wi_dir, cfg, cli_size="large",
                                      executor="local")
        self.assertEqual(cm.exception.args[0], "size_gate_local")

    def test_B2_local_large_estimated_reject(self):
        """executor=local + 估算 large(design.md 超 30KB)→ GateReject。"""
        wi_dir = self._mk_wi()
        with open(os.path.join(wi_dir, "design.md"), "wb") as f:
            f.write(b"#" * 40000)
        cfg = self._cfg_with_models()
        with self.assertRaises(fc.GateReject):
            fc.resolve_execute_params(wi_dir, cfg, executor="local")

    def test_B3_local_small_pass(self):
        """executor=local + size=small → 放行;模型 qwen35-9b(注册表),timeout 1200。"""
        wi_dir = self._mk_wi()
        cfg = self._cfg_with_models()
        p = fc.resolve_execute_params(wi_dir, cfg, cli_size="small",
                                      executor="local")
        self.assertEqual(p["size"], "small")
        self.assertEqual(p["model"], "qwen35-9b")
        self.assertEqual(p["timeout_s"], fc.LOCAL_TIMEOUT_DEFAULT)

    def test_B4_local_medium_pass(self):
        """executor=local + size=medium → 放行。"""
        wi_dir = self._mk_wi()
        cfg = self._cfg_with_models()
        p = fc.resolve_execute_params(wi_dir, cfg, cli_size="medium",
                                      executor="local")
        self.assertEqual(p["model"], "qwen35-9b")
        self.assertEqual(p["timeout_s"], 1200)

    def test_B5_local_timeout_configurable(self):
        """config executor.local.timeout_s 可配(600);缺失/损坏回退 1200。"""
        wi_dir = self._mk_wi()
        cfg = self._cfg_with_models(local_cfg={"timeout_s": 600})
        p = fc.resolve_execute_params(wi_dir, cfg, cli_size="small",
                                      executor="local")
        self.assertEqual(p["timeout_s"], 600)
        # 损坏值 → 兜底 1200
        cfg2 = self._cfg_with_models(local_cfg={"timeout_s": "abc"})
        p2 = fc.resolve_execute_params(wi_dir, cfg2, cli_size="small",
                                       executor="local")
        self.assertEqual(p2["timeout_s"], fc.LOCAL_TIMEOUT_DEFAULT)

    def test_B6_local_cli_timeout_override(self):
        """local 下 --timeout 显式覆盖仍生效。"""
        wi_dir = self._mk_wi()
        cfg = self._cfg_with_models()
        p = fc.resolve_execute_params(wi_dir, cfg, cli_size="small",
                                      cli_timeout="300", executor="local")
        self.assertEqual(p["timeout_s"], 300)

    def test_B7_reasonix_path_unchanged(self):
        """reasonix(默认)不破坏现状:small 映射 flash,large 强制 pro 无降级。"""
        wi_dir = self._mk_wi()
        cfg = _cfg(models=_default_models())  # 无 executor.local
        p = fc.resolve_execute_params(wi_dir, cfg, cli_size="small")
        self.assertEqual(p["model"], "deepseek-v4-flash")
        self.assertEqual(p["timeout_s"], 1200)
        p2 = fc.resolve_execute_params(wi_dir, cfg, cli_size="large")
        self.assertEqual(p2["model"], "deepseek-v4-pro")
        self.assertEqual(p2["timeout_s"], 2400)


class LocalModelDeclTests(unittest.TestCase):
    """C: 任务书 executor/model 声明解析(flow 块 + 正文行锚定)+ 路由。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="flow-local-decl-")
        self.workdir = os.path.join(self.tmp, "work")
        os.makedirs(self.workdir, exist_ok=True)
        self._saved = {k: v for k, v in os.environ.items()
                       if k.startswith(_ENV_PREFIXES)}
        for k in list(self._saved):
            os.environ.pop(k, None)
        os.environ["FLOW_DATA_DIR"] = os.path.join(self.tmp, ".flow")
        os.environ["FLOW_WORKDIR"] = self.workdir
        os.environ["COLLABFLOW_CONFIG"] = os.path.join(self.tmp, "no-cfg.yaml")
        os.environ["HOME"] = os.path.join(self.tmp, "home")
        os.makedirs(os.environ["HOME"], exist_ok=True)
        os.makedirs(fc.workitems_dir(), exist_ok=True)

    def tearDown(self):
        for k in [k for k in list(os.environ) if k.startswith(_ENV_PREFIXES)]:
            os.environ.pop(k, None)
        os.environ.update(self._saved)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _mk_wi(self, flow_body=None, body_extra=""):
        wi_dir = os.path.join(fc.workitems_dir(), "w1")
        os.makedirs(wi_dir, exist_ok=True)
        fc.save_status_atomic(wi_dir, _status("w1", "translated"))
        lines = ["# 任务书", "", "```flow", flow_body or "test_command: pytest",
                 "```", body_extra]
        with open(os.path.join(wi_dir, "taskbook.md"), "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        return wi_dir

    def test_C1_flow_block_executor_decl(self):
        """flow 块 executor: local → 路由 local;无声明 → cfg default。"""
        wi_dir = self._mk_wi("executor: local\ntest_command: pytest\n")
        cfg = _cfg(models=_default_models(), default="reasonix")
        name, tb_model = fc._resolve_executor_decl(wi_dir, cfg)
        self.assertEqual(name, "local")
        self.assertIsNone(tb_model)
        wi_dir2 = self._mk_wi("test_command: pytest\n")
        name2, _ = fc._resolve_executor_decl(wi_dir2, cfg)
        self.assertEqual(name2, "reasonix")

    def test_C2_flow_block_model_decl(self):
        """flow 块 model: qwen3-8b(注册表任意名)→ tb_model;CLI 优先于声明。"""
        wi_dir = self._mk_wi("executor: local\nmodel: qwen3-8b\n")
        cfg = _cfg(models=_default_models())
        p = fc.resolve_execute_params(wi_dir, cfg, cli_size="small",
                                      executor="local",
                                      tb_model=fc._taskbook_model_decl(wi_dir))
        self.assertEqual(p["model"], "qwen3-8b")
        p2 = fc.resolve_execute_params(wi_dir, cfg, cli_size="small",
                                       cli_model="qwen35-9b", executor="local",
                                       tb_model="qwen3-8b")
        self.assertEqual(p2["model"], "qwen35-9b")  # CLI > 任务书声明

    def test_C3_body_line_model_decl(self):
        """正文行锚定 model: 声明(flow 块无 model)→ 仍解析(与 wrapper sed 同语义)。"""
        wi_dir = self._mk_wi("test_command: pytest\n",
                             body_extra="\nmodel: qwen3-8b\n")
        tb_model = fc._taskbook_model_decl(wi_dir)
        self.assertEqual(tb_model, "qwen3-8b")

    def test_C4_unknown_tb_model_fail_closed(self):
        """任务书声明未注册模型 → UsageError(列出可用)。"""
        wi_dir = self._mk_wi("executor: local\nmodel: nope-xyz\n")
        cfg = _cfg(models=_default_models())
        with self.assertRaises(fc.UsageError) as cm:
            fc.resolve_execute_params(wi_dir, cfg, cli_size="small",
                                      executor="local", tb_model="nope-xyz")
        self.assertIn("nope-xyz", str(cm.exception))

    def test_C5_bad_executor_decl_fail_closed(self):
        """flow 块 executor 声明空值 → UsageError(不静默回退)。"""
        wi_dir = self._mk_wi("executor:\ntest_command: pytest\n")
        cfg = _cfg(models=_default_models())
        with self.assertRaises(fc.UsageError):
            fc._resolve_executor_decl(wi_dir, cfg)

    def test_C6_build_execute_command_local(self):
        """build_execute_command:executor=local 时固化 local 模型/超时(子进程幂等)。"""
        wi_dir = self._mk_wi("executor: local\nmodel: qwen3-8b\n")
        cfg = _cfg(models=_default_models())
        cmd = fc.build_execute_command(wi_dir, cfg)
        self.assertIn("--executor local", cmd)
        self.assertIn("--model qwen3-8b", cmd)
        self.assertIn("--timeout 1200", cmd)

    def test_C7_cmd_execute_local_large_rejected(self):
        """cmd_execute:executor=local + size=large → fail 2 + size_gate_local。"""
        import contextlib
        import io
        subprocess.run(["git", "init", "-q", self.workdir], check=True)
        subprocess.run(["git", "-C", self.workdir, "config", "user.email",
                        "t@example.com"], check=True)
        subprocess.run(["git", "-C", self.workdir, "config", "user.name",
                        "tester"], check=True)
        with open(os.path.join(self.workdir, "README.md"), "w",
                  encoding="utf-8") as f:
            f.write("# init\n")
        subprocess.run(["git", "-C", self.workdir, "add", "README.md"], check=True)
        subprocess.run(["git", "-C", self.workdir, "commit", "-q", "-m", "init"],
                       check=True)
        wi_dir = self._mk_wi("executor: local\nsize: large\n")
        cfg = _cfg(models=_default_models())
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            code = fc.cmd_execute(["w1", "--sync"], cfg)
        self.assertEqual(code, 2)
        self.assertIn("size_gate_local", buf.getvalue())



class LocalRunShContractTests(unittest.TestCase):
    """D: run.sh 契约(假 curl 注入,零真实 ollama)。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="flow-local-runsh-")
        self._saved = {k: v for k, v in os.environ.items()
                       if k.startswith(_ENV_PREFIXES)}
        for k in list(self._saved):
            os.environ.pop(k, None)
        self.fake_bin = os.path.join(self.tmp, "bin")
        os.makedirs(self.fake_bin, exist_ok=True)
        self.tb = os.path.join(self.tmp, "tb.md")
        with open(self.tb, "w", encoding="utf-8") as f:
            f.write("# 任务\n\n把这段文字翻译成英文。\n")

    def tearDown(self):
        for k in [k for k in list(os.environ) if k.startswith(_ENV_PREFIXES)]:
            os.environ.pop(k, None)
        os.environ.update(self._saved)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _run(self, *argv, env=None):
        merged = {"PATH": self.fake_bin + ":" + os.environ.get("PATH", ""),
                  "LOCAL_CURL_BIN": os.path.join(self.fake_bin, "curl")}
        merged["PATH"] = env.get("PATH", merged["PATH"]) if env else merged["PATH"]
        e = dict(os.environ)
        e.update(merged)
        if env:
            e.update(env)
        return subprocess.run(["bash", _RUN_SH] + list(argv),
                              capture_output=True, text=True, env=e, timeout=60)

    def _fake_curl(self, script):
        p = os.path.join(self.fake_bin, "curl")
        with open(p, "w", encoding="utf-8") as f:
            f.write(script)
        os.chmod(p, 0o755)

    def test_D1_missing_args_exit1(self):
        """run.sh 缺参(无任何参数)→ 退出码 1(用法错)。"""
        r = self._run()
        self.assertEqual(r.returncode, 1)

    def test_D2_missing_taskbook_exit1(self):
        """--taskbook 指向不存在文件 → 退出码 1。"""
        r = self._run("--taskbook", os.path.join(self.tmp, "nope.md"),
                      "--workdir", self.tmp, "--out", os.path.join(self.tmp, "o"))
        self.assertEqual(r.returncode, 1)

    def test_D3_ollama_unreachable_exit2(self):
        """ollama 不可达(假 curl 失败)→ 退出码 2;result.json 落盘 status=failed。"""
        self._fake_curl("#!/usr/bin/env bash\n"
                        "echo 'curl: (7) Failed to connect' >&2\n"
                        "exit 7\n")
        out = os.path.join(self.tmp, "out")
        r = self._run("--taskbook", self.tb, "--workdir", self.tmp,
                      "--out", out)
        self.assertEqual(r.returncode, 2)
        with open(os.path.join(out, "result.json"), encoding="utf-8") as f:
            rec = json.load(f)
        self.assertEqual(rec["status"], "failed")
        self.assertEqual(rec["exit_code"], 2)

    def test_D4_success_exit0(self):
        """假 ollama 成功响应 → 退出码 0;result.md + result.json 字段齐备。"""
        self._fake_curl(
            "#!/usr/bin/env bash\n"
            "cat <<'JSON'\n"
            '{"model":"qwen3.5:9b","message":{"role":"assistant",'
            '"content":"\\u4f60\\u597d\\uff0c\\u56de\\u590d\\u3002\\n\\u7b2c\\u4e8c\\u884c\\u3002"},'
            '"prompt_eval_count":120,"eval_count":45}\n'
            "JSON\n")
        out = os.path.join(self.tmp, "out")
        r = self._run("--taskbook", self.tb, "--workdir", self.tmp, "--out", out)
        self.assertEqual(r.returncode, 0)
        with open(os.path.join(out, "result.md"), encoding="utf-8") as f:
            md = f.read()
        self.assertIn("你好，回复。", md)
        self.assertIn("第二行。", md)
        with open(os.path.join(out, "result.json"), encoding="utf-8") as f:
            rec = json.load(f)
        self.assertEqual(rec["status"], "ok")
        self.assertEqual(rec["exit_code"], 0)
        self.assertEqual(rec["model"], "qwen3.5:9b")
        self.assertEqual(rec["think"], True)
        self.assertEqual(rec["tokens"], 45)
        self.assertEqual(rec["taskbook"], "tb.md")
        self.assertGreaterEqual(rec["duration_s"], 0)
        # 契约占位:diff.patch 存在(local 不产出代码 diff)
        self.assertTrue(os.path.isfile(os.path.join(out, "diff.patch")))

    def test_D5_empty_reply_exit3(self):
        """模型回复为空 → 退出码 3(status=failed exit_code=3)。"""
        self._fake_curl(
            "#!/usr/bin/env bash\n"
            "cat <<'JSON'\n"
            '{"model":"qwen3.5:9b","message":{"role":"assistant","content":""},'
            '"prompt_eval_count":10,"eval_count":0}\n'
            "JSON\n")
        out = os.path.join(self.tmp, "out")
        r = self._run("--taskbook", self.tb, "--workdir", self.tmp, "--out", out)
        self.assertEqual(r.returncode, 3)
        with open(os.path.join(out, "result.json"), encoding="utf-8") as f:
            rec = json.load(f)
        self.assertEqual(rec["status"], "failed")
        self.assertEqual(rec["exit_code"], 3)

    def test_D6_think_invalid_exit1(self):
        """--think 非法值 → 退出码 1(fail-closed)。"""
        self._fake_curl("#!/usr/bin/env bash\nexit 0\n")
        r = self._run("--taskbook", self.tb, "--workdir", self.tmp,
                      "--out", os.path.join(self.tmp, "o"), "--think", "maybe")
        self.assertEqual(r.returncode, 1)

    def test_D7_tb_model_think_decl_used(self):
        """任务书 model/think 声明 → 请求体与 result.json 正确(假 curl 捕获请求体)。"""
        tb7 = os.path.join(self.tmp, "tb7.md")
        with open(tb7, "w", encoding="utf-8") as f:
            f.write("# 任务\n```flow\nmodel: qwen3-8b\nthink: false\n```\n正文\n")
        cap = os.path.join(self.tmp, "cap.json")
        self._fake_curl(
            "#!/usr/bin/env bash\n"
            "for a in \"$@\"; do\n"
            "  case \"$a\" in\n"
            "    -d@*) cp \"${a#-d@}\" \"${CAP_FILE:?}\" ;;\n"
            "  esac\n"
            "done\n"
            "cat <<'JSON'\n"
            '{"model":"qwen3:8b","message":{"role":"assistant","content":"ok"},'
            '"prompt_eval_count":5,"eval_count":3}\n'
            "JSON\n")
        out = os.path.join(self.tmp, "out")
        r = self._run("--taskbook", tb7, "--workdir", self.tmp, "--out", out,
                      env={"CAP_FILE": cap})
        self.assertEqual(r.returncode, 0)
        with open(cap, encoding="utf-8") as f:
            body = json.load(f)
        self.assertEqual(body["model"], "qwen3:8b")
        self.assertFalse(body["think"])  # think 为 ollama 顶层参数（2026-08-26 修复）
        self.assertEqual(body["options"]["num_predict"], 4096)
        self.assertEqual(body["options"]["num_ctx"], 8192)
        self.assertEqual(body["keep_alive"], "30m")
        with open(os.path.join(out, "result.json"), encoding="utf-8") as f:
            rec = json.load(f)
        self.assertEqual(rec["model"], "qwen3:8b")
        self.assertFalse(rec["think"])

    def test_D8_cli_model_overrides_tb(self):
        """--model CLI 优先于任务书声明(qwen3-8b → qwen35-9b 映射 qwen3.5:9b)。"""
        tb7 = os.path.join(self.tmp, "tb8.md")
        with open(tb7, "w", encoding="utf-8") as f:
            f.write("# 任务\n```flow\nmodel: qwen3-8b\n```\n")
        cap = os.path.join(self.tmp, "cap8.json")
        self._fake_curl(
            "#!/usr/bin/env bash\n"
            "for a in \"$@\"; do\n"
            "  case \"$a\" in\n"
            "    -d@*) cp \"${a#-d@}\" \"${CAP_FILE:?}\" ;;\n"
            "  esac\n"
            "done\n"
            "cat <<'JSON'\n"
            '{"model":"qwen3.5:9b","message":{"role":"assistant","content":"ok"},'
            '"prompt_eval_count":1,"eval_count":1}\n'
            "JSON\n")
        out = os.path.join(self.tmp, "out")
        r = self._run("--taskbook", tb7, "--workdir", self.tmp, "--out", out,
                      "--model", "qwen35-9b", env={"CAP_FILE": cap})
        self.assertEqual(r.returncode, 0)
        with open(cap, encoding="utf-8") as f:
            body = json.load(f)
        self.assertEqual(body["model"], "qwen3.5:9b")


if __name__ == "__main__":
    unittest.main()
