#!/usr/bin/env python3
"""executor-size-gating W-S1 单测(design .flow/workitems/executor-size-gating/design.md §7)。

用例矩阵:A size 解析(声明/估算/非法) / B 映射表(含兜底) / C --force 门禁 /
D 命令串固化幂等 + 队列 expected-seconds 地板。
零 API、全程临时目录;直接 importlib 加载 flow-core.py(沿用 test-flow-core.py 模式)。
用法: python3 -m unittest discover tests
"""

import importlib.util
import os
import shlex
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock

_HERE = os.path.dirname(os.path.abspath(__file__))
_FLOW_CORE = os.path.join(_HERE, "..", "scripts", "flow-core.py")

_spec = importlib.util.spec_from_file_location("flow_core_sg", _FLOW_CORE)
fc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fc)

_ENV_PREFIXES = ("FLOW_", "COLLABFLOW_", "CHAIN_", "STUB_", "HOME", "DSH_", "DEEPSEEK")


def _status(wi_id="w1", state="translated"):
    return {
        "schema_version": 1, "id": wi_id, "state": state, "iteration": 0,
        "same_defect_count": 0, "primary_defect_type": None, "takeover": False,
        "re_execute_count": 0, "process_version": "1.0.0",
        "created_at": "2026-08-14T10:00:00+00:00",
        "updated_at": "2026-08-14T10:00:00+00:00",
        "event_seq": 1, "locked_by": None, "lock_expires_at": None,
    }


def _cfg(**kw):
    """最小 cfg;executor.size/size_estimate 段默认缺省(测兜底),可经 kw 覆盖。"""
    cfg = {
        "workitem": {"plane_id": "control", "lock_ttl_s": 3600, "id_max_len": 64},
        "gates": {"takeover_after_same_defect": 2, "iteration_limit": 3},
        "executor": {"default": "reasonix", "timeout_s": 1800, "diff_scope": None},
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
    cfg["executor"].update(kw)
    return cfg


def _flow_block(size=None):
    """taskbook ```flow 块;size 非 None 时注入 size 声明。"""
    lines = ["test_command: pytest"]
    if size is not None:
        lines.append(f"size: {size}")
    return "\n".join(lines) + "\n"


class SizeGatingTests(unittest.TestCase):
    """W-S1 §7:A size 解析 / B 映射 / C 门禁 / D 命令串与队列地板。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="flow-size-")
        self.workdir = os.path.join(self.tmp, "work")
        os.makedirs(self.workdir, exist_ok=True)
        self._saved = {k: v for k, v in os.environ.items()
                       if k.startswith(_ENV_PREFIXES)}
        for k in list(self._saved):
            os.environ.pop(k, None)
        os.environ["FLOW_DATA_DIR"] = os.path.join(self.tmp, ".flow")
        os.environ["FLOW_WORKDIR"] = self.workdir
        os.environ["FLOW_TASK_DIR"] = os.path.join(self.tmp, "tasks")
        os.environ["COLLABFLOW_CONFIG"] = os.path.join(self.tmp, "no-cfg.yaml")
        os.environ["HOME"] = os.path.join(self.tmp, "home")
        os.makedirs(os.environ["HOME"], exist_ok=True)
        os.makedirs(os.environ["FLOW_TASK_DIR"], exist_ok=True)
        self._orig_flow_bin = fc._flow_bin

    def tearDown(self):
        fc._flow_bin = self._orig_flow_bin
        for k in [k for k in list(os.environ) if k.startswith(_ENV_PREFIXES)]:
            os.environ.pop(k, None)
        os.environ.update(self._saved)
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ── helpers ──────────────────────────────────────────────────────────

    def _mk_wi(self, wi_id="w1", state="translated", size=None, design_bytes=None,
               changed_files=None):
        """建 workitem:status + taskbook(可带 size 声明) + design.md(可选,估算用)。"""
        wi_dir = os.path.join(fc.workitems_dir(), wi_id)
        os.makedirs(wi_dir, exist_ok=True)
        fc.save_status_atomic(wi_dir, _status(wi_id, state))
        with open(os.path.join(wi_dir, "taskbook.md"), "w", encoding="utf-8") as f:
            f.write("# 任务书\n\n```flow\n" + _flow_block(size) + "```\n")
        if design_bytes is not None:
            with open(os.path.join(wi_dir, "design.md"), "wb") as f:
                f.write(b"#" * design_bytes)
        elif changed_files is not None:
            lines = ["# 方案\n", "```diff_scope\n", "allow:\n"]
            for i in range(changed_files):
                lines.append(f"  - file{i}.py\n")
            lines.append("```\n")
            with open(os.path.join(wi_dir, "design.md"), "w", encoding="utf-8") as f:
                f.write("".join(lines))
        return wi_dir

    def _stub_flow(self):
        p = os.path.join(self.tmp, "stub-flow")
        cap = os.path.join(self.tmp, "flow-add.cap")
        with open(p, "w", encoding="utf-8") as f:
            f.write(f"#!/usr/bin/env bash\nprintf '%s\\n' '---' >> {cap}\n"
                    f"printf '%s\\n' \"$@\" >> {cap}\n"
                    f"echo '{{\"status\":\"ok\",\"id\":\"t-stub\",\"expected_seconds\":2515}}'\n")
        os.chmod(p, 0o755)
        return p, cap

    # ── A. size 解析 ─────────────────────────────────────────────────────

    def test_size_declared_tiers(self):
        """声明 small/medium/large → 对应 size,source=declared(§7 A 组)。"""
        for tier in fc.SIZE_TIERS:
            with self.subTest(tier=tier):
                wi_dir = self._mk_wi(f"w-{tier}", size=tier)
                r = fc.resolve_size(wi_dir, _cfg())
                self.assertEqual(r["size"], tier)
                self.assertEqual(r["source"], "declared")

    def test_size_illegal_value(self):
        """```flow size: huge → GateReject(invalid_size) fail-closed(§7 E1)。"""
        wi_dir = self._mk_wi("w1", size="huge")
        with self.assertRaises(fc.GateReject) as cm:
            fc.resolve_size(wi_dir, _cfg())
        self.assertEqual(cm.exception.args[0], "invalid_size")

    def test_size_estimate_large_by_bytes(self):
        """无 size 声明;design.md 30721 字节(>30KB)→ large,estimated(§7 E3)。"""
        wi_dir = self._mk_wi("w1", design_bytes=30721)
        r = fc.resolve_size(wi_dir, _cfg())
        self.assertEqual(r["size"], "large")
        self.assertEqual(r["source"], "estimated")
        self.assertEqual(r["design_bytes"], 30721)

    def test_size_estimate_large_by_files(self):
        """无 size 声明;diff_scope.allow 6 文件(>5)→ large(§7)。"""
        wi_dir = self._mk_wi("w1", changed_files=6)
        r = fc.resolve_size(wi_dir, _cfg())
        self.assertEqual(r["size"], "large")
        self.assertEqual(r["changed_files"], 6)

    def test_size_estimate_medium_boundary(self):
        """恰好 30720 字节 + 5 文件 → medium(严格大于,边界不升,§7 D5)。"""
        wi_dir = self._mk_wi("w1")
        block = ("```diff_scope\nallow:\n  - f0.py\n  - f1.py\n"
                 "  - f2.py\n  - f3.py\n  - f4.py\n```\n")
        # ocr F2:二进制模式写入,避免 Windows 文本模式 \n → \r\n 膨胀使 30720 字节
        # 边界断言失败(误判 large);block 全 ASCII,字符数 == 编码字节数
        # ocr8-L2:fence 前补换行,使 ```diff_scope 位于行首(合理 Markdown),
        # 不依赖 extract_change_list 的非锚定 re.search;总字节仍 30720。
        payload = ("#" * (30720 - len(block) - 1) + "\n" + block).encode("utf-8")
        with open(os.path.join(wi_dir, "design.md"), "wb") as f:
            f.write(payload)
        self.assertEqual(os.path.getsize(os.path.join(wi_dir, "design.md")), 30720)
        r = fc.resolve_size(wi_dir, _cfg())
        self.assertEqual(r["size"], "medium")
        self.assertEqual(r["changed_files"], 5)

    def test_size_estimate_medium_empty(self):
        """无 size 声明且无 design.md → medium,不抛(§7 E2)。"""
        wi_dir = self._mk_wi("w1")
        r = fc.resolve_size(wi_dir, _cfg())
        self.assertEqual(r["size"], "medium")
        self.assertEqual(r["design_bytes"], 0)
        self.assertEqual(r["changed_files"], 0)

    # ── B. 映射表 ────────────────────────────────────────────────────────

    def test_mapping_table(self):
        """small→(flash,1200) / medium→(flash,1800) / large→(pro,2400)(§7 B 组)。"""
        expected = {"small": ("deepseek-v4-flash", 1200),
                    "medium": ("deepseek-v4-flash", 1800),
                    "large": ("deepseek-v4-pro", 2400)}
        for tier, (model, tmo) in expected.items():
            with self.subTest(tier=tier):
                p = fc.executor_params_for(tier, _cfg())
                self.assertEqual((p["model"], p["timeout_s"]), (model, tmo))

    def test_mapping_table_fallback(self):
        """cfg 缺 executor.size → 硬编码兜底;timeout_s 非法/≤0/空 model → 兜底(§7 E4/E5)。"""
        for tier in fc.SIZE_TIERS:
            with self.subTest(tier=tier):
                fallback = fc.SIZE_DEFAULTS[tier]
                # cfg 无 executor 段
                self.assertEqual(fc.executor_params_for(tier, {}),
                                 fallback)
                # timeout_s 非正整数 → 兜底 timeout(model 同 fallback 才能整体相等)
                for bad in ("abc", 0, -5, None):
                    p = fc.executor_params_for(tier, _cfg(size={
                        tier: {"model": fallback["model"], "timeout_s": bad}}))
                    self.assertEqual(p, fallback)
                # model 空 → 兜底 model
                p = fc.executor_params_for(tier, _cfg(size={
                    tier: {"model": "", "timeout_s": 999}}))
                self.assertEqual(p["model"], fallback["model"])

    # ── C. --force 门禁 ──────────────────────────────────────────────────

    def test_gate_large_model_flash_rejected(self):
        """large + --model flash(无 force)→ GateReject(size_gate)(§7 E6)。"""
        wi_dir = self._mk_wi("w1", size="large")
        with self.assertRaises(fc.GateReject) as cm:
            fc.resolve_execute_params(wi_dir, _cfg(), cli_model="deepseek-v4-flash")
        self.assertEqual(cm.exception.args[0], "size_gate")

    def test_gate_large_timeout_rejected(self):
        """large + --timeout 1800(<2400)→ size_gate;非整数 → UsageError(§7 E7/E9)。"""
        wi_dir = self._mk_wi("w1", size="large")
        with self.assertRaises(fc.GateReject) as cm:
            fc.resolve_execute_params(wi_dir, _cfg(), cli_timeout="1800")
        self.assertEqual(cm.exception.args[0], "size_gate")
        with self.assertRaises(fc.UsageError):
            fc.resolve_execute_params(wi_dir, _cfg(), cli_timeout="abc")
        with self.assertRaises(fc.UsageError):
            fc.resolve_execute_params(wi_dir, _cfg(), cli_timeout="0")

    def test_gate_large_force_requires_reason(self):
        """large 降级 + --force:无/空白 reason → force_reason_required;非空 → 通过(§7 E8)。"""
        wi_dir = self._mk_wi("w1", size="large")
        for reason in (None, "", "   "):
            with self.subTest(reason=reason):
                with self.assertRaises(fc.GateReject) as cm:
                    fc.resolve_execute_params(wi_dir, _cfg(),
                                              cli_model="deepseek-v4-flash",
                                              force=True, force_reason=reason)
                self.assertEqual(cm.exception.args[0], "force_reason_required")
        p = fc.resolve_execute_params(wi_dir, _cfg(), cli_model="deepseek-v4-flash",
                                      force=True, force_reason="人工确认小改动")
        # 仅降 model:timeout 保持 base 2400
        self.assertEqual((p["model"], p["timeout_s"]), ("deepseek-v4-flash", 2400))
        # 同时降 timeout → 完整降级配置
        p2 = fc.resolve_execute_params(wi_dir, _cfg(), cli_model="deepseek-v4-flash",
                                       cli_timeout="1800", force=True,
                                       force_reason="人工确认小改动")
        self.assertEqual((p2["model"], p2["timeout_s"]), ("deepseek-v4-flash", 1800))

    def test_gate_large_defaults_ok(self):
        """large 无显式 model/timeout → pro+2400 不抛;--model 空串 → UsageError(§7 E10)。"""
        wi_dir = self._mk_wi("w1", size="large")
        p = fc.resolve_execute_params(wi_dir, _cfg())
        self.assertEqual((p["model"], p["timeout_s"]), ("deepseek-v4-pro", 2400))
        self.assertEqual(p["size"], "large")
        # 显式值等于 base → 不触发降级(幂等)
        p2 = fc.resolve_execute_params(wi_dir, _cfg(), cli_model="deepseek-v4-pro",
                                       cli_timeout="2400")
        self.assertEqual((p2["model"], p2["timeout_s"]), ("deepseek-v4-pro", 2400))
        with self.assertRaises(fc.UsageError):
            fc.resolve_execute_params(wi_dir, _cfg(), cli_model="")

    # ── D. 命令串固化 + 队列地板 ─────────────────────────────────────────

    def test_enqueue_command_roundtrip(self):
        """large → build_execute_command 固化 pro/2400;force 降级含 --force-reason;
        命令串重解析 → 门禁幂等通过(§7 R1/E11)。
        ocr8-H1:size 判定固化进命令串——sync 重跑路径(命令串经 task add 重执行)
        携带 --size,子进程跳过 resolve_size 重算,与 async 预解析路径同值。"""
        wi_dir = self._mk_wi("w1", size="large")
        cfg = _cfg()
        cmd = fc.build_execute_command(wi_dir, cfg)
        self.assertIn("workitem execute w1 --sync", cmd)
        self.assertIn("--size large", cmd)
        self.assertIn("--timeout 2400", cmd)
        self.assertIn("--model deepseek-v4-pro", cmd)
        # 幂等:固化值(含 --size)重解析 → 同值不触发降级
        argv = shlex.split(cmd)
        self.assertEqual(argv[argv.index("--size") + 1], "large")
        p = fc.resolve_execute_params(
            wi_dir, cfg, cli_model=argv[argv.index("--model") + 1],
            cli_timeout=argv[argv.index("--timeout") + 1],
            cli_size=argv[argv.index("--size") + 1])
        self.assertEqual((p["model"], p["timeout_s"], p["size"]),
                         ("deepseek-v4-pro", 2400, "large"))

        # force 降级:多词 reason 被 shlex.quote 成单 token
        cmd2 = fc.build_execute_command(wi_dir, cfg, force=True,
                                        force_reason="debug downgrade 降级")
        self.assertIn("--force", cmd2)
        self.assertIn("--force-reason", cmd2)
        argv2 = shlex.split(cmd2)
        reason = argv2[argv2.index("--force-reason") + 1]
        self.assertEqual(reason, "debug downgrade 降级")
        # 重解析(固化值 pro/2400 + force + reason)→ 门禁通过,幂等
        p2 = fc.resolve_execute_params(
            wi_dir, cfg, cli_model=argv2[argv2.index("--model") + 1],
            cli_timeout=argv2[argv2.index("--timeout") + 1],
            force=True, force_reason=reason)
        self.assertEqual((p2["model"], p2["timeout_s"]), ("deepseek-v4-pro", 2400))
        # 命令串二跳若带显式降级参数:force+reason 让门禁通过
        p3 = fc.resolve_execute_params(wi_dir, cfg, cli_model="deepseek-v4-flash",
                                       cli_timeout="1800", force=True,
                                       force_reason=reason)
        self.assertEqual((p3["model"], p3["timeout_s"]), ("deepseek-v4-flash", 1800))

    def test_enqueue_expected_seconds_floor(self):
        """large 自动入队 → expected-seconds ≥ 2400+115(§7 R2/E12)。"""
        stub, cap = self._stub_flow()
        fc._flow_bin = lambda: stub
        wi_dir = self._mk_wi("w1", size="large")
        fc._hook_auto_enqueue(wi_dir, _cfg(), False)
        args = fc.read_file(cap).splitlines()
        self.assertIn("--expected-seconds", args)
        exp = int(args[args.index("--expected-seconds") + 1])
        self.assertGreaterEqual(exp, 2400 + 115)

    def test_resolve_params_cli_size_pins(self):
        """ocr7-M6:cli_size 显式固定档位(跳过 resolve_size 重算),非法值 fail-closed。"""
        wi_dir = self._mk_wi("w1", size="large")       # 声明 large
        p = fc.resolve_execute_params(wi_dir, _cfg(), cli_size="medium")
        self.assertEqual(p["size"], "medium")          # 不重算,档位=显式值
        self.assertEqual(p["source"], "declared")      # 显式提供 = 声明语义
        # 档位变了 → base model/timeout 跟着 medium(1800)
        self.assertEqual(p["timeout_s"], 1800)
        with self.assertRaises(fc.GateReject) as cm:
            fc.resolve_execute_params(wi_dir, _cfg(), cli_size="huge")
        self.assertEqual(cm.exception.args[0], "invalid_size")

    def test_async_force_reason_whitespace_rejected(self):
        """ocr7-M4:async 入队命令串白名单(FLOW_WORKITEM_RE)每选项只吃一个无空白
        token,多词 --force-reason 会静默入队失败——含空白时显式拒绝(exit 2,不入队)。"""
        if shutil.which("git") is None:
            self.skipTest("git 不可用")
        subprocess.run(["git", "init", "-q", self.workdir], check=True)
        subprocess.run(["git", "-C", self.workdir, "config", "user.email", "t@t"],
                       check=True)
        subprocess.run(["git", "-C", self.workdir, "config", "user.name", "t"],
                       check=True)
        with open(os.path.join(self.workdir, "README.md"), "w", encoding="utf-8") as f:
            f.write("# init\n")
        subprocess.run(["git", "-C", self.workdir, "add", "README.md"], check=True)
        subprocess.run(["git", "-C", self.workdir, "commit", "-qm", "init"], check=True)
        self._mk_wi("w1", size="large")              # 降级需 --force --force-reason
        with mock.patch.object(fc, "_enqueue_workitem_op") as me:
            code = fc.cmd_execute(
                ["w1", "--executor", "stub", "--model", "deepseek-v4-flash",
                 "--force", "--force-reason", "debug downgrade 降级"], _cfg())
        self.assertEqual(code, 2)                    # 显式拒绝,不静默入队
        me.assert_not_called()
        # 无空白 reason 正常入队
        with mock.patch.object(fc, "_enqueue_workitem_op", return_value=0) as me2:
            code2 = fc.cmd_execute(
                ["w1", "--executor", "stub", "--model", "deepseek-v4-flash",
                 "--force", "--force-reason", "人工确认降级"], _cfg())
        self.assertEqual(code2, 0)
        me2.assert_called_once()

    def test_async_enqueue_pins_resolved_size(self):
        """ocr7-M6:async-first 入队命令串固化预解析 --size,子进程重执行不再
        resolve_size 重算——design.md 增长/改动清单变化不使档位漂移(否则 medium
        预估在入队后升 large,固化 flash/1800 会误触发 size_gate)。"""
        if shutil.which("git") is None:
            self.skipTest("git 不可用")
        subprocess.run(["git", "init", "-q", self.workdir], check=True)
        subprocess.run(["git", "-C", self.workdir, "config", "user.email", "t@t"],
                       check=True)
        subprocess.run(["git", "-C", self.workdir, "config", "user.name", "t"],
                       check=True)
        with open(os.path.join(self.workdir, "README.md"), "w", encoding="utf-8") as f:
            f.write("# init\n")
        subprocess.run(["git", "-C", self.workdir, "add", "README.md"], check=True)
        subprocess.run(["git", "-C", self.workdir, "commit", "-qm", "init"], check=True)
        wi_dir = self._mk_wi("w1", design_bytes=30000)  # 无 size 声明 → 估算 medium
        captured = {}

        def _fake_enqueue(cfg, sub, wi_id, inner_argv, kind, json_mode,
                          state_before, timeout=None):
            captured["argv"] = list(inner_argv)
            return 0

        with mock.patch.object(fc, "_enqueue_workitem_op", side_effect=_fake_enqueue):
            code = fc.cmd_execute(["w1", "--executor", "stub"], _cfg())
        self.assertEqual(code, 0)
        argv = captured["argv"]
        self.assertEqual(argv[argv.index("--size") + 1], "medium")
        self.assertIn("--timeout", argv)
        # 入队后 design.md 增长(>30KB):无 cli_size 重算会升 large——固化 --size 阻止漂移
        with open(os.path.join(wi_dir, "design.md"), "wb") as f:
            f.write(b"#" * 40000)
        self.assertEqual(fc.resolve_size(wi_dir, _cfg())["size"], "large")  # 对照:重算会升档
        p = fc.resolve_execute_params(  # 模拟子进程 re-exec(带固化值)
            wi_dir, _cfg(),
            cli_model=argv[argv.index("--model") + 1],
            cli_timeout=argv[argv.index("--timeout") + 1],
            cli_size=argv[argv.index("--size") + 1])
        self.assertEqual(p["size"], "medium")          # 与预解析一致,不漂移


if __name__ == "__main__":
    unittest.main(verbosity=2)
