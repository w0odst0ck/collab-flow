#!/usr/bin/env python3
"""flow-task-core.py W-S2 单测(executor-timeout-recovery §4):抢救判定(4) +
自动抢救(4) + 风暴防护(2) + audit(1) + token 预警(3) + 非 execute 不触发/边界(3)
+ runner 集成接线(1)。

用法: python3 -m unittest discover tests(合法模块名,discover 直接发现,无需桥接)
零 API、全程 FLOW_TASK_DIR/FLOW_DATA_DIR/FLOW_WORKDIR/HOME 临时目录隔离;
workitem fixture(status.yaml + taskbook.md + design.md)+ 临时 git 仓(git diff 未提交改动)
+ stub notify(cat 追加 rec 文件);不触真实 ~/.collabflow/~/.reasonix/项目仓。
"""

import contextlib
import importlib.util
import io
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock

_HERE = os.path.dirname(os.path.abspath(__file__))
_TASK_CORE = os.path.join(_HERE, "..", "scripts", "flow-task-core.py")

_spec = importlib.util.spec_from_file_location("flow_task_rescue_core", _TASK_CORE)
tc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tc)

_ENV_PREFIXES = ("FLOW_", "COLLABFLOW_", "STUB_", "DSH_", "DEEPSEEK", "RX_")


def _git_available():
    """一次性探测 git 可用性(ocr F5):which 命中且 `git --version` 可执行 → True;
    缺失/不可执行/超时 → False(调用方 skipTest 优雅降级,而非 CalledProcessError 崩溃)。"""
    if shutil.which("git") is None:
        return False
    try:
        proc = subprocess.run(["git", "--version"], capture_output=True, text=True,
                              timeout=30, check=False)
        return proc.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def _cfg(**task_over):
    """最小 task/host/chain 配置(与既有测试同形)。"""
    task = {"schema_version": 1, "max_parallel": 2, "log_tail_bytes": 2000,
            "kill_grace_s": 1, "expected_seconds_seed": {"design": 480, "execute": 1800},
            "seed_history_len": 8, "queue_cap": 50}
    task.update(task_over)
    return {"task": task, "host": {"notify": None, "wake_template": None},
            "chain": {"enabled": True}}


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


class RescueIsoBase(unittest.TestCase):
    """FLOW_TASK_DIR + FLOW_DATA_DIR + FLOW_WORKDIR + HOME 临时目录隔离。"""

    def setUp(self):
        self._saved = {k: v for k, v in os.environ.items() if k.startswith(_ENV_PREFIXES)}
        for k in list(self._saved):
            os.environ.pop(k, None)
        self._home_old = os.environ.get("HOME")
        self.tmp = tempfile.mkdtemp(prefix="flow-rescue-")
        self.task_dir = os.path.join(self.tmp, "task")
        self.data = os.path.join(self.tmp, ".flow")
        self.home = os.path.join(self.tmp, "home")
        self.repo = os.path.join(self.tmp, "repo")
        os.environ["FLOW_TASK_DIR"] = self.task_dir
        os.environ["FLOW_DATA_DIR"] = self.data
        os.environ["HOME"] = self.home
        os.environ["FLOW_WORKDIR"] = self.repo
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

    # ── fixture ────────────────────────────────────────────────────────────

    def _mk_git_repo(self, modified_files=("scripts/demo.py",)):
        """临时 git 仓:baseline commit(含各目标文件) + 未提交 unstaged 修改。

        用「修改既有文件不 git add」而非新建文件:真实执行器留下的改动是
        unstaged modified,git diff HEAD 与 git diff --name-only 口径一致,
        diff.patch 非空且 changed_files 正确(与 executors/*/wrapper.sh 同口径)。"""
        if not _git_available():
            self.skipTest("git 不可用,跳过依赖 git 仓的测试")  # ocr F5:缺失时优雅降级
        os.makedirs(self.repo, exist_ok=True)
        subprocess.run(["git", "-C", self.repo, "init", "-q"], check=True)
        subprocess.run(["git", "-C", self.repo, "config", "user.email", "t@t"],
                       check=True)
        subprocess.run(["git", "-C", self.repo, "config", "user.name", "t"], check=True)
        os.makedirs(os.path.join(self.repo, "scripts"), exist_ok=True)
        with open(os.path.join(self.repo, "baseline.txt"), "w", encoding="utf-8") as f:
            f.write("base\n")
        for rel in modified_files:
            with open(os.path.join(self.repo, rel), "w", encoding="utf-8") as f:
                f.write(f"# {rel} baseline\n")
        subprocess.run(["git", "-C", self.repo, "add", "."], check=True)
        subprocess.run(["git", "-C", self.repo, "commit", "-qm", "base"], check=True)
        for rel in modified_files:                    # 未提交 unstaged 修改
            with open(os.path.join(self.repo, rel), "w", encoding="utf-8") as f:
                f.write(f"# {rel} changed\n")

    def _mk_wi(self, wi_id, test_command=None, scope_allow=None, error_table=True,
               state="executed"):
        """构造 workitem(status.yaml + taskbook.md ```flow 块 + design.md 错误表)。
        test_command=None → flow 块无 test_command(走惯例探测/command_unresolved);
        scope_allow=None → 无 diff_scope(scope_undeclared → verify gate fail)。"""
        d = os.path.join(self.data, "workitems", wi_id)
        os.makedirs(os.path.join(d, "executor"), exist_ok=True)
        with open(os.path.join(d, "status.yaml"), "w", encoding="utf-8") as f:
            f.write(f"state: {state}\n")
        lines = []
        if test_command is not None:
            lines.append(f"test_command: \"{test_command}\"")
        if scope_allow is not None:
            allow = "\n".join(f"    - {x}" for x in scope_allow)
            lines.append("diff_scope:\n  allow:\n" + allow)
        with open(os.path.join(d, "taskbook.md"), "w", encoding="utf-8") as f:
            f.write("# 任务书\n\n```flow\n" + "\n".join(lines) + "\n```\n")
        if error_table:
            with open(os.path.join(d, "design.md"), "w", encoding="utf-8") as f:
                f.write("# 设计\n\n| E1 | 场景 | 处理方式 | 测试覆盖 |\n")
        return d

    def _seed_task(self, tid, **over):
        """注册表 seed 一条 execute 终态任务(默认 timeout/124;测试可覆盖)。"""
        entry = {"id": tid, "workitem": "rescue-w1", "command": "sleep 0.01",
                 "priority": "P2", "state": "timeout", "kind": "execute",
                 "expected_seconds": 30, "kill_on_timeout": False, "workdir": self.repo,
                 "scheduled_at": None, "why": "test", "cost_usd": None, "audit": None,
                 "created_at": "2026-08-24T10:00:00+00:00",
                 "started_at": "2026-08-24T10:00:00+00:00",
                 "finished_at": "2026-08-24T10:01:00+00:00",
                 "exit_code": 124, "failure_tail": None, "pid": None, "heartbeat_at": None}
        entry.update(over)
        reg = tc.load_registry()
        reg["tasks"][tid] = entry
        tc.save_registry_atomic(reg)
        return entry

    def _stub_notify(self, cfg, rec_path):
        """host.notify stub:stdin JSON 追加写 rec_path(每行一条,可断言多次)。"""
        stub = os.path.join(self.tmp, "notify.sh")
        with open(stub, "w", encoding="utf-8") as f:
            f.write("#!/usr/bin/env bash\ncat >> \"$1\"\n")
        os.chmod(stub, 0o755)
        cfg["host"]["notify"] = f"{stub} {rec_path}"
        return cfg

    def _notify_recs(self, rec_path):
        if not os.path.isfile(rec_path):
            return []
        with open(rec_path, encoding="utf-8") as f:
            return [json.loads(ln) for ln in f.read().splitlines() if ln.strip()]

    def _audit_all(self):
        p = os.path.join(tc.events_dir(), "audit.jsonl")
        if not os.path.isfile(p):
            return []
        with open(p, encoding="utf-8") as f:
            return [json.loads(ln) for ln in f.read().splitlines() if ln.strip()]

    def _audit_tail(self):
        recs = self._audit_all()
        return recs[-1] if recs else None

    def _status_text(self, wi_id):
        with open(os.path.join(self.data, "workitems", wi_id, "status.yaml"),
                  encoding="utf-8") as f:
            return f.read()

    def _wi_dir(self, wi_id):
        return os.path.join(self.data, "workitems", wi_id)


# ---------------------------------------------------------------------------
# §4.1 纯函数判定(4,零 I/O)
# ---------------------------------------------------------------------------

class RescueDecisionTests(unittest.TestCase):
    """rescue_decision 纯函数四象限(§2.2 决策表)。"""

    def test_rescue_decision_rescue(self):
        self.assertEqual(tc.rescue_decision(None, True), "rescue")

    def test_rescue_decision_requeue(self):
        self.assertEqual(tc.rescue_decision(None, False), "requeue")

    def test_rescue_decision_skip_ok(self):
        # 有完整产物 → 异常终态不补写不重跑(测试结果忽略)
        self.assertEqual(tc.rescue_decision("ok", True), "skip_ok")
        self.assertEqual(tc.rescue_decision("ok", False), "skip_ok")

    def test_rescue_decision_skip_partial(self):
        # 已有半成品 → 交 rx --continue,不重跑(不丢半成品)
        self.assertEqual(tc.rescue_decision("partial-complete", False), "skip_partial")
        self.assertEqual(tc.rescue_decision("partial-complete", True), "skip_partial")


# ---------------------------------------------------------------------------
# §4.2 自动抢救(4)
# ---------------------------------------------------------------------------

class RescueHookTests(RescueIsoBase):
    """冒烟①:测试绿 + result.json 缺失 → 补产物 + verify 通过 + 可 accept 报告。"""

    def test_rescue_completes_missing_result(self):
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-w1"
        self._mk_wi(wi, test_command="true", scope_allow=["scripts/demo.py"])
        rec_path = os.path.join(self.tmp, "notify.jsonl")
        cfg = self._stub_notify(_cfg(), rec_path)
        t = self._seed_task("t-000000000001", workitem=wi, state="timeout", exit_code=124)
        self.assertEqual(tc.run_rescue_hook(t, cfg, "timeout", 124), "rescued")
        # 补 result.json(结构同形 + 来源标注)
        with open(os.path.join(self._wi_dir(wi), "executor", "result.json"),
                  encoding="utf-8") as f:
            r = json.load(f)
        self.assertEqual(r["status"], "ok")
        self.assertIs(r["rescued"], True)
        self.assertEqual(r["note"], "自动抢救，原始 timeout")
        self.assertEqual(r["original_state"], "timeout")
        self.assertEqual(r["original_exit_code"], 124)
        self.assertEqual(r["executor"], "rescue")
        self.assertEqual(r["test_command"], "true")
        # diff.patch 非空(git diff HEAD 未提交改动)
        with open(os.path.join(self._wi_dir(wi), "executor", "diff.patch"),
                  encoding="utf-8") as f:
            self.assertIn("scripts/demo.py", f.read())
        # verify 通过(与正常 execute 同一质量门)
        with open(os.path.join(self._wi_dir(wi), "executor", "verify.json"),
                  encoding="utf-8") as f:
            v = json.load(f)
        self.assertIs(v["tests_pass"], True)
        self.assertIs(v["diff_match"], True)
        self.assertIs(v["error_table_match"], True)
        self.assertTrue(os.path.isfile(os.path.join(self._wi_dir(wi), "verify.md")))
        # 推送「可 accept」报告(绝不自动 accept)
        recs = self._notify_recs(rec_path)
        self.assertEqual(recs[-1]["kind"], "rescue_accept_pending")
        self.assertEqual(recs[-1]["workitem"], wi)
        self.assertIn("summary", recs[-1])
        # audit 落账
        tail = self._audit_tail()
        self.assertEqual(tail["stream"], "rescue")
        self.assertEqual(tail["action"], "rescue_success")

    def test_rescue_read_decision_inside_lock(self):
        """ocr9b-M2:读判定(assess_execute_completion)在 with_workitem_lock 临界区内
        执行——与 docstring 锁范围描述一致(读判定与写产物/verify 各持一次锁)。"""
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-m2"
        self._mk_wi(wi, test_command="true", scope_allow=["scripts/demo.py"])
        cfg = _cfg()
        t = self._seed_task("t-m2-0000000001", workitem=wi, state="timeout", exit_code=124)
        real_lock = tc._fc.with_workitem_lock
        in_lock = []
        assess_locked = []

        def wrap_lock(wi_dir, fn):
            in_lock.append(True)
            try:
                return real_lock(wi_dir, fn)
            finally:
                in_lock.pop()

        orig_assess = tc.assess_execute_completion

        def spy_assess(wi_dir, full_cfg):
            assess_locked.append(bool(in_lock))
            return orig_assess(wi_dir, full_cfg)

        with mock.patch.object(tc._fc, "with_workitem_lock", side_effect=wrap_lock), \
             mock.patch.object(tc, "assess_execute_completion", side_effect=spy_assess):
            self.assertEqual(tc.run_rescue_hook(t, cfg, "timeout", 124), "rescued")
        self.assertEqual(assess_locked, [True])       # 读判定持锁执行,非锁外调用

    def test_rescue_verify_fail_no_accept(self):
        # 测试绿但 diff_scope 未声明 → gate fail → 不推送 accept、产物保留、计数不误重置
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-w2"
        self._mk_wi(wi, test_command="true")          # 无 diff_scope → scope_undeclared
        wi_dir = self._wi_dir(wi)
        with open(os.path.join(wi_dir, "status.yaml"), "w", encoding="utf-8") as f:
            f.write("state: executed\nrescue_fail_count: 2\n")
        rec_path = os.path.join(self.tmp, "notify.jsonl")
        cfg = self._stub_notify(_cfg(), rec_path)
        t = self._seed_task("t-000000000002", workitem=wi, state="failed", exit_code=3)
        self.assertEqual(tc.run_rescue_hook(t, cfg, "failed", 3), "verify_fail")
        # result.json/diff.patch 已保留(不因 verify 失败回滚)
        self.assertTrue(os.path.isfile(os.path.join(wi_dir, "executor", "result.json")))
        self.assertTrue(os.path.isfile(os.path.join(wi_dir, "executor", "diff.patch")))
        # 不推送 accept,通知 verify_fail
        recs = self._notify_recs(rec_path)
        self.assertEqual(recs[-1]["kind"], "rescue_verify_fail")
        # ocr7-M3:gate 三项来自 verify 子字典(顶层 *_match + details.reason),
        # 不再全空错位——测试绿(true)+ diff scope 未声明 → diff match=False
        gate = recs[-1]["gate"]
        self.assertIs(gate["tests"]["pass"], True)
        self.assertIs(gate["diff"]["match"], False)
        self.assertEqual(gate["diff"]["reason"], "scope_undeclared")
        self.assertIn("tests", gate)
        self.assertIn("error_table", gate)
        # rescue_fail_count 不误重置(仍为 2)
        self.assertIn("rescue_fail_count: 2", self._status_text(wi))
        self.assertEqual(self._audit_tail()["action"], "rescue_verify_fail")

    def test_rescue_tests_red_requeue(self):
        # 冒烟②:测试红 + result.json 缺失 → 自动重入队 execute(下一空闲窗口)
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-w3"
        self._mk_wi(wi, test_command="false")         # 红
        cfg = _cfg()
        t = self._seed_task("t-000000000003", workitem=wi, state="failed", exit_code=3)
        self.assertEqual(tc.run_rescue_hook(t, cfg, "failed", 3), "requeued")
        reg = tc.load_registry()
        requeued = [x for x in reg["tasks"].values()
                    if x.get("workitem") == wi and x["id"] != "t-000000000003"]
        self.assertEqual(len(requeued), 1)
        self.assertEqual(requeued[0]["kind"], "execute")
        self.assertEqual(requeued[0]["state"], "scheduled")
        self.assertIsNotNone(requeued[0]["scheduled_at"])
        self.assertIn("rescue requeue", requeued[0]["why"])
        # 风暴计数 +1
        self.assertIn("rescue_fail_count: 1", self._status_text(wi))
        self.assertEqual(self._audit_tail()["action"], "rescue_requeue")

    def test_rescue_unresolved_test_command(self):
        # E1:无 test_command 源 → 判 requeue 且 reason=command_unresolved
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-w4"
        self._mk_wi(wi)                                # flow 块无 test_command;repo 无 tests/ 惯例
        wi_dir = self._wi_dir(wi)
        decision, result, tests = tc.assess_execute_completion(wi_dir, None)
        self.assertEqual(decision, "requeue")
        self.assertEqual(tests["reason"], "command_unresolved")
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# §4.3 风暴防护(2)
# ---------------------------------------------------------------------------

class RescueStormTests(RescueIsoBase):
    """冒烟③:连跑 2 次失败 → rescue_frozen + 升级人工;第 3 次 skip。"""

    def test_rescue_storm_freeze(self):
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-w5"
        self._mk_wi(wi, test_command="false")
        rec_path = os.path.join(self.tmp, "notify.jsonl")
        cfg = self._stub_notify(_cfg(), rec_path)
        # 第 1 次:count 1 < 2 → 重入队
        t1 = self._seed_task("t-000000000005", workitem=wi, state="failed", exit_code=3)
        self.assertEqual(tc.run_rescue_hook(t1, cfg, "failed", 3), "requeued")
        # 第 2 次:count 2 >= 2 → frozen,不重入队
        t2 = self._seed_task("t-000000000006", workitem=wi, state="failed", exit_code=3)
        self.assertEqual(tc.run_rescue_hook(t2, cfg, "failed", 3), "frozen")
        reg = tc.load_registry()
        scheduled = [x for x in reg["tasks"].values()
                     if x.get("workitem") == wi and x["state"] == "scheduled"]
        self.assertEqual(len(scheduled), 1)            # 第二次不再新增
        self.assertIn("rescue_frozen: true", self._status_text(wi))
        # 第 3 次:skip,无新增重入队、无重复 notify
        t3 = self._seed_task("t-000000000007", workitem=wi, state="failed", exit_code=3)
        self.assertEqual(tc.run_rescue_hook(t3, cfg, "failed", 3), "frozen_skip")
        reg = tc.load_registry()
        scheduled = [x for x in reg["tasks"].values()
                     if x.get("workitem") == wi and x["state"] == "scheduled"]
        self.assertEqual(len(scheduled), 1)
        recs = self._notify_recs(rec_path)
        self.assertEqual(recs[-1]["kind"], "rescue_frozen")
        # audit 序列:requeue → frozen → skip_frozen
        actions = [a["action"] for a in self._audit_all()]
        self.assertEqual(actions[-3:],
                         ["rescue_requeue", "rescue_frozen", "rescue_skip_frozen"])

    def test_rescue_success_resets_counter(self):
        # 成功抢救 → rescue_fail_count 归零;rescue_frozen 保持粘性不清
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-w6"
        self._mk_wi(wi, test_command="true", scope_allow=["scripts/demo.py"])
        wi_dir = self._wi_dir(wi)
        with open(os.path.join(wi_dir, "status.yaml"), "w", encoding="utf-8") as f:
            f.write("state: executed\nrescue_fail_count: 1\nrescue_frozen: true\n")
        t = self._seed_task("t-000000000010", workitem=wi, state="timeout", exit_code=124)
        # rescue(补产物)不受冻结阻断:补全产物是合法完成,不是重试风暴
        self.assertEqual(tc.run_rescue_hook(t, _cfg(), "timeout", 124), "rescued")
        st = self._status_text(wi)
        self.assertIn("rescue_fail_count: 0", st)
        self.assertIn("rescue_frozen: true", st)


# ---------------------------------------------------------------------------
# §4.4 audit(1)
# ---------------------------------------------------------------------------

class RescueAuditTests(RescueIsoBase):
    def test_rescue_audit_fields(self):
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-w7"
        self._mk_wi(wi, test_command="true", scope_allow=["scripts/demo.py"])
        t = self._seed_task("t-000000000011", workitem=wi, state="timeout", exit_code=124)
        tc.run_rescue_hook(t, _cfg(), "timeout", 124)
        tail = self._audit_tail()
        self.assertEqual(tail["stream"], "rescue")
        self.assertEqual(tail["actor"], "flow:rescue")
        self.assertEqual(tail["task_id"], "t-000000000011")
        self.assertEqual(tail["workitem"], wi)
        self.assertEqual(tail["kind"], "execute")
        self.assertEqual(tail["original_state"], "timeout")
        self.assertEqual(tail["original_exit_code"], 124)
        self.assertEqual(tail["decision"], "rescue")
        self.assertEqual(tail["action"], "rescue_success")
        self.assertIs(tail["result"]["gate_pass"], True)


# ---------------------------------------------------------------------------
# §4.5 token 预警(3)
# ---------------------------------------------------------------------------

class TokenWarningTests(RescueIsoBase):
    def test_token_peak_extraction(self):
        runs = os.path.join(self.home, ".reasonix", "runs")
        os.makedirs(runs, exist_ok=True)
        with open(os.path.join(runs, "20260824-100001.log"), "w", encoding="utf-8") as f:
            f.write("step · 250000 tok · done\n")
            f.write("step · 320,000 tok · done\n")      # 千分位变体
            f.write("no peak line\n")
        with open(os.path.join(runs, "20260824-100301.log"), "w", encoding="utf-8") as f:
            f.write("outside · 999999 tok ·\n")         # +3min > ±120s → 不计入
        peak = tc.extract_reasonix_token_peak("2026-08-24T02:00:01+00:00", None)
        self.assertEqual(peak, 320000)

    def test_token_peak_large_log_skipped(self):
        # ocr F4:超上限的 runaway run-log 跳过(不全量读入内存);上限内仍正常统计
        runs = os.path.join(self.home, ".reasonix", "runs")
        os.makedirs(runs, exist_ok=True)
        log = os.path.join(runs, "20260824-100001.log")
        with open(log, "w", encoding="utf-8") as f:
            f.write("step · 999999 tok · done\n" + "x" * 500 + "\n")
        with mock.patch.object(tc, "TOKEN_PEAK_SCAN_BYTES", 64):
            self.assertIsNone(
                tc.extract_reasonix_token_peak("2026-08-24T02:00:01+00:00", None))
        with mock.patch.object(tc, "TOKEN_PEAK_SCAN_BYTES", 1024):
            self.assertEqual(
                tc.extract_reasonix_token_peak("2026-08-24T02:00:01+00:00", None), 999999)

    def test_token_peak_finished_at_window(self):
        # ocr F5:finished_at 约束扫描窗口上界 [started_at-120s, finished_at]:
        # 任务结束后启动的 run(仍落在 ±120s 内)不计入;finished_at 缺失/非法 →
        # 上界回退 started_at+120s,维持原 ±120s 口径
        runs = os.path.join(self.home, ".reasonix", "runs")
        os.makedirs(runs, exist_ok=True)
        with open(os.path.join(runs, "20260824-100001.log"), "w", encoding="utf-8") as f:
            f.write("step · 100000 tok · done\n")       # 窗口内 [09:58:01, 10:01:00]
        with open(os.path.join(runs, "20260824-100130.log"), "w", encoding="utf-8") as f:
            f.write("step · 500000 tok · done\n")       # 10:01:30 > finished_at(10:01:00) → 排除
        with open(os.path.join(runs, "20260824-095759.log"), "w", encoding="utf-8") as f:
            f.write("step · 900000 tok · done\n")       # 09:57:59 < started_at-120s(09:58:01) → 排除
        started = "2026-08-24T02:00:01+00:00"
        self.assertEqual(tc.extract_reasonix_token_peak(started, "2026-08-24T02:01:00+00:00"),
                         100000)
        # finished_at 缺失/非法 → 上界回退 started_at+120s(10:02:01),100130 计入
        self.assertEqual(tc.extract_reasonix_token_peak(started, None), 500000)
        self.assertEqual(tc.extract_reasonix_token_peak(started, "bogus"), 500000)

    def test_token_peak_threshold_alert(self):
        # 冒烟④:peak > 300000 → token_warning 通知(含拆分建议);≤ 阈值不通知
        runs = os.path.join(self.home, ".reasonix", "runs")
        os.makedirs(runs, exist_ok=True)
        rec_path = os.path.join(self.tmp, "notify.jsonl")
        cfg = self._stub_notify(_cfg(), rec_path)
        t = {"id": "t-000000000020", "kind": "execute", "workitem": "w-tok",
             "started_at": "2026-08-24T02:00:01+00:00"}
        with open(os.path.join(runs, "20260824-100001.log"), "w", encoding="utf-8") as f:
            f.write("· 320000 tok ·\n")
        self.assertEqual(tc.run_token_warning(t, cfg, "2026-08-24T02:05:00+00:00"),
                         "alerted")
        recs = self._notify_recs(rec_path)
        self.assertEqual(recs[-1]["kind"], "token_warning")
        self.assertEqual(recs[-1]["peak_tokens"], 320000)
        self.assertEqual(recs[-1]["threshold"], 300000)
        self.assertIn("上下文过大，下次建议拆分任务", recs[-1]["guidance"])
        with open(os.path.join(runs, "20260824-100001.log"), "w", encoding="utf-8") as f:
            f.write("· 250000 tok ·\n")
        self.assertEqual(tc.run_token_warning(t, cfg, "2026-08-24T02:05:00+00:00"),
                         "below_threshold")
        self.assertEqual(len(self._notify_recs(rec_path)), 1)   # 无新增通知
        self.assertEqual(self._audit_tail()["action"], "token_warning")

    def test_token_peak_no_match(self):
        # E10:无 runs 目录/无 tok 行/时间窗无匹配/时间非法 → None,不 crash 不 notify
        t = {"id": "t-000000000021", "kind": "execute",
             "started_at": "2026-08-24T02:00:01+00:00"}
        rec_path = os.path.join(self.tmp, "notify.jsonl")
        cfg = self._stub_notify(_cfg(), rec_path)
        self.assertEqual(tc.run_token_warning(t, cfg, None), "skipped")
        runs = os.path.join(self.home, ".reasonix", "runs")
        os.makedirs(runs, exist_ok=True)
        with open(os.path.join(runs, "20260824-100001.log"), "w", encoding="utf-8") as f:
            f.write("no peak line here\n")
        self.assertIsNone(tc.extract_reasonix_token_peak("2026-08-24T02:00:01+00:00", None))
        self.assertEqual(tc.run_token_warning(t, cfg, None), "skipped")
        self.assertIsNone(tc.extract_reasonix_token_peak(None, None))
        self.assertIsNone(tc.extract_reasonix_token_peak("bogus", None))
        self.assertEqual(len(self._notify_recs(rec_path)), 0)

    def test_token_threshold_default(self):
        # E11:阈值配置缺失/非法 → 硬编码 300000;合法 env 可覆盖,非法 env 忽略
        self.assertEqual(tc._token_warn_threshold(_cfg()), 300000)
        self.assertEqual(tc._token_warn_threshold({"task": {}}), 300000)
        self.assertEqual(tc._token_warn_threshold({"task": {"token_warn_threshold": "abc"}}),
                         300000)
        self.assertEqual(tc._token_warn_threshold({"task": {"token_warn_threshold": 100}}),
                         100)
        os.environ["FLOW_TASK_TOKEN_WARN_THRESHOLD"] = "100"
        self.assertEqual(tc.load_task_config()["task"]["token_warn_threshold"], 100)
        os.environ["FLOW_TASK_TOKEN_WARN_THRESHOLD"] = "abc"
        self.assertNotIn("token_warn_threshold", tc.load_task_config()["task"])
        os.environ.pop("FLOW_TASK_TOKEN_WARN_THRESHOLD", None)
        self.assertNotIn("token_warn_threshold", tc.load_task_config()["task"])


# ---------------------------------------------------------------------------
# §4.6 非 execute 不触发(1)+ 边界(2)+ runner 集成接线(1)
# ---------------------------------------------------------------------------

class RescueBoundaryTests(RescueIsoBase):
    def test_git_probe_logic(self):
        # ocr F5:git 缺失时探测返回 False(不崩溃);可用时返回 True
        with mock.patch.object(shutil, "which", return_value=None):
            self.assertFalse(_git_available())
        if shutil.which("git") is not None:
            self.assertTrue(_git_available())

    def test_non_execute_no_rescue(self):
        # E13:kind=None / design / 无 workitem → skipped,零副作用
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-w8"
        self._mk_wi(wi, test_command="true", scope_allow=["scripts/demo.py"])
        rec_path = os.path.join(self.tmp, "notify.jsonl")
        cfg = self._stub_notify(_cfg(), rec_path)
        self.assertEqual(tc.run_rescue_hook(
            {"id": "t-000000000030", "kind": None, "workitem": wi}, cfg, "timeout", 124),
            "skipped")
        self.assertEqual(tc.run_rescue_hook(
            {"id": "t-000000000031", "kind": "design", "workitem": wi}, cfg, "timeout", 124),
            "skipped")
        self.assertEqual(tc.run_rescue_hook(
            {"id": "t-000000000032", "kind": "execute", "workitem": None}, cfg, "failed", 3),
            "skipped")
        wi_dir = self._wi_dir(wi)
        self.assertFalse(os.path.isfile(os.path.join(wi_dir, "executor", "result.json")))
        self.assertEqual(tc.load_registry()["tasks"], {})
        self.assertEqual(self._audit_all(), [])
        self.assertEqual(self._notify_recs(rec_path), [])

    def test_rescue_corrupt_result(self):
        # E2:result.json 损坏 → 归一缺失走判定,不 crash(测试绿 → rescue 重写为合法产物)
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-w9"
        self._mk_wi(wi, test_command="true", scope_allow=["scripts/demo.py"])
        with open(os.path.join(self._wi_dir(wi), "executor", "result.json"),
                  "w", encoding="utf-8") as f:
            f.write("not json {{{")
        t = self._seed_task("t-000000000040", workitem=wi, state="timeout", exit_code=124)
        self.assertEqual(tc.run_rescue_hook(t, _cfg(), "timeout", 124), "rescued")
        with open(os.path.join(self._wi_dir(wi), "executor", "result.json"),
                  encoding="utf-8") as f:
            r = json.load(f)
        self.assertEqual(r["status"], "ok")
        self.assertIs(r["rescued"], True)

    def test_rescue_chain_disabled(self):
        # E12:chain.enabled=false → 整体跳过,零动作、零 audit
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-w10"
        self._mk_wi(wi, test_command="false")
        cfg = _cfg()
        cfg["chain"]["enabled"] = False
        t = self._seed_task("t-000000000050", workitem=wi, state="timeout", exit_code=124)
        self.assertEqual(tc.run_rescue_hook(t, cfg, "timeout", 124), "skipped")
        wi_dir = self._wi_dir(wi)
        self.assertFalse(os.path.isfile(os.path.join(wi_dir, "executor", "result.json")))
        self.assertEqual(list(tc.load_registry()["tasks"]), ["t-000000000050"])
        self.assertEqual(self._audit_all(), [])

    def test_runner_timeout_rescue_integration(self):
        # 接线验证:完整 _runner 命令超时(rc=124) → 终态后抢救钩子自动补产物
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-w11"
        self._mk_wi(wi, test_command="true", scope_allow=["scripts/demo.py"])
        tid = "t-000000000060"
        self._seed_task(tid, workitem=wi, state="running", command="sleep 5",
                        expected_seconds=1, started_at=None, finished_at=None,
                        exit_code=None)
        os.makedirs(tc.logs_dir(), exist_ok=True)
        with _FDRedirect(tc.log_path(tid)):
            self.assertEqual(tc._runner(tid, _cfg()), 0)
        self.assertEqual(tc.load_registry()["tasks"][tid]["state"], "timeout")
        with open(os.path.join(self._wi_dir(wi), "executor", "result.json"),
                  encoding="utf-8") as f:
            r = json.load(f)
        self.assertEqual(r["original_exit_code"], 124)
        self.assertEqual(r["note"], "自动抢救，原始 timeout")
        self.assertIs(r["rescued"], True)

    def test_runner_single_notify_rescue_carries_failure(self):
        # ocr F6:execute 超时终态 rescue 已发通知 → 单通知:无 execute_failure,
        # rescue 通知携带 terminal_failure 失败上下文(不重复轰炸、不丢信息)
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-w12"
        self._mk_wi(wi, test_command="true", scope_allow=["scripts/demo.py"])
        tid = "t-000000000061"
        self._seed_task(tid, workitem=wi, state="running", command="sleep 5",
                        expected_seconds=1, started_at=None, finished_at=None,
                        exit_code=None)
        rec_path = os.path.join(self.tmp, "notify.jsonl")
        cfg = self._stub_notify(_cfg(), rec_path)
        os.makedirs(tc.logs_dir(), exist_ok=True)
        with _FDRedirect(tc.log_path(tid)):
            self.assertEqual(tc._runner(tid, cfg), 0)
        recs = self._notify_recs(rec_path)
        self.assertEqual([r["kind"] for r in recs], ["rescue_accept_pending"])
        self.assertIn("terminal_failure", recs[0])
        self.assertEqual(recs[0]["terminal_failure"]["state"], "timeout")
        self.assertEqual(recs[0]["terminal_failure"]["exit_code"], 124)

    def test_runner_notify_failure_when_rescue_silent(self):
        # ocr F6 对照:rescue 无通知动作(skipped)→ execute_failure 仍发(单通知不丢信息)
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-w13"
        self._mk_wi(wi, test_command="true", scope_allow=["scripts/demo.py"])
        tid = "t-000000000062"
        self._seed_task(tid, workitem=wi, state="running", command="sleep 5",
                        expected_seconds=1, started_at=None, finished_at=None,
                        exit_code=None)
        rec_path = os.path.join(self.tmp, "notify.jsonl")
        cfg = self._stub_notify(_cfg(), rec_path)
        os.makedirs(tc.logs_dir(), exist_ok=True)
        with mock.patch.object(tc, "run_rescue_hook", return_value="skipped"), \
                _FDRedirect(tc.log_path(tid)):
            self.assertEqual(tc._runner(tid, cfg), 0)
        recs = self._notify_recs(rec_path)
        self.assertEqual([r["kind"] for r in recs], ["execute_failure"])


# ---------------------------------------------------------------------------
# §3 错误处理表(E1/E3/E4/E5/E7/E8/E9)错误路径
# ---------------------------------------------------------------------------

class RescueErrorPathTests(RescueIsoBase):
    """E1-E14 错误路径:fail-closed、不 crash、计数不泄漏、任务终态不变。"""

    def test_rescue_no_git_diff(self):
        # E3:非 git 仓 / git 不可用 → 不 crash,diff.patch 空、changed_files=[](verify 收口)
        os.makedirs(self.repo, exist_ok=True)          # 存在但非 git 仓
        wi = "rescue-e3"
        self._mk_wi(wi, test_command="true", scope_allow=["scripts/demo.py"])
        t = self._seed_task("t-000000000070", workitem=wi, state="timeout", exit_code=124)
        # ocr F4:钉死精确动作——空 diff + gate-gated verify(测试 true + scope/错误表
        # 齐全)→ gate_pass → rescued;不得出现 verify_fail/其他分歧
        self.assertEqual(tc.run_rescue_hook(t, _cfg(), "timeout", 124), "rescued")
        with open(os.path.join(self._wi_dir(wi), "executor", "diff.patch"),
                  encoding="utf-8") as f:
            self.assertEqual(f.read(), "")            # 空 diff.patch
        with open(os.path.join(self._wi_dir(wi), "executor", "result.json"),
                  encoding="utf-8") as f:
            r = json.load(f)
        self.assertEqual(r["diff"]["changed_files"], [])

    def test_git_rescue_diff_unicode_paths(self):
        """ocr F4:含非 ASCII(中文)文件名的仓库,_git_rescue_diff 返回原始 UTF-8
        路径而非 quotePath 八进制转义(\"\\344\\270\\255...\")——与 _git_run 同口径。
        显式设 core.quotepath=true 复现 git 默认转义,验证 -c core.quotePath=false 覆盖。"""
        if not _git_available():
            self.skipTest("git 不可用,跳过依赖 git 仓的测试")
        os.makedirs(self.repo, exist_ok=True)
        subprocess.run(["git", "-C", self.repo, "init", "-q"], check=True)
        subprocess.run(["git", "-C", self.repo, "config", "user.email", "t@t"], check=True)
        subprocess.run(["git", "-C", self.repo, "config", "user.name", "t"], check=True)
        subprocess.run(["git", "-C", self.repo, "config", "core.quotepath", "true"],
                       check=True)                     # 显式 git 默认:非 ASCII 转义
        with open(os.path.join(self.repo, "中文方案.md"), "w", encoding="utf-8") as f:
            f.write("base\n")
        subprocess.run(["git", "-C", self.repo, "add", "."], check=True)
        subprocess.run(["git", "-C", self.repo, "commit", "-qm", "base"], check=True)
        with open(os.path.join(self.repo, "中文方案.md"), "w", encoding="utf-8") as f:
            f.write("changed\n")                       # M:中文文件名(改动文件)
        with open(os.path.join(self.repo, "翻译-交付.txt"), "w", encoding="utf-8") as f:
            f.write("u\n")                             # untracked:中文文件名
        d = tc._git_rescue_diff(self.repo)
        self.assertIn("中文方案.md", d["changed_files"])   # 原始 UTF-8,非 "\344\270\255..."
        self.assertIn("翻译-交付.txt", d["untracked_files"])
        for p in list(d["changed_files"]) + list(d["untracked_files"]):
            self.assertNotIn("\\", p)                  # 无任何八进制转义残留

    def test_git_rescue_diff_bounded_output(self):
        """ocr7-M1:git diff HEAD 有字节上限——超限截断 + stderr 告警,大 patch
        不全量进内存;截断标记落 patch,changed_files/untracked 不受影响。"""
        if not _git_available():
            self.skipTest("git 不可用,跳过依赖 git 仓的测试")
        os.makedirs(self.repo, exist_ok=True)
        subprocess.run(["git", "-C", self.repo, "init", "-q"], check=True)
        subprocess.run(["git", "-C", self.repo, "config", "user.email", "t@t"], check=True)
        subprocess.run(["git", "-C", self.repo, "config", "user.name", "t"], check=True)
        with open(os.path.join(self.repo, "big.txt"), "w", encoding="utf-8") as f:
            f.write("base\n")
        subprocess.run(["git", "-C", self.repo, "add", "."], check=True)
        subprocess.run(["git", "-C", self.repo, "commit", "-qm", "base"], check=True)
        with open(os.path.join(self.repo, "big.txt"), "w", encoding="utf-8") as f:
            f.write("\n".join(
                f"line {i} changed padding padding padding padding" for i in range(8000)))
        with mock.patch.object(tc, "RESCUE_DIFF_MAX_BYTES", 2048), \
             contextlib.redirect_stderr(io.StringIO()) as err:
            d = tc._git_rescue_diff(self.repo)
        self.assertTrue(d["patch"])                    # 有内容
        self.assertLessEqual(len(d["patch"].encode("utf-8")), 4096)  # 有界(非全量)
        self.assertIn("截断", d["patch"])              # 截断标记在 patch 尾部
        self.assertIn("告警", err.getvalue())          # 超限告警
        self.assertIn("big.txt", d["changed_files"])   # 路径清单不受截断影响

    def test_git_diff_bounded_non_posix_blocking_read(self):
        """ocr9-F1:非 POSIX(os.name != "posix")下 select 门禁失效——select 可
        import 成功但 select.select 对管道抛 OSError(WinError 10038);须跳过 select
        走阻塞读,不把 OSError 泄露成 rescue_write_fail。mock os.name="nt" + select.
        select 抛 OSError,验证 _git_diff_bounded 仍正常返回、不调 select 且不抛。"""
        if not _git_available():
            self.skipTest("git 不可用,跳过依赖 git 仓的测试")
        os.makedirs(self.repo, exist_ok=True)
        subprocess.run(["git", "-C", self.repo, "init", "-q"], check=True)
        subprocess.run(["git", "-C", self.repo, "config", "user.email", "t@t"], check=True)
        subprocess.run(["git", "-C", self.repo, "config", "user.name", "t"], check=True)
        with open(os.path.join(self.repo, "f.txt"), "w", encoding="utf-8") as f:
            f.write("base\n")
        subprocess.run(["git", "-C", self.repo, "add", "."], check=True)
        subprocess.run(["git", "-C", self.repo, "commit", "-qm", "base"], check=True)
        with open(os.path.join(self.repo, "f.txt"), "w", encoding="utf-8") as f:
            f.write("changed\n")

        def _boom(*_a, **_k):
            raise OSError("WinError 10038")

        with mock.patch.object(tc.os, "name", "nt"), \
             mock.patch.object(tc.select, "select", side_effect=_boom) as sel:
            patch, truncated = tc._git_diff_bounded(self.repo, 1_000_000)
        self.assertFalse(truncated)
        self.assertIn("changed", patch)               # 阻塞读正常读到 diff 内容
        self.assertFalse(sel.called)                  # select.select 未被调用(门禁生效)

    def test_rescue_wi_missing(self):
        # E4:workitem 目录缺失 → audit rescue_error,任务终态不变,不产生产物
        t = self._seed_task("t-000000000071", workitem="ghost-wi", state="failed",
                            exit_code=3)
        self.assertEqual(tc.run_rescue_hook(t, _cfg(), "failed", 3), "error")
        tail = self._audit_tail()
        self.assertEqual(tail["action"], "rescue_error")
        self.assertTrue(tail["result"]["error"])
        self.assertEqual(tc.load_registry()["tasks"]["t-000000000071"]["state"], "failed")

    def test_rescue_lock_degrade(self):
        # E5:fcntl 缺失(非 POSIX 降级直执行)→ 主流程行为不变
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-e5"
        self._mk_wi(wi, test_command="true", scope_allow=["scripts/demo.py"])
        t = self._seed_task("t-000000000072", workitem=wi, state="timeout", exit_code=124)
        with mock.patch.object(tc._fc, "fcntl", None):
            self.assertEqual(tc.run_rescue_hook(t, _cfg(), "timeout", 124), "rescued")
        self.assertTrue(os.path.isfile(
            os.path.join(self._wi_dir(wi), "executor", "result.json")))

    def test_rescue_requeue_duplicate(self):
        # E7:重入队遇同 workitem 非终态冲突 → 计数回滚(不递增),audit rescue_requeue_conflict
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-e7"
        self._mk_wi(wi, test_command="false")
        self._seed_task("t-000000000073", workitem=wi, state="scheduled", exit_code=None)
        t = self._seed_task("t-000000000074", workitem=wi, state="failed", exit_code=3)
        self.assertEqual(tc.run_rescue_hook(t, _cfg(), "failed", 3), "conflict")
        self.assertNotIn("rescue_fail_count", self._status_text(wi))  # 回滚生效
        tail = self._audit_tail()
        self.assertEqual(tail["action"], "rescue_requeue_conflict")
        self.assertEqual(len(tc.load_registry()["tasks"]), 2)   # 无新增任务

    def test_rescue_requeue_rollback_preserves_concurrent_bump(self):
        # ocr F1:回滚为条件式——_bump 后、_rollback 前并发 rescue 流 bump 计数并
        # 冻结,回滚不得用 stale before 快照覆盖并发更新(TOCTOU 竞态修复)
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-f1"
        self._mk_wi(wi, test_command="false")
        t = self._seed_task("t-000000000080", workitem=wi, state="failed", exit_code=3)

        def _conflicting_add(*a, **kw):
            # 模拟并发 rescue 流:此刻 _bump 已持锁完成、锁已释放,回滚尚未发生
            with open(os.path.join(self._wi_dir(wi), "status.yaml"), "w",
                      encoding="utf-8") as f:
                f.write("state: executed\nrescue_fail_count: 3\nrescue_frozen: true\n")
            raise tc.DuplicateWorkitem(wi)

        with mock.patch.object(tc, "add_task", side_effect=_conflicting_add):
            self.assertEqual(tc.run_rescue_hook(t, _cfg(), "failed", 3), "conflict")
        text = self._status_text(wi)
        self.assertIn("rescue_fail_count: 3", text)   # 并发 bump 不被回滚覆盖
        self.assertIn("rescue_frozen: true", text)    # 并发冻结不被清除
        self.assertEqual(self._audit_tail()["action"], "rescue_requeue_conflict")

    def test_rescue_bump_corrupt_counter(self):
        # 计数损坏 → _bump 归一 1,不 crash 不提前冻结
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-e6"
        self._mk_wi(wi, test_command="false")
        with open(os.path.join(self._wi_dir(wi), "status.yaml"), "w",
                  encoding="utf-8") as f:
            f.write("state: executed\nrescue_fail_count: bogus\n")
        t = self._seed_task("t-000000000077", workitem=wi, state="failed", exit_code=3)
        self.assertEqual(tc.run_rescue_hook(t, _cfg(), "failed", 3), "requeued")
        self.assertIn("rescue_fail_count: 1", self._status_text(wi))

    def test_rescue_requeue_queue_full(self):
        # E7 变体:队列满 → rescue_requeue_queue_full,计数回滚(非任务本身失败不递增)
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-e7q"
        self._mk_wi(wi, test_command="false")
        t = self._seed_task("t-000000000078", workitem=wi, state="failed", exit_code=3)
        with mock.patch.object(tc, "add_task", side_effect=tc.QueueFull(50)):
            self.assertEqual(tc.run_rescue_hook(t, _cfg(), "failed", 3), "conflict")
        self.assertNotIn("rescue_fail_count", self._status_text(wi))  # 回滚生效
        self.assertEqual(self._audit_tail()["action"], "rescue_requeue_queue_full")

    def test_rescue_write_fail(self):
        # E8:写 result.json/diff.patch 失败 → audit rescue_write_fail + notify,不改任务终态
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-e8"
        self._mk_wi(wi, test_command="true", scope_allow=["scripts/demo.py"])
        rec_path = os.path.join(self.tmp, "notify.jsonl")
        cfg = self._stub_notify(_cfg(), rec_path)
        t = self._seed_task("t-000000000075", workitem=wi, state="timeout", exit_code=124)
        with mock.patch.object(tc, "_atomic_write_local", side_effect=OSError("disk full")):
            self.assertEqual(tc.run_rescue_hook(t, cfg, "timeout", 124), "write_fail")
        self.assertEqual(self._audit_tail()["action"], "rescue_write_fail")
        recs = self._notify_recs(rec_path)
        self.assertEqual(recs[-1]["kind"], "rescue_write_fail")
        self.assertEqual(tc.load_registry()["tasks"]["t-000000000075"]["state"], "timeout")

    def test_rescue_verify_error(self):
        # E9:verify 前置失败(产物缺失,理论不发生)→ rescue_verify_error + notify
        self._mk_git_repo(["scripts/demo.py"])
        wi = "rescue-e9"
        self._mk_wi(wi, test_command="true", scope_allow=["scripts/demo.py"])
        rec_path = os.path.join(self.tmp, "notify.jsonl")
        cfg = self._stub_notify(_cfg(), rec_path)
        t = self._seed_task("t-000000000076", workitem=wi, state="timeout", exit_code=124)
        with mock.patch.object(tc._fc, "run_verify_auto_core",
                               return_value={"ok": False,
                                             "error": "executor/result.json 缺失"}):
            self.assertEqual(tc.run_rescue_hook(t, cfg, "timeout", 124), "verify_error")
        self.assertEqual(self._audit_tail()["action"], "rescue_verify_error")
        recs = self._notify_recs(rec_path)
        self.assertEqual(recs[-1]["kind"], "rescue_verify_error")


if __name__ == "__main__":
    unittest.main()
