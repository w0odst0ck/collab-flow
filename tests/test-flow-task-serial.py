#!/usr/bin/env python3
# noqa: N999
# 注:文件名连字符为项目惯例(与 test-flow-core.py 等旧文件一致)
"""flow-task-core.py 调度亲和门控单测(cost-opt-cache-dispatch 省钱批次 1b,design §2.1)。

覆盖:
  T1-T8   同仓串行 / 跨仓并行 / pro 串行 / 豁免 / 开关回归 / max_parallel 边界
  T9-T10  model 字段落地 + pro 集合从 config 读(不硬编码)
  T11-T12 豁免者占资源 + 两参向后兼容(回归护栏)
  E1-E10  错误路径:最小 cfg / env 非整数 / 老条目缺 workdir/model /
          command 无 --model / max_parallel 非法 / reconcile 回收解阻塞 /
          pro timeout 解阻塞 / 双关全关回归 / force 豁免混用

用法: python3 -m unittest discover tests
零 API、全程 FLOW_TASK_DIR 临时目录隔离;纯函数直接 import(零 I/O)。
"""

import importlib.util
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
_TASK_CORE = os.path.join(_HERE, "..", "scripts", "flow-task-core.py")

_spec = importlib.util.spec_from_file_location("flow_task_serial", _TASK_CORE)
tc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tc)

PRO = "deepseek-v4-pro"
FLASH = "deepseek-v4-flash"


def _entry(tid="t-000000000001", state="queued", priority="P2", workitem=None,
           command="sleep 0.01", created_at="2026-08-21T10:00:00+00:00",
           workdir="/w1", model=None, audit=None, kind=None,
           expected_seconds=None, scheduled_at=None, **extra):
    """任务条目助手(含 workdir/model/audit/kind 字段;缺省即老条目形态)。"""
    e = {
        "id": tid, "workitem": workitem, "command": command, "priority": priority,
        "state": state, "kind": kind, "model": model, "workdir": workdir,
        "expected_seconds": expected_seconds, "scheduled_at": scheduled_at,
        "created_at": created_at, "started_at": None, "finished_at": None,
        "exit_code": None, "failure_tail": None, "pid": None,
        "heartbeat_at": None, "audit": audit,
    }
    e.update(extra)
    return e


def _cfg(**kw):
    """config 助手(模拟 load_task_config 全量形态:task + roles + executor)。"""
    base = {
        "task": {"schema_version": 1, "max_parallel": 2, "default_priority": "P2",
                 "same_workdir_serial": 1, "pro_serial": 1},
        "roles": {"designer": {"model": {"pro": PRO}}},
        "executor": {"size": {
            "small": {"model": FLASH},
            "medium": {"model": FLASH},
            "large": {"model": PRO},
        }},
    }
    base["task"].update(kw)
    return base


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


def _reg(*entries):
    return {"schema_version": 1, "tasks": {t["id"]: t for t in entries}}


class TaskIsoBase(unittest.TestCase):
    """FLOW_TASK_DIR 临时目录隔离基类(集成用例)。"""

    def setUp(self):
        self._old_env = os.environ.get("FLOW_TASK_DIR")
        self.task_dir = tempfile.mkdtemp(prefix="flow-task-serial-test-")
        os.environ["FLOW_TASK_DIR"] = self.task_dir

    def tearDown(self):
        if self._old_env is None:
            os.environ.pop("FLOW_TASK_DIR", None)
        else:
            os.environ["FLOW_TASK_DIR"] = self._old_env
        shutil.rmtree(self.task_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# T1-T8 / T10-T12 / E1/E3/E4/E5/E6/E9:纯函数门控(零 I/O)
# ---------------------------------------------------------------------------

class SerialGatingTests(unittest.TestCase):
    """调度亲和门控纯函数单测(design §2.1 T1-T12 + §5 E 系列)。"""

    def test_T1_same_workdir_blocked(self):
        """T1:running=/w1;queued=/w1(B)+/w2(C)→ 只提升 C,B 留 queued。"""
        reg = _reg(
            _entry(tid="r", state="running", workdir="/w1"),
            _entry(tid="b", workdir="/w1"),
            _entry(tid="c", workdir="/w2"),
        )
        got = tc.plan_dispatch(reg, 2, _cfg())  # free=1
        self.assertEqual([t["id"] for t in got], ["c"])
        self.assertEqual(reg["tasks"]["b"]["state"], "queued")  # plan 不改 reg

    def test_T2_cross_repo_parallel_skip_head(self):
        """T2:running=/w1;queued=/w1(B)、/w2(C)、/w3(D),free=2 → 提升 C、D,
        B 阻塞且不 head-of-line 阻塞后续跨仓任务。"""
        reg = _reg(
            _entry(tid="r", state="running", workdir="/w1"),
            _entry(tid="b", workdir="/w1"),
            _entry(tid="c", workdir="/w2"),
            _entry(tid="d", workdir="/w3"),
        )
        got = tc.plan_dispatch(reg, 3, _cfg())  # free=2
        self.assertEqual([t["id"] for t in got], ["c", "d"])

    def test_T3_empty_run_same_workdir_serial(self):
        """T3:无 running,3 个同 /w1 queued,free=2 → 只提升第 1 个(本遍内同仓互斥)。"""
        reg = _reg(
            _entry(tid="a", workdir="/w1"),
            _entry(tid="b", workdir="/w1"),
            _entry(tid="c", workdir="/w1"),
        )
        got = tc.plan_dispatch(reg, 2, _cfg())
        self.assertEqual([t["id"] for t in got], ["a"])

    def test_T4_pro_design_execute_mutex(self):
        """T4:running=pro(execute);queued=pro(design)+flash(execute)→ 只提升 flash。"""
        reg = _reg(
            _entry(tid="r", state="running", workdir="/w1", model=PRO, kind="execute"),
            _entry(tid="d", workdir="/w2", model=PRO, kind="design"),
            _entry(tid="f", workdir="/w3", model=FLASH, kind="execute"),
        )
        got = tc.plan_dispatch(reg, 2, _cfg())  # free=1
        self.assertEqual([t["id"] for t in got], ["f"])

    def test_T5_pro_p0_exempt(self):
        """T5:running=pro;queued=P0 pro → 提升(豁免串行等待)。"""
        reg = _reg(
            _entry(tid="r", state="running", workdir="/w1", model=PRO),
            _entry(tid="q", workdir="/w2", model=PRO, priority="P0"),
        )
        got = tc.plan_dispatch(reg, 2, _cfg())
        self.assertEqual([t["id"] for t in got], ["q"])

    def test_T6_pro_force_exempt(self):
        """T6:running=pro;queued=pro 且 audit.force_reason 非空 → 提升(force 豁免)。"""
        reg = _reg(
            _entry(tid="r", state="running", workdir="/w1", model=PRO),
            _entry(tid="q", workdir="/w2", model=PRO,
                   audit={"force_reason": "emergency hotfix"}),
        )
        got = tc.plan_dispatch(reg, 2, _cfg())
        self.assertEqual([t["id"] for t in got], ["q"])

    def test_T7_switches_off_parallel(self):
        """T7:双开关置 0 → 恢复并行,等价旧切片(running=/w1+pro,queued=/w1 B+pro 均提升)。"""
        cfg = _cfg(same_workdir_serial=0, pro_serial=0)
        reg = _reg(
            _entry(tid="r1", state="running", workdir="/w1"),
            _entry(tid="r2", state="running", workdir="/w9", model=PRO),
            _entry(tid="b", workdir="/w1"),
            _entry(tid="p", workdir="/w2", model=PRO),
        )
        got = tc.plan_dispatch(reg, 4, cfg)  # free=2
        self.assertEqual([t["id"] for t in got], ["b", "p"])

    def test_T8_max_parallel_boundary(self):
        """T8:max_parallel=1 不变;E6:max_parallel 非法(0/负)→ 空。豁免不越容量。"""
        cfg = _cfg()
        # running=1 → free=0 → 空
        reg = _reg(_entry(tid="r", state="running", workdir="/w1"),
                   _entry(tid="q", workdir="/w2"))
        self.assertEqual(tc.plan_dispatch(reg, 1, cfg), [])
        # 0 running + 2 同仓,free=1 → 只 1 个
        reg2 = _reg(_entry(tid="a", workdir="/w1"), _entry(tid="b", workdir="/w1"))
        got = tc.plan_dispatch(reg2, 1, cfg)
        self.assertEqual([t["id"] for t in got], ["a"])
        # P0 也不破容量(free=0 → 空)
        reg3 = _reg(_entry(tid="r", state="running", workdir="/w1"),
                    _entry(tid="q", workdir="/w2", priority="P0"))
        self.assertEqual(tc.plan_dispatch(reg3, 1, cfg), [])
        # E6:max_parallel=0/负 → free 钳制 0 → 空
        self.assertEqual(tc.plan_dispatch(reg2, 0, cfg), [])
        self.assertEqual(tc.plan_dispatch(reg2, -1, cfg), [])

    def test_T10_pro_models_from_config(self):
        """T10:pro 集合从 config 读(去重);flash 任务非 pro;无 model 老条目非 pro。"""
        self.assertEqual(tc._pro_models(_cfg()), {PRO})
        self.assertTrue(tc._is_pro(_entry(model=PRO), _cfg()))
        self.assertFalse(tc._is_pro(_entry(model=FLASH), _cfg()))
        self.assertFalse(tc._is_pro(_entry(model=None), _cfg()))
        # 显式 --model 传了非 pro 模型 → 非 pro(pro 判定以字段为准,不猜 kind)
        self.assertFalse(tc._is_pro(_entry(kind="design", model=FLASH), _cfg()))

    def test_T11_exempt_occupies_resource(self):
        """T11:running=pro;同遍 queued=P0 pro + 普通 pro,free=2 → P0 提升后,
        普通 pro 仍被 pro_running 阻塞(豁免不"免费放行"后续 pro)。"""
        reg = _reg(
            _entry(tid="r", state="running", workdir="/w1", model=PRO),
            _entry(tid="p0", workdir="/w2", model=PRO, priority="P0"),
            _entry(tid="p1", workdir="/w3", model=PRO),
        )
        got = tc.plan_dispatch(reg, 3, _cfg())  # free=2
        self.assertEqual([t["id"] for t in got], ["p0"])

    def test_T12_two_arg_backward_compat(self):
        """T12:plan_dispatch(reg, n)(cfg=None)→ 与旧切片完全一致;plan_pump 同理。"""
        reg = _reg(
            _entry(tid="a", workdir="/w1"),
            _entry(tid="b", workdir="/w1"),
            _entry(tid="c", workdir="/w2", model=PRO),
            _entry(tid="d", workdir="/w3"),
            _entry(tid="r", state="running", workdir="/w1", model=PRO),
        )
        free = max(5 - 1, 0)
        old = [t["id"] for t in tc.sort_queue(reg["tasks"].values())[:free]]
        got = tc.plan_dispatch(reg, 5)
        self.assertEqual([t["id"] for t in got], old)
        # plan_pump 三参(cfg=None)等价旧切片:due 排序后取 free=1
        now = datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)
        reg2 = _reg(
            _entry(tid="s1", state="scheduled", workdir="/w1",
                   scheduled_at="2026-08-21T11:00:00+00:00"),
            _entry(tid="s2", state="scheduled", workdir="/w2",
                   scheduled_at="2026-08-21T11:00:00+00:00"),
            _entry(tid="r", state="running", workdir="/w1"),
        )
        due = [tid for tid, t in reg2["tasks"].items()
               if t.get("state") == "scheduled"
               and datetime.fromisoformat(t["scheduled_at"]) <= now]
        due.sort(key=lambda i: (tc.PRIO_ORDER.get(reg2["tasks"][i].get("priority"), 99),
                                reg2["tasks"][i].get("scheduled_at", ""), i))
        got2 = tc.plan_pump(reg2, 2, now)  # free=1
        self.assertEqual([t["id"] for t in got2], due[:1])

    def test_E1_minimal_cfg_no_roles_executor(self):
        """E1:最小 cfg(无 roles/executor)→ 不抛,pro 集空,model 推导 None(fail-open)。"""
        cfg_min = {"task": {"same_workdir_serial": 1, "pro_serial": 1}}
        self.assertEqual(tc._pro_models(cfg_min), set())
        self.assertIsNone(tc._default_model_for("anything", "design", cfg_min))
        self.assertIsNone(tc._default_model_for("anything", "execute", cfg_min))
        reg = _reg(_entry(tid="a", workdir="/w1", model=PRO))
        got = tc.plan_dispatch(reg, 2, cfg_min)  # 不抛
        self.assertEqual([t["id"] for t in got], ["a"])

    def test_E3_legacy_no_workdir(self):
        """E3:老条目缺 workdir → 不参与同仓阻塞(按跨仓处理),不崩溃。"""
        self.assertIsNone(tc._wd_key(_entry(workdir=None)))
        reg = _reg(
            _entry(tid="r", state="running", workdir="/w1"),
            _entry(tid="b", workdir=None),  # 缺 workdir → 无法关联 → 放行
        )
        got = tc.plan_dispatch(reg, 2, _cfg())
        self.assertEqual([t["id"] for t in got], ["b"])

    def test_E4_legacy_no_model(self):
        """E4:老条目缺 model 键 → 非 pro,不参与 pro 串行,不崩溃。"""
        old = _entry(tid="q", workdir="/w2")
        del old["model"]  # 老注册表条目形态(无 model 字段)
        self.assertFalse(tc._is_pro(old, _cfg()))
        reg = _reg(_entry(tid="r", state="running", workdir="/w1", model=PRO), old)
        got = tc.plan_dispatch(reg, 2, _cfg())
        self.assertEqual([t["id"] for t in got], ["q"])

    def test_T9_workdir_normalization(self):
        """T9:workdir 路径归一——/repo 与 /repo/ 判同仓(normpath+abspath);同 cwd 相对路径亦归一。"""
        self.assertEqual(tc._wd_key(_entry(workdir="/repo")),
                         tc._wd_key(_entry(workdir="/repo/")))
        # 相对路径按 cwd abspath 解析:同 cwd 下 "repo" 与 "./repo/" 归一一致
        self.assertEqual(tc._wd_key(_entry(workdir="repo")),
                         tc._wd_key(_entry(workdir="./repo/")))
        # 门控实际生效:running=/repo1,queued=/repo1/(带尾斜杠)→ 仍同仓阻塞
        reg = _reg(
            _entry(tid="r", state="running", workdir="/repo1"),
            _entry(tid="b", workdir="/repo1/"),
            _entry(tid="c", workdir="/repo2"),
        )
        got = tc.plan_dispatch(reg, 2, _cfg())
        self.assertEqual([t["id"] for t in got], ["c"])

    def test_E5_model_command_missing(self):
        """E5:command 无 --model / shlex 解析失败 → None;值以 -- 开头视为缺值。"""
        self.assertIsNone(tc._model_from_command("flow workitem execute x --sync"))
        self.assertIsNone(tc._model_from_command(""))
        self.assertIsNone(tc._model_from_command("cmd --model"))
        self.assertIsNone(tc._model_from_command("cmd --model --flag"))
        self.assertIsNone(tc._model_from_command("echo 'unbalanced \"quote"))
        self.assertEqual(
            tc._model_from_command(
                "flow workitem execute x --sync --model deepseek-v4-pro"),
            "deepseek-v4-pro")
        # _default_model_for 回退 flash / None,不抛
        cfg = _cfg()
        self.assertEqual(
            tc._default_model_for("flow workitem execute x --sync", "execute", cfg),
            FLASH)
        self.assertEqual(tc._default_model_for("x", "design", cfg), PRO)
        self.assertIsNone(tc._default_model_for("x", "other-kind", cfg))

    def test_E9_switches_off_fast_path_equivalent(self):
        """E9:双关全关 → _select_gated 走 ordered[:free] 快路径,逐字节等价旧切片。"""
        reg = _reg(
            _entry(tid="a", workdir="/w1"),
            _entry(tid="b", workdir="/w1"),
            _entry(tid="c", workdir="/w2", model=PRO),
            _entry(tid="r", state="running", workdir="/w1"),
        )
        cfg_off = _cfg(same_workdir_serial=0, pro_serial=0)
        got_gated = tc._select_gated(
            tc.sort_queue(reg["tasks"].values()),
            [t for t in reg["tasks"].values() if t.get("state") == "running"],
            2, cfg_off)
        got_old = tc.plan_dispatch(reg, 3)  # cfg=None 旧切片(free=2)
        self.assertEqual([t["id"] for t in got_gated], [t["id"] for t in got_old])

    def test_E10_force_mixed_with_regular_pro(self):
        """E10:force 豁免与普通 pro 混用——豁免仅作用于亲和门控:force pro 提升后
        占用资源,普通 pro 仍被 pro_running 阻塞;无 audit(未 force)不豁免。"""
        reg = _reg(
            _entry(tid="r", state="running", workdir="/w1", model=PRO),
            _entry(tid="f", workdir="/w2", model=PRO,
                   audit={"force_reason": "hotfix"}),
            _entry(tid="p", workdir="/w3", model=PRO),
        )
        got = tc.plan_dispatch(reg, 3, _cfg())  # free=2
        self.assertEqual([t["id"] for t in got], ["f"])
        # 无 audit(force=False)→ 不豁免,pro 串行照常阻塞
        reg2 = _reg(
            _entry(tid="r", state="running", workdir="/w1", model=PRO),
            _entry(tid="p", workdir="/w2", model=PRO, audit=None),
        )
        self.assertEqual(tc.plan_dispatch(reg2, 2, _cfg()), [])

    def test_plan_pump_gated_same_workdir(self):
        """pump 侧:到期 scheduled 同仓串行(design §1.5 接入点 plan_pump 走 _select_gated)。"""
        now = datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)
        reg = _reg(
            _entry(tid="r", state="running", workdir="/w1"),
            _entry(tid="a", state="scheduled", workdir="/w1",
                   scheduled_at="2026-08-21T11:00:00+00:00"),
            _entry(tid="b", state="scheduled", workdir="/w2",
                   scheduled_at="2026-08-21T11:00:00+00:00"),
        )
        got = tc.plan_pump(reg, 2, now, _cfg())  # free=1
        self.assertEqual([t["id"] for t in got], ["b"])


# ---------------------------------------------------------------------------
# T9 / E2 / E7 / E8:集成(FLOW_TASK_DIR 临时目录隔离)
# ---------------------------------------------------------------------------

class SerialIsoTests(TaskIsoBase):
    """model 字段落地 + env 开关 + 死 pid/timeout 解阻塞集成单测。"""

    def test_T9_model_field_landing(self):
        """T9:add_task(design)→ model==roles.designer.model.pro;execute 带 --model →
        命令串解析;无 --model → flash;显式 model 优先。"""
        cfg = _cfg()
        later = "2099-01-01T00:00:00+00:00"
        t1 = tc.add_task(cfg, "dsh design taskbook", workitem="w-d", kind="design",
                         workdir="/w1", expected_seconds=60, priority="P2",
                         why="t9", scheduled_at=later)
        self.assertEqual(tc.load_registry()["tasks"][t1]["model"], PRO)
        t2 = tc.add_task(cfg, "flow workitem execute x --sync --model deepseek-v4-pro",
                         workitem="w-x", kind="execute", workdir="/w1",
                         expected_seconds=60, priority="P2", why="t9",
                         scheduled_at=later)
        self.assertEqual(tc.load_registry()["tasks"][t2]["model"], "deepseek-v4-pro")
        t3 = tc.add_task(cfg, "flow workitem execute y --sync", workitem="w-y",
                         kind="execute", workdir="/w1", expected_seconds=60,
                         priority="P2", why="t9", scheduled_at=later)
        self.assertEqual(tc.load_registry()["tasks"][t3]["model"], FLASH)
        t4 = tc.add_task(cfg, "flow workitem execute z --sync", workitem="w-z",
                         kind="execute", workdir="/w1", expected_seconds=60,
                         priority="P2", why="t9", scheduled_at=later,
                         model="explicit-model")
        self.assertEqual(tc.load_registry()["tasks"][t4]["model"], "explicit-model")

    def test_E2_switch_env_invalid(self):
        """E2:env 非整数 → 默认 1(开);置 0 → 关;load_task_config 透传 roles/executor。"""
        defaults = os.path.join(_HERE, "..", "config", "defaults.yaml")
        saved = {}
        for k in ("COLLABFLOW_DEFAULTS", "COLLABFLOW_CONFIG",
                  "FLOW_TASK_SAME_WORKDIR_SERIAL", "FLOW_TASK_PRO_SERIAL"):
            saved[k] = os.environ.get(k)
            os.environ.pop(k, None)
        try:
            os.environ["COLLABFLOW_DEFAULTS"] = defaults
            os.environ["COLLABFLOW_CONFIG"] = "/nonexistent/config.yaml"
            cfg = tc.load_task_config()
            self.assertEqual(cfg["task"]["same_workdir_serial"], 1)  # 默认开
            self.assertEqual(cfg["task"]["pro_serial"], 1)
            self.assertIn("roles", cfg)      # 透传块
            self.assertIn("executor", cfg)
            os.environ["FLOW_TASK_SAME_WORKDIR_SERIAL"] = "0"
            self.assertEqual(tc.load_task_config()["task"]["same_workdir_serial"], 0)
            os.environ["FLOW_TASK_SAME_WORKDIR_SERIAL"] = "abc"  # 非整数 → 默认 1
            cfg2 = tc.load_task_config()
            self.assertEqual(cfg2["task"]["same_workdir_serial"], 1)
            os.environ["FLOW_TASK_PRO_SERIAL"] = "abc"
            self.assertEqual(tc.load_task_config()["task"]["pro_serial"], 1)
            # _serial_enabled 值损坏 → 默认 1(成本保守)
            self.assertTrue(tc._serial_enabled(
                {"task": {"same_workdir_serial": "bogus"}}, "same_workdir_serial"))
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def test_E7_reconcile_unblocks(self):
        """E7:同仓 blocker 死 pid → reconcile_running 先回收为 failed → 下轮放行。"""
        reg = _reg(
            _entry(tid="t-000000000001", state="running", workdir="/w1", pid=999999,
                   started_at="2026-08-21T10:00:00+00:00"),
            _entry(tid="t-000000000002", workdir="/w1"),
        )
        tc.save_registry_atomic(reg)
        reg2 = tc.load_registry()
        reaped = tc.reconcile_running(reg2)
        self.assertIn("t-000000000001", reaped)
        got = tc.plan_dispatch(reg2, 2, _cfg())
        self.assertEqual([t["id"] for t in got], ["t-000000000002"])

    def test_E8_pro_timeout_unblocks(self):
        """E8:pro blocker 超时(expected_seconds 硬超时)→ timeout 终态 → 后续 pro 出队。"""
        cfg = _cfg()
        r_id, b_id = "t-000000000001", "t-000000000002"
        reg = _reg(
            _entry(tid=r_id, state="running", workdir="/w1", model=PRO,
                   command="sleep 5", expected_seconds=1,
                   started_at="2026-08-21T10:00:00+00:00", workitem="w8"),
            _entry(tid=b_id, workdir="/w2", model=PRO),
        )
        tc.save_registry_atomic(reg)
        self.assertEqual(tc.plan_dispatch(tc.load_registry(), 2, cfg), [])  # pro 阻塞
        os.makedirs(tc.logs_dir(), exist_ok=True)
        with _FDRedirect(tc.log_path(r_id)):
            rc = tc._runner(r_id, cfg)
        self.assertEqual(rc, 0)
        reg2 = tc.load_registry()
        self.assertEqual(reg2["tasks"][r_id]["state"], "timeout")
        # runner 终态后自续 dispatch(§1.4(3)):pro blocker 已解除,后续 pro 出队
        self.assertEqual(reg2["tasks"][b_id]["state"], "running")


if __name__ == "__main__":
    unittest.main()
