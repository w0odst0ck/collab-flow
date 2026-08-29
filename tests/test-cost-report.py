#!/usr/bin/env python3
"""cost-report.py 单测(cost-weekly-report 设计 §3.2 + E 表):W1-W16。

覆盖:周统计聚合 / 周边界(半开区间)/ 多周 / 项目·执行器·模型分布 / 失败成本 /
峰谷节省(R 来自 config 非硬编码,window 为唯一权威)/ 空·缺失 / 汇率·周起始覆盖 /
--json 契约 / --push stub(OK→0,失败→3,缺失→2,截断,JSON+push 组合)/ E1-E17。

测试隔离:FLOW_TASK_DIR 指向临时目录;FEISHU_NOTIFY 指向 stub(零真实凭证/网络)。
峰谷判定用真实 window.py;R 推导用 patch get_prices 验证「来自 config 非硬编码」。
"""

import contextlib
import importlib.util
import io
import json
import os
import subprocess
import tempfile
import unittest
import warnings
from datetime import datetime, timedelta, timezone
from unittest import mock

_HERE = os.path.dirname(os.path.abspath(__file__))
_SCRIPT = os.path.join(_HERE, "..", "scripts", "cost-report.py")

_spec = importlib.util.spec_from_file_location("cost_report", _SCRIPT)
cr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cr)

TZ_CN = timezone(timedelta(hours=8))
NOW = datetime(2026, 8, 31, 1, 0, 0, tzinfo=TZ_CN)  # 2026-08-31 周一(上周 = 08-24 ~ 08-31)
CFG = {"exchange_rate": 7.2, "week_start_dow": 0, "week_timezone": "Asia/Shanghai"}


def _reg(*tasks):
    return {"schema_version": 1, "tasks": {f"t-{i}": t for i, t in enumerate(tasks)}}


def _task(**kw):
    base = {"id": "x", "state": "done", "workdir": "/home/l/.openclaw/workspace/projects/collab-flow",
            "executor": "reasonix", "model": "deepseek-v4-flash",
            "created_at": "2026-08-25T00:00:00+08:00",
            "started_at": "2026-08-25T10:00:00+08:00",
            "finished_at": "2026-08-25T10:30:00+08:00", "cost_usd": 0.5}
    base.update(kw)
    return base


def _bounds(now=NOW, weeks=1, dow=0, tz="Asia/Shanghai"):
    return cr.week_bounds(now, tz, dow, weeks)


def _last_week_ts(days=3, hour=10):
    """真实 now 的上周区间内时间点 ISO(+08:00):走 main() 的测试用(周界随真实日历)。"""
    s, e = cr.week_bounds(datetime.now(timezone.utc), "Asia/Shanghai", 0, 1)[0]
    mid = s + timedelta(days=days, hours=hour)
    assert s <= mid < e, f"测试时间点 {mid} 不在上周区间 [{s}, {e})"
    return mid.isoformat()


class LoadRegistryTests(unittest.TestCase):
    """E1-E3:缺失/损坏/schema 非法。"""

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.addCleanup(self._td.cleanup)
        self.dir = self._td.name

    def test_missing(self):
        """E1:目录/文件缺失 → 空数据集(不抛)。"""
        self.assertEqual(cr.load_registry(os.path.join(self.dir, "nope"))["tasks"], {})

    def test_corrupt_json(self):
        """E2:非 JSON → CostReportError。"""
        with open(os.path.join(self.dir, "tasks.json"), "w") as f:
            f.write("{not json")
        with self.assertRaises(cr.CostReportError):
            cr.load_registry(self.dir)

    def test_bad_schema(self):
        """E3:schema_version 非法 / 顶层非 dict / tasks 非 dict → CostReportError。"""
        for payload in ({"schema_version": 99, "tasks": {}}, [], {"schema_version": 1, "tasks": []}):
            with open(os.path.join(self.dir, "tasks.json"), "w") as f:
                json.dump(payload, f)
            with self.assertRaises(cr.CostReportError):
                cr.load_registry(self.dir)


class WeekWindowTests(unittest.TestCase):
    """周窗口/归属口径(§1.2):半开区间 + ts 回退。"""

    def test_week_boundary(self):
        """finished_at=周一 00:00 属新周、周日 23:59:59 属旧周(半开区间 [周一00:00, 次周一00:00))。"""
        bounds = _bounds()  # [(2026-08-24 00:00+08, 2026-08-31 00:00+08)]
        self.assertEqual(len(bounds), 1)
        mon = _task(finished_at="2026-08-31T00:00:00+08:00")   # 新周起点
        sun = _task(finished_at="2026-08-30T23:59:59+08:00")   # 上周最后一秒
        ts_m, idx_m = cr.bucket_task(mon, bounds)
        ts_s, idx_s = cr.bucket_task(sun, bounds)
        self.assertIsNone(idx_m, "周一 00:00 应属新周(不在上周区间内)")
        self.assertEqual(idx_s, 0, "周日 23:59:59 应属上周")

    def test_ts_fallback(self):
        """ts = finished_at || started_at || created_at(首个非空可解析)。"""
        bounds = _bounds()
        t1 = _task(finished_at="garbage", started_at="2026-08-26T09:00:00+08:00")
        ts1, idx1 = cr.bucket_task(t1, bounds)
        self.assertEqual(idx1, 0)
        self.assertEqual(ts1.isoformat(), "2026-08-26T09:00:00+08:00")
        t2 = _task(finished_at=None, started_at=None, created_at="2026-08-27T08:00:00+08:00")
        _, idx2 = cr.bucket_task(t2, bounds)
        self.assertEqual(idx2, 0)

    def test_multi_weeks(self):
        """--weeks 2 恰好覆盖两周、不计当前未完成周。"""
        bounds = _bounds(weeks=2)
        self.assertEqual(len(bounds), 2)
        self.assertEqual(bounds[0][0].date().isoformat(), "2026-08-17")
        self.assertEqual(bounds[1][0].date().isoformat(), "2026-08-24")
        self.assertEqual(bounds[1][1].date().isoformat(), "2026-08-31")
        older = _task(started_at="2026-08-19T10:00:00+08:00", finished_at="2026-08-19T11:00:00+08:00")
        last = _task(started_at="2026-08-28T10:00:00+08:00", finished_at="2026-08-28T11:00:00+08:00")
        cur = _task(started_at="2026-09-02T10:00:00+08:00", finished_at=None)  # 未终态
        self.assertEqual(cr.bucket_task(older, bounds)[1], 0)
        self.assertEqual(cr.bucket_task(last, bounds)[1], 1)
        ts_c, idx_c = cr.bucket_task(cur, bounds)
        self.assertIsNone(idx_c, "当前未完成周不计入")

    def test_bad_timestamp(self):
        """E5:三个时间戳全非法/缺失 → 不计入周、计入 untimed。"""
        bounds = _bounds()
        t = _task(created_at="bad", started_at=None, finished_at=None)
        self.assertIsNone(cr.bucket_task(t, bounds))
        report = cr.build_report(_reg(t), CFG, bounds)
        self.assertEqual(report["weeks"][0]["untimed_tasks"], 1)
        self.assertEqual(report["weeks"][0]["tasks"]["total"], 0)


class BucketTests(unittest.TestCase):
    """§1.3 分桶 + E4/E13 容错。"""

    def test_by_project(self):
        self.assertEqual(cr.project_of("/home/l/.openclaw/workspace/projects/collab-flow"), "collab-flow")
        self.assertEqual(cr.project_of("/tmp/x"), "未分类")
        self.assertEqual(cr.project_of(None), "未分类")

    def test_by_executor(self):
        for e in ("reasonix", "script", "local"):
            self.assertEqual(cr.executor_bucket({"executor": e}), e)
        self.assertEqual(cr.executor_bucket({"executor": None}), "其他")
        self.assertEqual(cr.executor_bucket({}), "其他")
        self.assertEqual(cr.executor_bucket({"executor": "ollama"}), "其他")

    def test_by_model(self):
        self.assertEqual(cr.model_bucket({"model": "deepseek-v4-flash"}), "flash")
        self.assertEqual(cr.model_bucket({"model": "deepseek-v4-pro"}), "pro")
        self.assertEqual(cr.model_bucket({"model": "qwen3:9b"}), "本地")
        self.assertEqual(cr.model_bucket({"executor": "local", "model": None}), "本地")
        self.assertEqual(cr.model_bucket({"executor": "local", "model": ""}), "本地")
        self.assertEqual(cr.model_bucket({"model": None, "executor": "reasonix"}), "未知")
        self.assertEqual(cr.model_bucket({}), "未知")

    def test_partial_fields(self):
        """E4:单条字段缺失 → 兜底桶,不崩。"""
        report = cr.build_report(
            _reg({"id": "t0", "state": "killed", "cost_usd": 0.1,
                  "created_at": "2026-08-25T00:00:00+08:00"}), CFG, _bounds())
        w = report["weeks"][0]
        self.assertEqual(w["tasks"]["other"], 1)
        self.assertEqual(w["by_project"][0]["project"], "未分类")
        self.assertEqual(w["by_executor"][0]["executor"], "其他")
        self.assertEqual(w["by_model"][0]["model"], "未知")


class CostAggregationTests(unittest.TestCase):
    """周统计聚合 / 失败成本 / unbilled / 坏成本。"""

    def test_weekly_aggregation(self):
        t1 = _task(state="done", cost_usd=0.5)
        t2 = _task(state="failed", cost_usd=0.3, started_at="2026-08-26T09:00:00+08:00",
                   finished_at="2026-08-26T09:10:00+08:00")
        t3 = _task(state="timeout", cost_usd=0.2, started_at="2026-08-27T14:00:00+08:00",
                   finished_at="2026-08-27T15:00:00+08:00")
        t4 = _task(state="running", cost_usd=None)  # → other + unbilled
        report = cr.build_report(_reg(t1, t2, t3, t4), CFG, _bounds())
        w = report["weeks"][0]
        self.assertEqual(w["tasks"], {"total": 4, "done": 1, "failed": 1, "timeout": 1, "other": 1})
        self.assertAlmostEqual(w["cost_usd"], 1.0)
        self.assertAlmostEqual(w["cost_cny"], 7.2)
        self.assertEqual(w["unbilled_tasks"], 1)

    def test_failed_cost(self):
        """failed+timeout 成本单独求和。"""
        t1 = _task(state="done", cost_usd=0.5)
        t2 = _task(state="failed", cost_usd=0.3, started_at="2026-08-26T09:00:00+08:00")
        t3 = _task(state="timeout", cost_usd=0.2, started_at="2026-08-27T14:00:00+08:00")
        report = cr.build_report(_reg(t1, t2, t3), CFG, _bounds())
        w = report["weeks"][0]
        self.assertAlmostEqual(w["failed_cost_usd"], 0.5)
        self.assertAlmostEqual(w["failed_cost_cny"], 3.6)

    def test_unbilled(self):
        """E11:cost_usd null → 计 0 成本 + 计入 unbilled。"""
        report = cr.build_report(_reg(_task(cost_usd=None)), CFG, _bounds())
        w = report["weeks"][0]
        self.assertEqual(w["unbilled_tasks"], 1)
        self.assertAlmostEqual(w["cost_usd"], 0.0)

    def test_bad_cost(self):
        """E12:非数值忽略+计数(unbilled);负数按 0(不虚减)。"""
        report = cr.build_report(_reg(_task(cost_usd="abc"), _task(cost_usd=-2.0)), CFG, _bounds())
        w = report["weeks"][0]
        self.assertAlmostEqual(w["cost_usd"], 0.0)
        self.assertEqual(w["unbilled_tasks"], 1)   # 非数值 → unbilled
        self.assertEqual(w["tasks"]["total"], 2)   # 任务数照常计数

    def test_by_distributions(self):
        """项目/执行器/模型分布:按 cost 降序、任务数+成本。"""
        a = _task(workdir="/home/l/.openclaw/workspace/projects/collab-flow", cost_usd=0.6)
        b = _task(workdir="/tmp/other", executor="script", model="qwen3:9b", cost_usd=0.3,
                  started_at="2026-08-27T09:00:00+08:00")
        report = cr.build_report(_reg(a, b), CFG, _bounds())
        w = report["weeks"][0]
        self.assertEqual([r["project"] for r in w["by_project"]], ["collab-flow", "未分类"])
        self.assertEqual(w["by_project"][0]["cost_usd"], 0.6)
        self.assertEqual(w["by_project"][1]["cost_usd"], 0.3)
        self.assertEqual([r["executor"] for r in w["by_executor"]], ["reasonix", "script"])
        self.assertEqual([r["model"] for r in w["by_model"]], ["flash", "本地"])


class SavingsTests(unittest.TestCase):
    """峰谷节省(§1.4):R 来自 config 非硬编码;window 唯一权威。"""

    def _bounds(self):
        return _bounds()

    def test_peak_ratio_from_config(self):
        """E9/R 推导:prices 缺失/分母 0/peak<offpeak 错价 → 兜底 2.0;存在 → peak/offpeak。"""
        with mock.patch.object(cr.window, "get_prices", return_value={}):
            self.assertEqual(cr.peak_ratio(), 2.0)
        with mock.patch.object(cr.window, "get_prices",
                               return_value={"flash": {"input_miss": {"peak": 0, "offpeak": 0}}}):
            self.assertEqual(cr.peak_ratio(), 2.0)
        with mock.patch.object(cr.window, "get_prices",
                               return_value={"flash": {"input_miss": {"peak": 4.0, "offpeak": 1.0}}}):
            self.assertEqual(cr.peak_ratio(), 4.0)   # 来自 config,非硬编码
        # 错价配置 peak<offpeak → 拒绝负节省,兜底 2.0
        with mock.patch.object(cr.window, "get_prices",
                               return_value={"flash": {"input_miss": {"peak": 1.0, "offpeak": 2.0}}}):
            self.assertEqual(cr.peak_ratio(), 2.0)
        # inf/nan 非有限值 → 兜底 2.0,不污染节省口径/JSON 输出
        with mock.patch.object(cr.window, "get_prices",
                               return_value={"flash": {"input_miss": {"peak": float("inf"), "offpeak": 1.0}}}):
            self.assertEqual(cr.peak_ratio(), 2.0)
        with mock.patch.object(cr.window, "get_prices",
                               return_value={"flash": {"input_miss": {"peak": float("nan"), "offpeak": 1.0}}}):
            self.assertEqual(cr.peak_ratio(), 2.0)

    def test_peak_savings_offpeak(self):
        """offpeak 任务 cost×(R−1),R 来自 config(此处 R=4 → 节省 cost×3)。"""
        with mock.patch.object(cr.window, "get_prices",
                               return_value={"flash": {"input_miss": {"peak": 4.0, "offpeak": 1.0}}}), \
             mock.patch.object(cr.window, "window_kind", return_value="offpeak"):
            report = cr.build_report(_reg(_task(cost_usd=1.0)), CFG, self._bounds())
            w = report["weeks"][0]
            self.assertAlmostEqual(w["peak_savings_usd"], 3.0)
            self.assertAlmostEqual(w["peak_savings_cny"], 21.6)
            self.assertEqual(w["peak_savings_tasks"], 1)

    def test_peak_savings_peak(self):
        """peak 任务节省 0。"""
        with mock.patch.object(cr.window, "window_kind", return_value="peak"):
            self.assertEqual(
                cr.savings_usd({"cost_usd": 1.0, "started_at": "2026-08-19T10:00:00+08:00"}, 4.0), 0.0)

    def test_savings_requires_cost(self):
        """cost null → 不计节省、计入 unbilled。"""
        with mock.patch.object(cr.window, "window_kind", return_value="offpeak"):
            self.assertEqual(
                cr.savings_usd({"cost_usd": None, "started_at": "2026-08-19T10:00:00+08:00"}, 2.0), 0.0)
            report = cr.build_report(_reg(_task(cost_usd=None)), CFG, self._bounds())
            w = report["weeks"][0]
            self.assertAlmostEqual(w["peak_savings_usd"], 0.0)
            self.assertEqual(w["unbilled_tasks"], 1)

    def test_savings_requires_started_at(self):
        """started_at 缺失 → 不算节省(保守 0)。"""
        with mock.patch.object(cr.window, "window_kind", return_value="offpeak"):
            self.assertEqual(cr.savings_usd({"cost_usd": 1.0, "started_at": None}, 2.0), 0.0)

    def test_window_import_fallback(self):
        """E10:window import 失败 → 节省降级 0 + warning + 其余统计照常。"""
        saved = cr.window
        cr.window = None
        cr._WINDOW_FALLBACK_WARNED = False   # 重置一次性标志,保证本用例独立
        try:
            with warnings.catch_warnings(record=True) as wl:
                warnings.simplefilter("always")
                self.assertEqual(cr.peak_ratio(), 2.0)
                self.assertEqual(
                    cr.savings_usd({"cost_usd": 1.0, "started_at": "2026-08-19T10:00:00+08:00"}, 2.0), 0.0)
                report = cr.build_report(_reg(_task(cost_usd=1.0)), CFG, self._bounds())
                self.assertAlmostEqual(report["weeks"][0]["peak_savings_usd"], 0.0)
                self.assertAlmostEqual(report["weeks"][0]["cost_usd"], 1.0)  # 成本统计不受影响
            self.assertTrue(any("window 模块不可用" in str(x.message) for x in wl),
                            f"期望 E10 warning, 实际: {[str(x.message) for x in wl]}")
        finally:
            cr.window = saved
            cr._WINDOW_FALLBACK_WARNED = False

    def test_cross_window_annotation(self):
        """§1.4.5:start 与 finish 跨峰谷 → 计数 + 文本标注「跨峰谷按起始窗口计」。"""
        crossing = _task(cost_usd=1.0,
                         started_at="2026-08-25T09:00:00+08:00",   # 周一 09:00 = peak
                         finished_at="2026-08-25T13:00:00+08:00")  # 13:00 = offpeak
        same = _task(cost_usd=0.5,
                     started_at="2026-08-25T09:00:00+08:00",
                     finished_at="2026-08-25T10:00:00+08:00")     # 同窗口(peak→peak)
        report = cr.build_report(_reg(crossing, same), CFG, self._bounds())
        w = report["weeks"][0]
        self.assertEqual(w["cross_window_tasks"], 1)
        text = cr.render_text(report)
        self.assertIn("跨峰谷按起始窗口计 (1 条)", text)
        # 无跨峰谷任务 → 不标注
        plain = cr.build_report(_reg(same), CFG, self._bounds())
        self.assertEqual(plain["weeks"][0]["cross_window_tasks"], 0)
        self.assertNotIn("跨峰谷", cr.render_text(plain))


class ConfigTests(unittest.TestCase):
    """§1.5 配置优先级 + E7/E8 校验。"""

    def test_config_defaults(self):
        """默认值:defaults.yaml + 内置默认(env 全清空,用户 config 指向不存在路径,防泄漏)。"""
        with mock.patch.dict(os.environ,
                             {"COLLABFLOW_CONFIG": "/nonexistent/cr-defaults-test.yaml"},
                             clear=True):
            cfg = cr.load_budget_config()
        self.assertEqual(cfg["exchange_rate"], 7.2)
        self.assertEqual(cfg["week_start_dow"], 0)
        self.assertEqual(cfg["week_timezone"], "Asia/Shanghai")

    def test_exchange_rate_env(self):
        """env 覆盖生效。"""
        with mock.patch.dict(os.environ,
                             {"COST_EXCHANGE_RATE": "6.5",
                              "COLLABFLOW_CONFIG": "/nonexistent/cr-config.yaml"},
                             clear=True):
            cfg = cr.load_budget_config()
            self.assertEqual(cfg["exchange_rate"], 6.5)
            report = cr.build_report(_reg(_task(cost_usd=1.0)), cfg, _bounds())
            self.assertAlmostEqual(report["weeks"][0]["cost_cny"], 6.5)

    def test_week_start_env(self):
        """周起始覆盖生效(周日=6)。"""
        with mock.patch.dict(os.environ,
                             {"COST_WEEK_START_DOW": "6",
                              "COLLABFLOW_CONFIG": "/nonexistent/cr-config.yaml"},
                             clear=True):
            cfg = cr.load_budget_config()
            self.assertEqual(cfg["week_start_dow"], 6)
        bounds = cr.week_bounds(NOW, "Asia/Shanghai", 6, 1)
        self.assertEqual(bounds[0][0].weekday(), 6)   # 周界为周日 00:00

    def test_bad_rate(self):
        """E7:exchange_rate 非正数/非数字 → exit 2。"""
        for bad in ("0", "-1", "abc"):
            with mock.patch.dict(os.environ,
                                 {"COST_EXCHANGE_RATE": bad,
                                  "FLOW_TASK_DIR": "/tmp/nonexistent-cr-test",
                                  "COLLABFLOW_CONFIG": "/nonexistent/cr-config.yaml"},
                                 clear=True):
                with self.assertRaises(cr.CostReportError):
                    cr.load_budget_config()

    def test_bad_week_cfg(self):
        """E8:dow 越界 → exit 2;时区不存在 → 回退默认 + warning 不崩。"""
        with mock.patch.dict(os.environ, {"COST_WEEK_START_DOW": "7",
                                          "COLLABFLOW_CONFIG": "/nonexistent/cr-config.yaml"},
                             clear=True):
            with self.assertRaises(cr.CostReportError):
                cr.load_budget_config()
        # 时区不存在:week_bounds 抛,main 回退 Asia/Shanghai → exit 0
        with mock.patch.dict(os.environ,
                             {"COST_WEEK_TZ": "No/SuchZone",
                              "FLOW_TASK_DIR": "/tmp/nonexistent-cr-test",
                              "COLLABFLOW_CONFIG": "/nonexistent/cr-config.yaml"},
                             clear=True), \
             contextlib.redirect_stdout(io.StringIO()), \
             contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(cr.main(["--json"]), 0)


class CliAndJsonTests(unittest.TestCase):
    """CLI 退出码 + --json 契约 + 空/缺失。"""

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.addCleanup(self._td.cleanup)
        self.dir = self._td.name

    def _write_tasks(self, tasks):
        with open(os.path.join(self.dir, "tasks.json"), "w") as f:
            json.dump(_reg(*tasks), f)

    def _bounds_now(self):
        return cr.week_bounds(datetime.now(timezone.utc), "Asia/Shanghai", 0, 1)[0]

    def _main(self, argv, env=None):
        out = io.StringIO()
        err = io.StringIO()
        # clear=True:隔绝宿主 COST_*/COLLABFLOW_CONFIG 与真实用户 config,保证可复现
        full_env = {"FLOW_TASK_DIR": self.dir,
                    "COLLABFLOW_CONFIG": "/nonexistent/cr-config.yaml",
                    "PATH": os.environ.get("PATH", "/usr/bin:/bin")}
        if env:
            full_env.update(env)
        with mock.patch.dict(os.environ, full_env, clear=True), \
             contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cr.main(argv)
        return code, out.getvalue(), err.getvalue()

    def test_empty(self):
        """E6:无符合周窗口的任务 → empty=true、exit 0(成本全 0 结构)。"""
        future = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()  # 区间外(当前/未来周)
        self._write_tasks([_task(started_at=future, finished_at=None)])
        code, out, _ = self._main(["--json"])
        self.assertEqual(code, 0)
        report = json.loads(out)
        self.assertTrue(report["empty"])
        w = report["weeks"][0]
        self.assertEqual(w["cost_usd"], 0.0)
        self.assertEqual(w["tasks"]["total"], 0)

    def test_missing(self):
        """E1:目录/文件缺失 → 无数据、exit 0。"""
        code, out, _ = self._main(["--json"], env={"FLOW_TASK_DIR": os.path.join(self.dir, "nope")})
        self.assertEqual(code, 0)
        self.assertTrue(json.loads(out)["empty"])
        code2, out2, _ = self._main([])  # 文本模式 → 「无数据」
        self.assertEqual(code2, 0)
        self.assertEqual(out2.strip(), "无数据")

    def test_corrupt_json_exit2(self):
        with open(os.path.join(self.dir, "tasks.json"), "w") as f:
            f.write("{oops")
        code, _, err = self._main(["--json"])
        self.assertEqual(code, 2)
        self.assertIn("损坏", err)

    def test_bad_schema_exit2(self):
        with open(os.path.join(self.dir, "tasks.json"), "w") as f:
            json.dump({"schema_version": 99, "tasks": {}}, f)
        code, _, err = self._main(["--json"])
        self.assertEqual(code, 2)
        self.assertIn("schema_version", err)

    def test_bad_weeks(self):
        """E17:--weeks 0 / 非整数 → exit 2。"""
        for argv in (["--weeks", "0"], ["--weeks", "-3"], ["--weeks", "abc"]):
            with self.assertRaises(SystemExit) as cm:
                cr.main(argv)
            self.assertEqual(cm.exception.code, 2)

    def test_json_schema(self):
        """--json 输出与 §2.3 一致(键齐全、可解析)。"""
        self._write_tasks([_task(finished_at=None, started_at=_last_week_ts())])
        code, out, _ = self._main(["--json"])
        self.assertEqual(code, 0)
        report = json.loads(out)
        for key in ("generated_at", "empty", "exchange_rate", "weeks"):
            self.assertIn(key, report)
        w = report["weeks"][0]
        for key in ("week_start", "week_end", "tasks", "cost_usd", "cost_cny",
                    "failed_cost_usd", "failed_cost_cny", "peak_savings_usd", "peak_savings_cny",
                    "unbilled_tasks", "untimed_tasks", "by_project", "by_executor", "by_model"):
            self.assertIn(key, w)
        s, e = self._bounds_now()
        self.assertEqual(w["week_start"], s.date().isoformat())
        self.assertEqual(w["week_end"], e.date().isoformat())
        self.assertEqual(w["by_project"][0]["project"], "collab-flow")

    def test_text_report(self):
        """文本周报布局(§2.4 要点)。"""
        self._write_tasks([_task(finished_at=None, started_at=_last_week_ts())])
        code, out, _ = self._main([])
        self.assertEqual(code, 0)
        s, e = self._bounds_now()
        cover = (e - timedelta(days=1)).date().isoformat()
        self.assertIn(f"成本周报 {s.date().isoformat()} ~ {cover}", out)
        self.assertIn("任务: 总数 1 (done 1 / failed 0 / timeout 0 / 其他 0)", out)
        self.assertIn("总成本: ¥3.60 (US$0.50 × 7.2)   未落账: 0 条", out)
        self.assertIn("失败成本: ¥0.00 (US$0.00)", out)
        self.assertIn("峰谷节省: ¥0.00", out)
        self.assertIn("项目: collab-flow 1 任务 ¥3.60", out)


class PushTests(unittest.TestCase):
    """--push:stub feishu-notify.sh,零真实凭证/网络(E14-E18)。"""

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.addCleanup(self._td.cleanup)
        self.dir = self._td.name
        self.out_file = os.path.join(self.dir, "pushed.txt")
        with open(os.path.join(self.dir, "tasks.json"), "w") as f:
            json.dump(_reg(_task(finished_at=None, started_at=_last_week_ts())), f)

    def _stub(self, mark="推送 OK", body='cat > "$STUB_OUT"'):
        stub = os.path.join(self.dir, "feishu-stub.sh")
        with open(stub, "w") as f:
            f.write(f"#!/usr/bin/env bash\n{body}\necho '{mark}'\n")
        os.chmod(stub, 0o755)
        return stub

    def _run(self, argv, stub):
        out = io.StringIO()
        err = io.StringIO()
        with mock.patch.dict(os.environ,
                             {"FLOW_TASK_DIR": self.dir,
                              "FEISHU_NOTIFY": stub,
                              "STUB_OUT": self.out_file,
                              "COLLABFLOW_CONFIG": "/nonexistent/cr-config.yaml",
                              "PATH": os.environ.get("PATH", "/usr/bin:/bin")},
                             clear=True), \
             contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cr.main(argv)
        return code, out.getvalue(), err.getvalue()

    def test_push_ok(self):
        """推送 OK → exit 0,推送内容为文本周报。"""
        stub = self._stub()
        code, out, _ = self._run(["--push"], stub)
        self.assertEqual(code, 0)
        self.assertIn("成本周报", out)
        with open(self.out_file) as f:
            pushed = f.read()
        self.assertIn("成本周报", pushed)
        self.assertNotIn("推送 OK", pushed)   # echo 走 stdout,不回写 STUB_OUT

    def test_push_fail(self):
        """E15:推送失败(无「推送 OK」信号)→ exit 3,周报照常打印。"""
        stub = self._stub(mark="token 无效")
        code, out, err = self._run(["--push"], stub)
        self.assertEqual(code, 3)
        self.assertIn("成本周报", out)
        self.assertIn("推送失败", err)

    def test_push_not_executable(self):
        """E14:exec 时才暴露的不可执行(FileNotFoundError)→ CostReportError(exit 2)。"""
        stub = self._stub()
        with mock.patch("subprocess.run",
                        side_effect=FileNotFoundError(2, "No such file or directory")):
            with self.assertRaises(cr.CostReportError):
                cr.push_text("hello", stub)

    def test_push_timeout(self):
        """E19:推送网络超时 → 归 E15 路径(失败,exit 3)。"""
        stub = self._stub()
        with mock.patch("subprocess.run",
                        side_effect=subprocess.TimeoutExpired("feishu-notify", 90)):
            ok, reason = cr.push_text("hello", stub)
        self.assertFalse(ok)
        self.assertIn("超时", reason)

    def test_push_missing(self):
        """E14:feishu-notify.sh 缺失 → exit 2。"""
        code, _, err = self._run(["--push"], os.path.join(self.dir, "no-such-stub.sh"))
        self.assertEqual(code, 2)
        self.assertIn("feishu-notify.sh 缺失", err)

    def test_push_truncate(self):
        """E16:超长截断 + 末尾「(已截断)」,不崩。"""
        stub = self._stub()
        text = "x" * 5000
        with mock.patch.dict(os.environ, {"STUB_OUT": self.out_file}):
            ok, _ = cr.push_text(text, stub)
        self.assertTrue(ok)
        with open(self.out_file) as f:
            sent = f.read()
        self.assertEqual(len(sent), cr._PUSH_MAX_CHARS)
        self.assertTrue(sent.endswith("(已截断)"))

    def test_json_push_combo(self):
        """E18:--json + --push → stdout=JSON,推送=文本,互不污染。"""
        stub = self._stub()
        code, out, _ = self._run(["--json", "--push"], stub)
        self.assertEqual(code, 0)
        report = json.loads(out)          # stdout 是 JSON
        self.assertIn("weeks", report)
        with open(self.out_file) as f:
            pushed = f.read()
        self.assertIn("成本周报", pushed)  # 推送内容始终为文本
        self.assertNotIn('"weeks"', pushed)


if __name__ == "__main__":
    unittest.main()
