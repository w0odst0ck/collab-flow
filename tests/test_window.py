#!/usr/bin/env python3
"""window.py 单测(flow-cost-ledger §2.1,权威窗口用例):WN1-WN4。

selftest() 为权威(用例从 config/pricing.yaml 自动生成——改计费配置后测试
自动跟随,不依赖具体日期);WN4 与宿主 flowq 同实现一致性(验收 7),
flowq 文件缺失时 skip(宿主侧不入仓)。
零 API、零网络;纯函数直接 import。
"""

import importlib.util
import os
import unittest
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
_WINDOW = os.path.join(_HERE, "..", "scripts", "window.py")

_spec = importlib.util.spec_from_file_location("window_test", _WINDOW)
window = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(window)


def _utc(iso):
    return datetime.fromisoformat(iso)


class WindowKindTests(unittest.TestCase):
    """WN1-WN2:selftest 权威 + 显式边界(config 生成 + 手工抽查)。"""

    def test_WN1_selftest(self):
        """window.selftest() 返回 0(时段+建议用例全过,从 config 自动生成)。"""
        self.assertEqual(window.selftest(), 0)

    def test_WN2_window_kind_edges(self):
        """显式抽查:工作日 span 边界(lo 起 peak / hi 起空闲)+ 周末全天 offpeak。
        期望值从 window 模块的 config 派生(不硬编码日期,防过期)。"""
        wk = window._anchor_day(0)   # 未来周一(北京)
        we = window._anchor_day(5)   # 未来周六(北京)
        lo0, hi0 = window._SPANS[0]
        # 工作日:span 起点 peak / span 终点空闲
        self.assertEqual(window.window_kind(
            wk.replace(hour=lo0, minute=0, tzinfo=window.TZ_CN).astimezone(timezone.utc)), "peak")
        self.assertEqual(window.window_kind(
            wk.replace(hour=hi0, minute=0, tzinfo=window.TZ_CN).astimezone(timezone.utc)), "offpeak")
        # 工作日 00:00 / 23:59 必空闲
        self.assertEqual(window.window_kind(
            wk.replace(hour=0, minute=0, tzinfo=window.TZ_CN).astimezone(timezone.utc)), "offpeak")
        self.assertEqual(window.window_kind(
            wk.replace(hour=23, minute=59, tzinfo=window.TZ_CN).astimezone(timezone.utc)), "offpeak")
        # 周末:weekday_only 时全天 offpeak(含原高峰时段)
        if window._WEEKDAY_ONLY:
            for h, mi in ((lo0, 0), (hi0 - 1, 59)):
                self.assertEqual(window.window_kind(
                    we.replace(hour=h, minute=mi, tzinfo=window.TZ_CN).astimezone(timezone.utc)),
                    "offpeak", f"周末 {h}:{mi} 应全天 offpeak")


class WindowSuggestTests(unittest.TestCase):
    """WN3:建议窗口决策表(P0 立即 / P1 短→最近空闲 / P2 长→顺延 / 空闲→现在可跑)。"""

    def _tsk(self, pri, exp):
        return {"priority": pri, "expected_seconds": exp}

    def test_WN3_window_suggest(self):
        wk = window._anchor_day(0)
        lo0, hi0 = window._SPANS[0]
        lo1, hi1 = window._SPANS[-1]
        # 前置校验:config 若把 span 起点改到 0 点,lo-1 会越界(防晦涩 ValueError)
        self.assertGreater(lo0, 0, "span[0] 起点需 >0(测试用 lo0-1 构造早间)")
        self.assertGreater(lo1, 0, "末 span 起点需 >0(测试用 lo1-1 构造午间尾)")
        mid = window._mid_label()
        eve = window._eve_label()

        def at(h, mi):
            return wk.replace(hour=h, minute=mi, tzinfo=window.TZ_CN).astimezone(timezone.utc)

        cases = [
            (self._tsk("P0", 3600), at(lo0 + 1, 0), "立即"),                          # 工作日 peak 内
            (self._tsk("P1", 600), at(lo0 + 1, 0), mid),                              # 上午 peak → 午间
            (self._tsk("P2", 3600), at(lo0 + 1, 0), eve),                             # 上午 peak → 晚间
            (self._tsk("P1", 600), at(lo1 + 1, 0), eve),                              # 下午 peak → 晚间
            (self._tsk("P1", 600), at(hi0, 30), "现在可跑"),                          # 午间窗口内
            (self._tsk("P2", 2400), at(lo1 - 1, 50), f"顺延今日 {window._fmt_min(hi1 * 60)}"),  # 午间尾→晚间
            (self._tsk("P1", 600), at(lo0 - 1, 50), f"顺延今日 {window._fmt_min(hi0 * 60)}"),  # 早间尾→午间
            (self._tsk("P2", 2400), at(22, 0), "现在可跑"),                           # 工作日夜间
        ]
        if window._WEEKDAY_ONLY:
            we = window._anchor_day(5)
            cases += [
                (self._tsk("P2", 3600), we.replace(hour=lo0 + 1, minute=0, tzinfo=window.TZ_CN)
                 .astimezone(timezone.utc), "现在可跑"),                              # 周六
                (self._tsk("P2", 3600), window._anchor_day(4)
                 .replace(hour=hi1, minute=30, tzinfo=window.TZ_CN)
                 .astimezone(timezone.utc), "现在可跑"),                              # 周五晚间(连周末)
            ]
        for t, dt, expect in cases:
            self.assertEqual(window.window_suggest(t, dt), expect, (t, dt))


class WindowConsistencyTests(unittest.TestCase):
    """WN4:与 flowq 宿主同实现一致性(验收 7);flowq 缺失/未迁移公共模块时 skip。"""

    def test_WN4_import_consistency(self):
        # 用 passwd 数据库取真实 home(不依赖 HOME env——既有测试基类可能泄漏临时 HOME)
        try:
            import pwd
            real_home = pwd.getpwuid(os.getuid()).pw_dir
        except (ImportError, KeyError, AttributeError):
            real_home = os.path.expanduser("~")
        q = os.path.join(real_home, ".local", "bin", "flowq")
        if not os.path.isfile(q):
            self.skipTest("flowq 宿主文件缺失(宿主侧不入仓)")
        from importlib.machinery import SourceFileLoader
        loader = SourceFileLoader("flowq", q)
        spec = importlib.util.spec_from_loader("flowq", loader)
        fq = importlib.util.module_from_spec(spec)
        loader.exec_module(fq)
        # 验收 7 的前提是宿主已引用公共模块(§1.8 sys.path + from window import …);
        # flowq 实际以别名导入(window_kind as _window_kind),两个名字都认;
        # 未迁移(内联私有实现)时无此属性 → skip,避免误报。
        fq_kind = getattr(fq, "window_kind", None) or getattr(fq, "_window_kind", None)
        fq_suggest = getattr(fq, "window_suggest", None) or getattr(fq, "_window_suggest", None)
        if fq_kind is None or fq_suggest is None:
            self.skipTest("flowq 宿主未迁移公共窗口模块(验收 7 待宿主侧实施,不入仓)")
        # 抽查:config 生成用例中的代表性时间点(取 selftest 用例,验证宿主一致)
        for iso, _expect in window._gen_kind_cases()[::3]:
            dt = _utc(iso)
            self.assertEqual(window.window_kind(dt), fq_kind(dt), iso)
        for t, iso, _expect in window._gen_sug_cases():
            self.assertEqual(window.window_suggest(t, _utc(iso)),
                             fq_suggest(t, _utc(iso)), iso)


if __name__ == "__main__":
    unittest.main()
