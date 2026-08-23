#!/usr/bin/env python3
"""window.py 单测(flow-cost-ledger §2.1,权威窗口用例):WN1-WN4。

selftest() 迁移自 flowq --selftest 为权威(9 时段 + 7 建议);WN4 与宿主 flowq
同实现一致性(验收 7),flowq 文件缺失时 skip(宿主侧不入仓)。
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
    """WN1-WN2:selftest 权威 + 时段边界(09:00/12:00/14:00/18:00 整 + 周末全天)。"""

    def test_WN1_selftest(self):
        """window.selftest() 返回 0(时段 13 边界 + 建议 10 用例全过,迁移自 flowq)。"""
        self.assertEqual(window.selftest(), 0)

    def test_WN2_window_kind_edges(self):
        """工作日:09:00 整=peak 起、12:00 整=offpeak 起、14:00 整=peak 起、18:00 整=offpeak 起;
        周末(周六/周日)全天 offpeak。"""
        cases = [
            ("2026-08-24T01:00:00+00:00", "peak"),      # 周一 09:00 北京整
            ("2026-08-24T03:59:59+00:00", "peak"),      # 11:59:59
            ("2026-08-24T04:00:00+00:00", "offpeak"),   # 12:00 北京整
            ("2026-08-24T05:59:59+00:00", "offpeak"),   # 13:59:59
            ("2026-08-24T06:00:00+00:00", "peak"),      # 14:00 北京整
            ("2026-08-24T09:59:59+00:00", "peak"),      # 17:59:59
            ("2026-08-24T10:00:00+00:00", "offpeak"),   # 18:00 北京整
            ("2026-08-24T00:00:00+00:00", "offpeak"),   # 08:00 夜间
            ("2026-08-24T15:59:59+00:00", "offpeak"),   # 23:59:59
            # 周末全天谷价(旧规则这些点是 peak)
            ("2026-08-22T02:00:00+00:00", "offpeak"),   # 周六 10:00
            ("2026-08-22T07:00:00+00:00", "offpeak"),   # 周六 15:00
            ("2026-08-23T01:00:00+00:00", "offpeak"),   # 周日 09:00
            ("2026-08-23T10:00:00+00:00", "offpeak"),   # 周日 18:00
        ]
        for iso, expect in cases:
            self.assertEqual(window.window_kind(_utc(iso)), expect, iso)


class WindowSuggestTests(unittest.TestCase):
    """WN3:建议窗口决策表(P0 立即 / P1 短→午间或晚间 / P2 长→顺延 / 夜间与周末→现在可跑)。"""

    def _tsk(self, pri, exp):
        return {"priority": pri, "expected_seconds": exp}

    def test_WN3_window_suggest(self):
        cases = [
            (self._tsk("P0", 3600), "2026-08-24T02:00:00+00:00", "立即"),        # 周一 10:00
            (self._tsk("P1", 600), "2026-08-24T02:00:00+00:00", "12:00 午间"),   # 周一 10:00
            (self._tsk("P2", 3600), "2026-08-24T02:00:00+00:00", "18:00 晚间"),  # 周一 10:00
            (self._tsk("P1", 600), "2026-08-24T07:00:00+00:00", "18:00 晚间"),   # 周一 15:00
            (self._tsk("P1", 600), "2026-08-24T04:30:00+00:00", "现在可跑"),     # 12:30 午间
            (self._tsk("P2", 2400), "2026-08-24T05:50:00+00:00", "顺延18:00"),   # 13:50 剩10min
            (self._tsk("P2", 2400), "2026-08-24T14:00:00+00:00", "现在可跑"),    # 周一 22:00 夜间
            # 周末/周五晚:全天谷价,现在可跑
            (self._tsk("P2", 3600), "2026-08-22T02:00:00+00:00", "现在可跑"),    # 周六 10:00
            (self._tsk("P1", 600), "2026-08-23T07:00:00+00:00", "现在可跑"),     # 周日 15:00
            (self._tsk("P2", 3600), "2026-08-21T10:30:00+00:00", "现在可跑"),    # 周五 18:30
        ]
        for t, iso, expect in cases:
            self.assertEqual(window.window_suggest(t, _utc(iso)), expect, (t, iso))


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
        for iso in ("2026-08-24T00:00:00+00:00", "2026-08-24T01:00:00+00:00",
                    "2026-08-24T04:00:00+00:00", "2026-08-24T06:00:00+00:00",
                    "2026-08-24T10:00:00+00:00", "2026-08-24T15:59:59+00:00",
                    "2026-08-22T02:00:00+00:00", "2026-08-23T07:00:00+00:00"):
            dt = _utc(iso)
            self.assertEqual(window.window_kind(dt), fq_kind(dt), iso)
        t = {"priority": "P2", "expected_seconds": 2400}
        for iso in ("2026-08-24T02:00:00+00:00", "2026-08-24T05:50:00+00:00",
                    "2026-08-24T14:00:00+00:00", "2026-08-22T02:00:00+00:00"):
            self.assertEqual(window.window_suggest(t, _utc(iso)),
                             fq_suggest(t, _utc(iso)), iso)


if __name__ == "__main__":
    unittest.main()
