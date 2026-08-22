#!/usr/bin/env python3
"""window.py —— 峰谷时段窗口公共模块(flow-cost-ledger §1.8,防 DRY,D1)。

唯一权威实现:flowq 与 flow-task-core.py 均 import 本模块(flowq 宿主侧
`sys.path.insert(0, "<collab-flow>/scripts")` + `from window import ...`)。
纯函数、stdlib、零 I/O:判定显式转 Asia/Shanghai,不依赖系统时区。

规则 v2(2026-08-22,官方 api-docs.deepseek.com 确认):
  高峰: 北京 >=09:00 <12:00 或 >=14:00 <18:00
  空闲: 其余(12:00-14:00 午间、18:00-次日09:00 夜间),半价
入参一律 UTC(datetime 带 tz);naive datetime 按 UTC 处理(调用方负责带偏移)。

selftest() 为权威用例(迁移自 flowq --selftest):9 时段边界 + 7 建议窗口。
"""

from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
    TZ_CN = ZoneInfo("Asia/Shanghai")
except Exception:  # Python <3.9 或无 tzdata 兜底(本机 3.11+,理论不走)
    TZ_CN = timezone(timedelta(hours=8), name="Asia/Shanghai")

_PEAK_SPANS = ((9 * 60, 12 * 60), (14 * 60, 18 * 60))  # 北京分钟 [lo, hi)


def _cn_minutes(dt_utc):
    """UTC → 北京当天分钟数(0-1439)。"""
    t = dt_utc.astimezone(TZ_CN)
    return t.hour * 60 + t.minute


def window_kind(dt_utc):
    """peak / offpeak(边界:09:00 整起高峰,12:00/18:00 整起空闲)。"""
    m = _cn_minutes(dt_utc)
    return "peak" if any(lo <= m < hi for lo, hi in _PEAK_SPANS) else "offpeak"


def window_state_line(dt_utc):
    """状态栏:⛰️ 高峰 至 12:00(剩 2h30m)/ 🌙 空闲 至 14:00(剩 1h30m)。"""
    m = _cn_minutes(dt_utc)
    if window_kind(dt_utc) == "peak":
        end, end_s = (12 * 60, "12:00") if m < 12 * 60 else (18 * 60, "18:00")
        icon, kind = "⛰️", "高峰"
    elif m < 9 * 60:
        end, end_s, icon, kind = 9 * 60, "09:00", "🌙", "空闲"  # 00:00-09:00 夜间
    elif m < 14 * 60:
        end, end_s, icon, kind = 14 * 60, "14:00", "🌙", "空闲"  # 12:00-14:00 午间
    else:
        end, end_s, icon, kind = 33 * 60, "09:00(次日)", "🌙", "空闲"  # 18:00-次日09:00（24*60+9*60）
    remain = end - m
    hh, mm = divmod(remain, 60)
    return f"{icon} {kind} 至 {end_s}(剩 {hh}h{mm:02d}m)"


def offpeak_remaining_sec(dt_utc):
    """当前空闲窗口剩余秒数；高峰返回 0；夜间跨天按到次日 09:00 实算（不再返回固定大数）。"""
    if window_kind(dt_utc) != "offpeak":
        return 0.0
    m = _cn_minutes(dt_utc)
    t = dt_utc.astimezone(TZ_CN)
    if m < 9 * 60:
        return (9 * 60 - m) * 60 - t.second  # 00:00-09:00 → 09:00
    if m >= 18 * 60:
        return (33 * 60 - m) * 60 - t.second  # 18:00-次日09:00（跨天）
    return (14 * 60 - m) * 60 - t.second  # 午间窗口到 14:00


def next_offpeak_start(dt_utc):
    """最近未来空闲窗口起点(UTC)。当前已在空闲窗口则返回 now。"""
    if window_kind(dt_utc) == "offpeak":
        return dt_utc
    t = dt_utc.astimezone(TZ_CN)
    m = t.hour * 60 + t.minute
    if m < 12 * 60:
        start = t.replace(hour=12, minute=0, second=0, microsecond=0)
    else:  # 14:00-18:00 高峰
        start = t.replace(hour=18, minute=0, second=0, microsecond=0)
    return start.astimezone(timezone.utc)


def can_finish_in_window(dt_utc, expected_sec):
    """边界纪律:空闲窗口剩余 ≥ 预估×1.2 才算跑得完。"""
    return offpeak_remaining_sec(dt_utc) >= float(expected_sec) * 1.2


def window_suggest(task, dt_utc):
    """queued/scheduled 任务建议执行窗口(规则 v2 决策表:P0 立即 / P1 短→最近空闲 / 其余→晚间)。"""
    pri = (task.get("priority") or "P2").upper()
    if pri == "P0":
        return "立即"
    try:
        exp = float(task.get("expected_seconds") or 900)
    except (TypeError, ValueError):
        exp = 900.0
    if window_kind(dt_utc) == "offpeak":
        return "现在可跑" if can_finish_in_window(dt_utc, exp) else "顺延18:00"
    # peak 中:P1 短任务取最近空闲窗口(12:00 午间 / 18:00 晚间),其余锁晚间窗口
    if pri == "P1" and exp <= 15 * 60:
        return "12:00 午间" if _cn_minutes(dt_utc) < 12 * 60 else "18:00 晚间"
    return "18:00 晚间"


def selftest():
    """权威用例自检(迁移自 flowq --selftest):时段边界 + 建议窗口。全过返回 0。"""
    def utc(iso):
        return datetime.fromisoformat(iso)

    kind_cases = [
        ("2026-08-22T00:00:00+00:00", "offpeak"),  # 08:00 北京
        ("2026-08-22T01:00:00+00:00", "peak"),     # 09:00 整 = 高峰起
        ("2026-08-22T03:59:59+00:00", "peak"),     # 11:59:59
        ("2026-08-22T04:00:00+00:00", "offpeak"),  # 12:00 整 = 空闲起
        ("2026-08-22T05:59:59+00:00", "offpeak"),  # 13:59:59
        ("2026-08-22T06:00:00+00:00", "peak"),     # 14:00 整 = 高峰起
        ("2026-08-22T09:59:59+00:00", "peak"),     # 17:59:59
        ("2026-08-22T10:00:00+00:00", "offpeak"),  # 18:00 整 = 空闲起
        ("2026-08-22T15:59:59+00:00", "offpeak"),  # 23:59:59
    ]
    for iso, expect in kind_cases:
        got = window_kind(utc(iso))
        assert got == expect, f"window_kind({iso}) = {got}, 期望 {expect}"

    def tsk(pri, exp):
        return {"priority": pri, "expected_seconds": exp}

    sug_cases = [
        (tsk("P0", 3600), "2026-08-22T02:00:00+00:00", "立即"),          # 10:00 北京
        (tsk("P1", 600), "2026-08-22T02:00:00+00:00", "12:00 午间"),     # 10:00 北京
        (tsk("P2", 3600), "2026-08-22T02:00:00+00:00", "18:00 晚间"),    # 10:00 北京
        (tsk("P1", 600), "2026-08-22T07:00:00+00:00", "18:00 晚间"),     # 15:00 北京
        (tsk("P1", 600), "2026-08-22T04:30:00+00:00", "现在可跑"),       # 12:30 北京午间
        (tsk("P2", 2400), "2026-08-22T05:50:00+00:00", "顺延18:00"),     # 13:50 剩10min < 48min
        (tsk("P2", 2400), "2026-08-22T14:00:00+00:00", "现在可跑"),      # 22:00 北京夜间
    ]
    for t, iso, expect in sug_cases:
        got = window_suggest(t, utc(iso))
        assert got == expect, f"window_suggest({t}, {iso}) = {got}, 期望 {expect}"

    print(f"selftest OK({len(kind_cases)} 时段用例 + {len(sug_cases)} 建议用例)")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(selftest())
