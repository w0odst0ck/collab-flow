#!/usr/bin/env python3
"""window.py —— 峰谷时段窗口公共模块(flow-cost-ledger §1.8,防 DRY,D1)。

唯一权威实现:flowq 与 flow-task-core.py 均 import 本模块(flowq 宿主侧
`sys.path.insert(0, "<collab-flow>/scripts")` + `from window import ...`)。
纯函数、stdlib、零 I/O:判定显式转 Asia/Shanghai,不依赖系统时区。

规则 v3(2026-08-23,官方 api-docs.deepseek.com 确认):
  高峰: 北京 周一至周五 >=09:00 <12:00 或 >=14:00 <18:00
  空闲: 其余(工作日午间/夜间 + 周末全天),半价
入参一律 UTC(datetime 带 tz);naive datetime 按 UTC 处理(调用方负责带偏移)。

selftest() 为权威用例(迁移自 flowq --selftest):13 时段边界 + 10 建议窗口。
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


def _cn_dt(dt_utc):
    """UTC → 北京 datetime(带 tz)。"""
    return dt_utc.astimezone(TZ_CN)


def window_kind(dt_utc):
    """peak / offpeak(规则 v3:高峰仅限工作日,周末全天 offpeak;
    边界:工作日 09:00 整起高峰,12:00/18:00 整起空闲)。"""
    t = _cn_dt(dt_utc)
    if t.weekday() >= 5:  # 周六=5 周日=6
        return "offpeak"
    m = t.hour * 60 + t.minute
    return "peak" if any(lo <= m < hi for lo, hi in _PEAK_SPANS) else "offpeak"


_WEEKDAY_CN = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")


def next_peak_start(dt_utc):
    """下一高峰起点(UTC);当前已在高峰返回 now。
    跨天/跨周末实算:工作日 18:00 后与周末全天 → 下一工作日 09:00。"""
    t = _cn_dt(dt_utc)
    if window_kind(dt_utc) == "peak":
        return dt_utc
    m = t.hour * 60 + t.minute
    # 今天剩余可成高峰的起点(仅工作日):09:00 / 14:00
    for cand in (9 * 60, 14 * 60):
        if t.weekday() < 5 and m < cand:
            start = t.replace(hour=cand // 60, minute=cand % 60, second=0, microsecond=0)
            return start.astimezone(timezone.utc)
    # 否则从下一天起找最近工作日 09:00(周五 18:00 后 / 周末 → 周一 09:00)
    day = t + timedelta(days=1)
    while day.weekday() >= 5:
        day += timedelta(days=1)
    start = day.replace(hour=9, minute=0, second=0, microsecond=0)
    return start.astimezone(timezone.utc)


def window_state_line(dt_utc):
    """状态栏:⛰️ 高峰 至 12:00(剩 2h30m)/ 🌙 空闲 至 14:00(剩 1h30m)/
    🌙 空闲 至 周一 09:00(剩 1d23h)。"""
    t = _cn_dt(dt_utc)
    if window_kind(dt_utc) == "peak":
        m = t.hour * 60 + t.minute
        end, end_s = (12 * 60, "12:00") if m < 12 * 60 else (18 * 60, "18:00")
        remain = end - m
        hh, mm = divmod(remain, 60)
        return f"⛰️ 高峰 至 {end_s}(剩 {hh}h{mm:02d}m)"
    nxt = _cn_dt(next_peak_start(dt_utc))
    days = (nxt.date() - t.date()).days
    # 跨周末(days==1 但今天是周六/日)直接显示星期几,避免「明日」需心算;平日「明日」即可
    if days == 1 and t.weekday() >= 5:
        label = _WEEKDAY_CN[nxt.weekday()]
    else:
        label = "今日" if days == 0 else ("明日" if days == 1 else _WEEKDAY_CN[nxt.weekday()])
    end_s = f"{label} {nxt.hour:02d}:{nxt.minute:02d}"
    remain_sec = (nxt - dt_utc).total_seconds()
    hh, mm = divmod(int(remain_sec) // 60, 60)
    if hh >= 24:  # 周末长窗口,显示天数
        dd, hh = divmod(hh, 24)
        return f"🌙 空闲 至 {end_s}(剩 {dd}d{hh}h{mm:02d}m)"
    return f"🌙 空闲 至 {end_s}(剩 {hh}h{mm:02d}m)"


def offpeak_remaining_sec(dt_utc):
    """当前空闲窗口剩余秒数；高峰返回 0；跨天/跨周末按下一高峰起点实算。"""
    if window_kind(dt_utc) != "offpeak":
        return 0.0
    return (next_peak_start(dt_utc) - dt_utc).total_seconds()


def next_offpeak_start(dt_utc):
    """最近未来空闲窗口起点(UTC)。当前已在空闲窗口则返回 now。
    高峰只可能出现在工作日(规则 v3),故 12:00/18:00 分支不受周末影响。"""
    if window_kind(dt_utc) == "offpeak":
        return dt_utc
    t = _cn_dt(dt_utc)
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
    """权威用例自检(迁移自 flowq --selftest):时段边界 + 建议窗口。全过返回 0。
    规则 v3 日期:2026-08-21 周五 / 08-22 周六 / 08-23 周日 / 08-24 周一。"""
    def utc(iso):
        return datetime.fromisoformat(iso)

    kind_cases = [
        # 工作日(周一 08-24):时段边界不变
        ("2026-08-24T00:00:00+00:00", "offpeak"),  # 08:00 北京
        ("2026-08-24T01:00:00+00:00", "peak"),     # 09:00 整 = 高峰起
        ("2026-08-24T03:59:59+00:00", "peak"),     # 11:59:59
        ("2026-08-24T04:00:00+00:00", "offpeak"),  # 12:00 整 = 空闲起
        ("2026-08-24T05:59:59+00:00", "offpeak"),  # 13:59:59
        ("2026-08-24T06:00:00+00:00", "peak"),     # 14:00 整 = 高峰起
        ("2026-08-24T09:59:59+00:00", "peak"),     # 17:59:59
        ("2026-08-24T10:00:00+00:00", "offpeak"),  # 18:00 整 = 空闲起
        ("2026-08-24T15:59:59+00:00", "offpeak"),  # 23:59:59
        # 周末全天 offpeak(旧规则这些点都是 peak)
        ("2026-08-22T02:00:00+00:00", "offpeak"),  # 周六 10:00
        ("2026-08-22T07:00:00+00:00", "offpeak"),  # 周六 15:00
        ("2026-08-23T01:00:00+00:00", "offpeak"),  # 周日 09:00
        ("2026-08-23T10:00:00+00:00", "offpeak"),  # 周日 18:00
    ]
    for iso, expect in kind_cases:
        got = window_kind(utc(iso))
        assert got == expect, f"window_kind({iso}) = {got}, 期望 {expect}"

    def tsk(pri, exp):
        return {"priority": pri, "expected_seconds": exp}

    sug_cases = [
        # 工作日(周一 08-24):决策表不变
        (tsk("P0", 3600), "2026-08-24T02:00:00+00:00", "立即"),          # 10:00 北京
        (tsk("P1", 600), "2026-08-24T02:00:00+00:00", "12:00 午间"),     # 10:00 北京
        (tsk("P2", 3600), "2026-08-24T02:00:00+00:00", "18:00 晚间"),    # 10:00 北京
        (tsk("P1", 600), "2026-08-24T07:00:00+00:00", "18:00 晚间"),     # 15:00 北京
        (tsk("P1", 600), "2026-08-24T04:30:00+00:00", "现在可跑"),       # 12:30 北京午间
        (tsk("P2", 2400), "2026-08-24T05:50:00+00:00", "顺延18:00"),     # 13:50 剩10min < 48min
        (tsk("P2", 2400), "2026-08-24T14:00:00+00:00", "现在可跑"),      # 22:00 北京夜间
        # 周末/周五晚:全天可跑(旧规则这些点是高峰/晚间锁定)
        (tsk("P2", 3600), "2026-08-22T02:00:00+00:00", "现在可跑"),       # 周六 10:00
        (tsk("P1", 600), "2026-08-23T07:00:00+00:00", "现在可跑"),        # 周日 15:00
        (tsk("P2", 3600), "2026-08-21T10:30:00+00:00", "现在可跑"),       # 周五 18:30(连周末)
    ]
    for t, iso, expect in sug_cases:
        got = window_suggest(t, utc(iso))
        assert got == expect, f"window_suggest({t}, {iso}) = {got}, 期望 {expect}"

    print(f"selftest OK({len(kind_cases)} 时段用例 + {len(sug_cases)} 建议用例)")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(selftest())
