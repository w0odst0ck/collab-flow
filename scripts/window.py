#!/usr/bin/env python3
"""window.py —— 峰谷时段窗口公共模块(flow-cost-ledger §1.8,防 DRY,D1)。

唯一权威实现:flowq 与 flow-task-core.py 均 import 本模块(flowq 宿主侧
`sys.path.insert(0, "<collab-flow>/scripts")` + `from window import ...`)。
判定逻辑纯函数:显式转计费时区,不依赖系统时区;入参一律 UTC(datetime 带 tz)。

计费规则唯一真相源 = config/pricing.yaml(官方 api-docs.deepseek.com 同步):
  高峰: 北京 周一至周五 spans 内(默认 09-12 / 14-18)
  空闲: 其余(工作日午间/夜间 + 周末全天),半价
改计费模式 = 只改 config/pricing.yaml,本文件与测试自动跟随(selftest 从
config 生成用例)。config 缺失 → 内置默认规则 + warning(裸环境不崩);
config 存在但解析/校验失败 → import 期抛 RuntimeError(fail-fast,防静默
跑错计费;宿主需能处理 import 异常——flowq 有 ImportError 降级路径)。

selftest() 为权威用例(迁移自 flowq --selftest):时段边界 + 建议窗口,全过返回 0。
"""

import os
import sys
import warnings
from datetime import datetime, timedelta, timezone

try:
    import yaml
except ImportError:  # 环境无 pyyaml → 用内置默认规则(见 _load_pricing)
    yaml = None

try:
    from zoneinfo import ZoneInfo
    _HAS_ZONEINFO = True
except Exception:  # Python <3.9 或无 tzdata 兜底(本机 3.11+,理论不走)
    _HAS_ZONEINFO = False

_DIR = os.path.dirname(os.path.abspath(__file__))
_CFG_PATH = os.environ.get("PRICING_CONFIG") or os.path.join(_DIR, "..", "config", "pricing.yaml")

# 内置默认规则(v3,2026-08-23 官方确认)——仅在 config 缺失/解析失败时兜底
_DEFAULT_PEAK = {
    "weekday_only": True,
    "tz": "Asia/Shanghai",
    "spans": ((9, 12), (14, 18)),
}


def _load_pricing():
    """读 config/pricing.yaml → 规范化规则。
    缺失(未部署 config)→ 内置默认 + warning(fail-open 防裸环境崩);
    存在但解析/校验失败 → 抛错(fail-fast:配置写错必须立即暴露,防静默跑错计费)。"""
    if yaml is None:
        warnings.warn("window: 无 pyyaml, 使用内置默认计费规则(未读 pricing.yaml)")
        return dict(_DEFAULT_PEAK)
    try:
        with open(_CFG_PATH, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    except FileNotFoundError:
        warnings.warn(f"window: 计费配置缺失({_CFG_PATH}), 使用内置默认规则")
        return dict(_DEFAULT_PEAK)
    except Exception as e:
        raise RuntimeError(f"window: 计费配置读取失败({_CFG_PATH}): {e}") from e
    try:
        peak = cfg["peak"]
        spans = tuple((int(a), int(b)) for a, b in peak["spans"])
        if not spans or any(not (0 <= a < b <= 23) for a, b in spans):
            raise ValueError(f"spans 非法(需 0<=a<b<=23, 不允许 24 整点): {peak['spans']}")
        return {
            "weekday_only": bool(peak.get("weekday_only", True)),
            "tz": str(peak.get("tz", "Asia/Shanghai")),
            "spans": spans,
        }
    except Exception as e:
        raise RuntimeError(f"window: 计费配置校验失败({_CFG_PATH}): {e}") from e


_PRICING = _load_pricing()
_SPANS = _PRICING["spans"]                        # ((9,12),(14,18)) 小时 [lo,hi)
_WEEKDAY_ONLY = _PRICING["weekday_only"]
_TZ_NAME = _PRICING["tz"]

if _HAS_ZONEINFO:
    try:
        TZ_CN = ZoneInfo(_TZ_NAME)
    except Exception:
        TZ_CN = timezone(timedelta(hours=8), name=_TZ_NAME)
else:
    TZ_CN = timezone(timedelta(hours=8), name=_TZ_NAME)

_PEAK_SPANS = tuple((lo * 60, hi * 60) for lo, hi in _SPANS)  # 分钟 [lo, hi)
_WEEKDAY_CN = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")


def _cn_minutes(dt_utc):
    """UTC → 计费时区当天分钟数(0-1439)。"""
    t = dt_utc.astimezone(TZ_CN)
    return t.hour * 60 + t.minute


def _cn_dt(dt_utc):
    """UTC → 计费时区 datetime(带 tz)。"""
    return dt_utc.astimezone(TZ_CN)


def _fmt_min(m):
    """分钟 → 'HH:MM'(如 720 → '12:00')。"""
    return f"{m // 60:02d}:{m % 60:02d}"


def _in_span(m):
    """当前分钟所在高峰 span 索引;不在任何 span 返回 None。"""
    for i, (lo, hi) in enumerate(_PEAK_SPANS):
        if lo <= m < hi:
            return i
    return None


def window_kind(dt_utc):
    """peak / offpeak(高峰仅限工作日 + spans 内;hi 整点起空闲)。"""
    t = _cn_dt(dt_utc)
    if _WEEKDAY_ONLY and t.weekday() >= 5:  # 周六=5 周日=6
        return "offpeak"
    return "peak" if _in_span(t.hour * 60 + t.minute) is not None else "offpeak"


def next_peak_start(dt_utc):
    """下一高峰起点(UTC);当前已在高峰返回 now。
    跨天/跨周末实算:工作日最后 span 结束后与周末全天 → 下一工作日首 span 起点。"""
    t = _cn_dt(dt_utc)
    if window_kind(dt_utc) == "peak":
        return dt_utc
    m = t.hour * 60 + t.minute
    # 今天剩余可成高峰的起点(仅工作日):各 span 的 lo 且 > m
    if t.weekday() < 5 or not _WEEKDAY_ONLY:
        for lo, _hi in _PEAK_SPANS:
            if m < lo:
                start = t.replace(hour=lo // 60, minute=lo % 60, second=0, microsecond=0)
                return start.astimezone(timezone.utc)
    # 否则从下一天起找最近「有高峰的日」首 span 起点(周五晚/周末 → 周一 09:00)
    day = t + timedelta(days=1)
    while _WEEKDAY_ONLY and day.weekday() >= 5:
        day += timedelta(days=1)
    lo0 = _PEAK_SPANS[0][0]
    start = day.replace(hour=lo0 // 60, minute=lo0 % 60, second=0, microsecond=0)
    return start.astimezone(timezone.utc)


def window_state_line(dt_utc):
    """状态栏:⛰️ 高峰 至 12:00(剩 2h30m)/ 🌙 空闲 至 14:00(剩 1h30m)/
    🌙 空闲 至 周一 09:00(剩 1d23h)。"""
    t = _cn_dt(dt_utc)
    if window_kind(dt_utc) == "peak":  # 含工作日限定;周末/空闲走下方分支
        idx = _in_span(t.hour * 60 + t.minute)
        hi = _PEAK_SPANS[idx][1]
        remain = hi - (t.hour * 60 + t.minute)
        hh, mm = divmod(remain, 60)
        return f"⛰️ 高峰 至 {_fmt_min(hi)}(剩 {hh}h{mm:02d}m)"
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
    """当前空闲窗口剩余秒数;高峰返回 0;跨天/跨周末按下一高峰起点实算。"""
    if window_kind(dt_utc) != "offpeak":
        return 0.0
    return (next_peak_start(dt_utc) - dt_utc).total_seconds()


def next_offpeak_start(dt_utc):
    """最近未来空闲窗口起点(UTC)。当前已在空闲窗口则返回 now。"""
    if window_kind(dt_utc) == "offpeak":
        return dt_utc
    t = _cn_dt(dt_utc)
    idx = _in_span(t.hour * 60 + t.minute)
    hi = _PEAK_SPANS[idx][1] if idx is not None else _PEAK_SPANS[0][1]
    start = t.replace(hour=hi // 60, minute=hi % 60, second=0, microsecond=0)
    return start.astimezone(timezone.utc)


def can_finish_in_window(dt_utc, expected_sec):
    """边界纪律:空闲窗口剩余 ≥ 预估×1.2 才算跑得完。"""
    return offpeak_remaining_sec(dt_utc) >= float(expected_sec) * 1.2


def _mid_label():
    """午间标签:首 span 的 hi 时间(如 '12:00 午间')。"""
    return f"{_fmt_min(_PEAK_SPANS[0][1])} 午间"


def _eve_label():
    """晚间标签:末 span 的 hi 时间(如 '18:00 晚间')。"""
    return f"{_fmt_min(_PEAK_SPANS[-1][1])} 晚间"


def _next_runnable_window(dt_utc, need_sec):
    """从 dt 起找第一个可容纳 need_sec 的空闲窗口起点(UTC)。
    用于 offpeak 中剩余不够时的顺延目标:当前窗口结束(下一高峰起点)后,
    逐窗口检查剩余是否 ≥ need;不足则跳到该窗口结束继续。
    找不到可容纳窗口(极端大任务)时返回最早可跑窗口起点(不误导为很晚)。"""
    t = dt_utc
    first = None
    for _ in range(16):  # 最多看 16 个窗口(防极端 config 死循环)
        peak_start = next_peak_start(t)                    # 当前空闲窗口结束
        off_start = next_offpeak_start(peak_start)         # 下一空闲窗口起点
        if first is None:
            first = off_start
        if offpeak_remaining_sec(off_start) >= need_sec:
            return off_start
        t = off_start + timedelta(seconds=offpeak_remaining_sec(off_start))
    return first  # 无窗口能容纳 → 最早可跑窗口


def window_suggest(task, dt_utc):
    """queued/scheduled 任务建议执行窗口(决策表:P0 立即 / P1 短→最近空闲 / 其余→晚间)。"""
    pri = (task.get("priority") or "P2").upper()
    if pri == "P0":
        return "立即"
    try:
        exp = float(task.get("expected_seconds") or 900)
    except (TypeError, ValueError):
        exp = 900.0
    if window_kind(dt_utc) == "offpeak":
        if can_finish_in_window(dt_utc, exp):
            return "现在可跑"
        # 当前窗口不够:顺延到最近可容纳的空闲窗口起点(非固定末 span hi)
        nxt = _cn_dt(_next_runnable_window(dt_utc, exp * 1.2))
        days = (nxt.date() - _cn_dt(dt_utc).date()).days
        if days >= 2:
            day_lbl = _WEEKDAY_CN[nxt.weekday()]
        elif days == 1:
            day_lbl = "明日"
        else:
            day_lbl = "今日"
        return f"顺延{day_lbl} {nxt.hour:02d}:{nxt.minute:02d}"
    # peak 中:P1 短任务取最近空闲(午间/晚间),其余锁晚间窗口
    m = _cn_minutes(dt_utc)
    if pri == "P1" and exp <= 15 * 60:
        return _mid_label() if m < _PEAK_SPANS[0][1] else _eve_label()
    return _eve_label()


# ── selftest:用例从 config 自动生成(改计费配置后测试自动跟随,防过期) ──────


def _anchor_day(dow):
    """找最近一个星期 dow(0=周一)的日期(UTC 锚点,北京时间当天)。"""
    today = datetime.now(TZ_CN).date()
    delta = (dow - today.weekday()) % 7
    if delta == 0:
        delta = 7  # 永远指向未来,避免边界歧义
    return datetime(today.year, today.month, today.day, 0, 0, tzinfo=TZ_CN) + timedelta(days=delta)


def _gen_kind_cases():
    """从 _SPANS/_WEEKDAY_ONLY 自动生成时段边界用例(工作日+周末,UTC 表示)。"""
    wk = _anchor_day(0)  # 未来周一
    we = _anchor_day(5)  # 未来周六
    cases = []

    def add(day, h, mi, expect):
        dt = day.replace(hour=h, minute=mi, second=0, microsecond=0).astimezone(timezone.utc)
        cases.append((dt.isoformat(), expect))

    # 工作日:00:00/23:59 必空闲 + 每个 span 边界(lo-1 空闲 / lo 高峰 / hi-1 高峰 / hi 空闲)
    add(wk, 0, 0, "offpeak")
    for lo, hi in _SPANS:
        add(wk, lo, 0, "peak")            # span 起点 = 高峰起
        add(wk, hi - 1, 59, "peak")       # hi 前最后一分钟
        add(wk, hi, 0, "offpeak")         # hi 整点 = 空闲起
        if lo > 0:
            add(wk, lo - 1, 59, "offpeak")  # span 前最后一分钟
    # span 间缝隙(若存在)必空闲:首 span hi 与次 span lo 之间取中点
    for i in range(len(_SPANS) - 1):
        gap_mid = (_SPANS[i][1] * 60 + _SPANS[i + 1][0] * 60) // 2
        add(wk, gap_mid // 60, gap_mid % 60, "offpeak")
    add(wk, 23, 59, "offpeak")

    # 周末:weekday_only 时全天空闲;否则按 spans 判定
    if _WEEKDAY_ONLY:
        for h, mi in ((0, 0), (10, 0), (_SPANS[0][0], 0), (_SPANS[0][1] - 1, 59), (23, 59)):
            add(we, h, mi, "offpeak")
    else:
        add(we, 0, 0, "offpeak")
        for lo, hi in _SPANS:
            add(we, lo, 0, "peak")
            add(we, hi, 0, "offpeak")
        add(we, 23, 59, "offpeak")
    return cases


def _gen_sug_cases():
    """建议窗口用例(决策表语义固定,时间点从 config 推导)。"""
    wk = _anchor_day(0)
    we = _anchor_day(5)
    fr = _anchor_day(4)
    lo0, hi0 = _SPANS[0]
    lo1, hi1 = _SPANS[-1]
    mid = _mid_label()
    eve = _eve_label()

    def tsk(pri, exp):
        return {"priority": pri, "expected_seconds": exp}

    def at(day, h, mi):
        return day.replace(hour=h, minute=mi, second=0, microsecond=0).astimezone(timezone.utc).isoformat()

    cases = [
        (tsk("P0", 3600), at(wk, lo0 + 1, 0), "立即"),                          # 工作日 peak 内
        (tsk("P1", 600), at(wk, lo0 + 1, 0), mid),                              # 上午 peak → 午间
        (tsk("P2", 3600), at(wk, lo0 + 1, 0), eve),                             # 上午 peak → 晚间
        (tsk("P1", 600), at(wk, lo1 + 1, 0), eve),                              # 下午 peak → 晚间
        (tsk("P1", 600), at(wk, hi0 + 0, 30), "现在可跑"),                      # 午间窗口内
        (tsk("P2", 2400), at(wk, lo1 - 1, 50), f"顺延今日 {_fmt_min(hi1 * 60)}"),  # 午间尾 13:50:剩10min < 48min
        (tsk("P1", 600), at(wk, lo0 - 1, 50), f"顺延今日 {_fmt_min(hi0 * 60)}"),  # 早间 08:50:窗口不够 → 最近可跑=午间
        (tsk("P2", 2400), at(wk, 22, 0), "现在可跑"),                           # 工作日夜间
    ]
    if _WEEKDAY_ONLY:
        # 周末/周五晚:全天谷价可跑(旧规则这些点是高峰/晚间锁定)
        cases += [
            (tsk("P2", 3600), at(we, lo0 + 1, 0), "现在可跑"),                  # 周六
            (tsk("P1", 600), at(we, lo1 + 1, 0), "现在可跑"),                   # 周日
            (tsk("P2", 3600), at(fr, hi1 + 0, 30), "现在可跑"),                 # 周五晚间(连周末)
        ]
    return cases


def selftest():
    """权威用例自检(用例从 config 自动生成)。全过返回 0。"""
    kind_cases = _gen_kind_cases()
    for iso, expect in kind_cases:
        got = window_kind(datetime.fromisoformat(iso))
        assert got == expect, f"window_kind({iso}) = {got}, 期望 {expect}"

    sug_cases = _gen_sug_cases()
    for t, iso, expect in sug_cases:
        got = window_suggest(t, datetime.fromisoformat(iso))
        assert got == expect, f"window_suggest({t}, {iso}) = {got}, 期望 {expect}"

    print(f"selftest OK({len(kind_cases)} 时段用例 + {len(sug_cases)} 建议用例)"
          f" | 规则: weekday_only={_WEEKDAY_ONLY} spans={_SPANS} tz={_TZ_NAME}")
    return 0


if __name__ == "__main__":
    sys.exit(selftest())
