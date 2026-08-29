#!/usr/bin/env python3
"""cost-report.py —— 成本周报自动化(cost-weekly-report 设计 §二)。

数据源:只读 FLOW_TASK_DIR/tasks.json(默认 ~/.collabflow),零写盘、零锁
(registry 原子写,读取天然一致)。零 LLM、零网络(网络仅 feishu-notify.sh 内部)。

统计口径(唯一权威 = design.md §1):
- 周窗口:week_timezone 下「dow 00:00」为周界,自然周为半开区间 [周一00:00, 次周一00:00);
  --weeks N = 当前周起点之前的 N 个已完整结束的自然周,默认 1 = 上周(cron 周一推上周)。
- 周归属:ts = finished_at || started_at || created_at(首个非空可解析);
  三个全缺/非法 → 不计入任何周,计入 untimed_tasks。
- 峰谷节省:import scripts/window.py 为唯一权威(window_kind 判窗口、get_prices 推导
  价比 R = flash.input_miss.peak / offpeak,缺失/分母 0 → 兜底 2.0),禁止内联复制时段。
  节省仅按 started_at 判定:offpeak → cost_usd × (R−1),peak/无 started_at → 0。
- 失败分层:空/缺失 → 「无数据」exit 0;JSON 损坏/配置非法 → fail-fast exit 2;
  单条坏字段 → 逐条容错(兜底桶),不崩。

退出码:0 成功(含无数据);2 参数/配置/数据损坏;3 --push 推送失败(以
feishu-notify.sh 输出含「推送 OK」为唯一成功信号,该脚本网络失败仍 exit 0)。
"""

import argparse
import json
import math
import os
import re
import subprocess
import sys
import warnings
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - Python <3.9 或无 tzdata 兜底
    ZoneInfo = None

try:
    import yaml
except ImportError:  # 环境无 pyyaml → 读不了 defaults.yaml/user config,用内置默认
    yaml = None

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

try:
    import window  # 峰谷判定唯一权威(window.py;E10 import 失败 → 节省降级 0)
except Exception as _e:  # pragma: no cover - 正常环境必有 window.py
    window = None
    _WINDOW_IMPORT_ERR = str(_e)

_SCHEMA_VERSION = 1          # tasks.json 的 schema_version(与 flow-task-core SCHEMA_VERSION 一致)
_PROJECT_RE = re.compile(r"/workspace/projects/([^/]+)")
_PUSH_OK_MARK = "推送 OK"     # feishu-notify.sh 唯一成功信号(网络失败仍 exit 0,不能看退出码)
_PUSH_MAX_CHARS = 4000        # E16:飞书文本消息截断上限

# 内置默认(仅当 defaults.yaml/user config/env 均未提供时;与 design §2.5 一致)
_DEFAULT_BUDGET = {
    "exchange_rate": 7.2,
    "week_start_dow": 0,      # 0=周一(Python weekday 口径)
    "week_timezone": "Asia/Shanghai",
}

_DEFAULTS_YAML = os.path.join(_SCRIPT_DIR, "..", "config", "defaults.yaml")

_WINDOW_FALLBACK_WARNED = False  # E10:window import 失败只警告一次,不刷屏


def _warn_window_fallback():
    """E10:window 模块不可用 → 峰谷节省降级 0,警告一次(stderr 可见,不崩)。"""
    global _WINDOW_FALLBACK_WARNED
    if not _WINDOW_FALLBACK_WARNED:
        _WINDOW_FALLBACK_WARNED = True
        warnings.warn("cost-report: window 模块不可用, 峰谷节省降级为 0 (E10)",
                      stacklevel=2)


class CostReportError(Exception):
    """数据损坏 / 配置非法 → fail-fast(exit 2);消息直接打 stderr。"""


# ── 数据加载(E1/E2/E3) ────────────────────────────────────────────────


def load_registry(data_dir):
    """读 tasks.json → dict。缺失/目录不存在 → 空数据集(不抛,E1);
    JSON 损坏(E2)/schema_version 非法/顶层非 dict/tasks 非 dict(E3) → CostReportError。"""
    path = os.path.join(data_dir, "tasks.json")
    if not os.path.isfile(path):
        return {"schema_version": _SCHEMA_VERSION, "tasks": {}}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError) as e:
        raise CostReportError(f"{path}: tasks.json 损坏(非 JSON): {e}") from e
    if not isinstance(data, dict):
        raise CostReportError(f"{path}: 注册表顶层必须是对象")
    if data.get("schema_version") != _SCHEMA_VERSION:
        raise CostReportError(
            f"{path}: schema_version 非法: {data.get('schema_version')!r}(期望 {_SCHEMA_VERSION})")
    tasks = data.get("tasks")
    if not isinstance(tasks, dict):
        raise CostReportError(f"{path}: tasks 必须是对象")
    return data


def iter_tasks(registry):
    """遍历 registry["tasks"].values();单条非 dict → 跳过(逐条容错 E4)。"""
    tasks = registry.get("tasks")
    if not isinstance(tasks, dict):
        return
    for entry in tasks.values():
        if isinstance(entry, dict):
            yield entry


# ── 时间解析与周窗口(§1.2) ────────────────────────────────────────────


def parse_ts(s):
    """ISO 解析 → aware datetime;非法/缺失 → None。
    naive 输入视为 UTC(registry 内时间戳惯例 UTC,坏数据不炸)。"""
    if not s or not isinstance(s, str):
        return None
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _tz(tz_name):
    """ZoneInfo(tz_name);ZoneInfo 不可用(无 tzdata)→ 固定 UTC+8 兜底(同 window.py)。"""
    if ZoneInfo is not None:
        return ZoneInfo(tz_name)
    return timezone(timedelta(hours=8), name=tz_name)


def week_bounds(now_utc, tz_name=_DEFAULT_BUDGET["week_timezone"],
                dow=_DEFAULT_BUDGET["week_start_dow"], weeks=1):
    """过去 weeks 个已完整结束的自然周 → [(start, end), ...] 半开区间,时间升序(旧→新)。
    周界 = 配置时区下「dow 00:00」;当前周(now 所在)永不纳入。时区不存在 → ZoneInfoNotFoundError
    (由调用方 E8 回退默认 + warning)。"""
    tz = _tz(tz_name)
    now_local = now_utc.astimezone(tz)
    cur_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    cur_start -= timedelta(days=(cur_start.weekday() - dow) % 7)
    bounds = []
    end = cur_start
    for _ in range(max(1, int(weeks))):
        # 按日历 7 天回退(固定偏移假设);DST 时区会使 00:00 周界漂移,默认 Asia/Shanghai 无 DST。
        start = end - timedelta(days=7)
        bounds.append((start, end))
        end = start
    bounds.reverse()  # 升序:最旧在前
    return bounds


def bucket_task(task, bounds):
    """周归属:ts = finished_at || started_at || created_at(首个非空可解析,§1.2)。
    返回 (ts, week_idx):ts 为 None → 无时间戳(计入 untimed_tasks);week_idx None →
    ts 有效但不在所选周(忽略,非 untimed)。全无时间戳 → None。"""
    ts = None
    for key in ("finished_at", "started_at", "created_at"):
        if task.get(key):
            ts = parse_ts(task.get(key))
            if ts is not None:
                break
    if ts is None:
        return None
    for i, (start, end) in enumerate(bounds):
        if start <= ts < end:
            return (ts, i)
    return (ts, None)


# ── 分桶(§1.3) ────────────────────────────────────────────────────────


def state_bucket(task):
    """done/failed/timeout 精确匹配;其余(含 None)→ other。"""
    s = task.get("state")
    if s in ("done", "failed", "timeout"):
        return s
    return "other"


def executor_bucket(task):
    """reasonix/script/local 精确匹配;None/其他 → 其他。"""
    e = task.get("executor")
    if e in ("reasonix", "script", "local"):
        return e
    return "其他"


def model_bucket(task):
    """deepseek-v4-flash→flash;deepseek-v4-pro→pro;非 deepseek 且非空 或 executor==local→本地;
    None/空/无法归类 → 未知。"""
    model = task.get("model")
    if model == "deepseek-v4-flash":
        return "flash"
    if model == "deepseek-v4-pro":
        return "pro"
    if model or task.get("executor") == "local":
        return "本地"
    return "未知"


def project_of(workdir):
    """workdir 正则 /workspace/projects/([^/]+) 捕获第 1 段;不匹配/None → 未分类。"""
    if not isinstance(workdir, str):
        return "未分类"
    m = _PROJECT_RE.search(workdir)
    return m.group(1) if m else "未分类"


# ── 成本与峰谷节省(§1.3/§1.4) ─────────────────────────────────────────


def _norm_cost(v):
    """成本归一(E11/E12):None/非数值 → None(unbilled);负数 → 0.0(fail-closed);正数 → float。"""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v) if v > 0 else 0.0
    return None  # 非数值(字符串等)→ 忽略成本,计入 unbilled(E12)


def peak_ratio():
    """价比 R = prices.flash.input_miss.peak / offpeak(唯一权威 = window.get_prices());
    结构缺失/分母 0 → 兜底 2.0(E9,与 pricing.yaml 全段 peak=2×offpeak 一致,非写死来源)。"""
    if window is None:
        _warn_window_fallback()  # E10
        return 2.0
    try:
        p = window.get_prices()
        flash = p["flash"]["input_miss"]
        peak, offpeak = float(flash["peak"]), float(flash["offpeak"])
        # peak<offpeak 属错价配置,拒绝负节省;inf/nan 非有限值 → 兜底 2.0(不污染 JSON 输出)
        if (offpeak > 0 and peak >= offpeak
                and math.isfinite(peak) and math.isfinite(offpeak)):
            return peak / offpeak
    except (KeyError, TypeError, ValueError):
        pass
    return 2.0


def savings_usd(task, ratio):
    """峰谷节省(§1.4):按 started_at 判定窗口(跨峰谷任务按起始窗口计,§1.4.5);
    offpeak → cost_usd × (R−1);peak/无 started_at/cost 无效 → 0(保守,不算节省)。"""
    if window is None:
        _warn_window_fallback()  # E10:window import 失败 → 节省降级 0
        return 0.0
    cost = _norm_cost(task.get("cost_usd"))
    if cost is None or cost <= 0:
        return 0.0
    started = task.get("started_at")
    if not started:
        return 0.0
    dt = parse_ts(started)
    if dt is None:
        return 0.0
    try:
        kind = window.window_kind(dt)
    except Exception:
        return 0.0
    if kind == "offpeak":
        return cost * (ratio - 1.0)
    return 0.0


# ── 聚合(§1.3/§2.3) ───────────────────────────────────────────────────


def _crosses_window(task):
    """§1.4.5 跨峰谷检测:start 与 finish 分属不同窗口(节省按起始窗口计,单标量不拆分)。
    任一时间戳缺失/非法或 window 不可用 → False(保守不计)。"""
    if window is None:
        return False
    started, finished = task.get("started_at"), task.get("finished_at")
    if not started or not finished:
        return False
    s, f = parse_ts(started), parse_ts(finished)
    if s is None or f is None:
        return False
    try:
        return window.window_kind(s) != window.window_kind(f)
    except Exception:
        return False


def _empty_week(start, end):
    """全零周结构(empty 时同样返回,§2.3「empty=true 时仍返回全零结构」)。"""
    return {
        "week_start": start.date().isoformat(),
        "week_end": end.date().isoformat(),
        "tasks": {"total": 0, "done": 0, "failed": 0, "timeout": 0, "other": 0},
        "cost_usd": 0.0, "cost_cny": 0.0,
        "failed_cost_usd": 0.0, "failed_cost_cny": 0.0,
        "peak_savings_usd": 0.0, "peak_savings_cny": 0.0,
        "peak_savings_tasks": 0,
        "cross_window_tasks": 0,
        "unbilled_tasks": 0, "untimed_tasks": 0,
        "by_project": [], "by_executor": [], "by_model": [],
    }


def build_report(registry, cfg, weeks):
    """产出 §2.3 JSON 结构。weeks: week_bounds() 返回的 [(start,end),...]。
    纯函数无副作用;所有聚合按周分桶,unbilled/untimed 为周内计数(untimed 为全局数,每周同值)。"""
    rate = float(cfg["exchange_rate"])
    per_week = [_empty_week(s, e) for s, e in weeks]
    untimed_total = 0
    ratio = peak_ratio()

    for task in iter_tasks(registry):
        placed = bucket_task(task, weeks)
        if placed is None:
            untimed_total += 1
            continue
        ts, idx = placed
        if idx is None:
            continue
        w = per_week[idx]

        state = state_bucket(task)
        w["tasks"]["total"] += 1
        w["tasks"][state] += 1

        cost = _norm_cost(task.get("cost_usd"))
        if cost is None:
            w["unbilled_tasks"] += 1
        else:
            w["cost_usd"] += cost
            if state in ("failed", "timeout"):
                w["failed_cost_usd"] += cost
            sav = savings_usd(task, ratio)
            w["peak_savings_usd"] += sav
            if sav > 0:
                w["peak_savings_tasks"] += 1

        if cost is not None and _crosses_window(task):
            w["cross_window_tasks"] += 1   # §1.4.5:文本标注口径

        w["by_project"].append((project_of(task.get("workdir")), cost))
        w["by_executor"].append((executor_bucket(task), cost))
        w["by_model"].append((model_bucket(task), cost))

    total_tasks = sum(w["tasks"]["total"] for w in per_week)
    for w in per_week:
        w["cost_cny"] = round(w["cost_usd"] * rate, 2)
        w["failed_cost_cny"] = round(w["failed_cost_usd"] * rate, 2)
        w["peak_savings_cny"] = round(w["peak_savings_usd"] * rate, 2)
        w["untimed_tasks"] = untimed_total
        w["by_project"] = _dist_list(w["by_project"], "project", rate)
        w["by_executor"] = _dist_list(w["by_executor"], "executor", rate)
        w["by_model"] = _dist_list(w["by_model"], "model", rate)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "empty": total_tasks == 0,
        "exchange_rate": rate,
        "weeks": per_week,
    }


def _dist_list(rows, key, rate):
    """[(bucket, cost|None), ...] → [{key, tasks, cost_usd, cost_cny}] 按 cost 降序。
    cost None(unbilled)计任务数不计成本。"""
    agg = {}
    for name, cost in rows:
        d = agg.setdefault(name, {"tasks": 0, "cost_usd": 0.0})
        d["tasks"] += 1
        if cost is not None:
            d["cost_usd"] += cost
    out = [{key: k, "tasks": v["tasks"], "cost_usd": v["cost_usd"],
            "cost_cny": round(v["cost_usd"] * rate, 2)} for k, v in agg.items()]
    out.sort(key=lambda x: (x["cost_usd"], x["tasks"]), reverse=True)
    return out


# ── 文本渲染(§2.4) ────────────────────────────────────────────────────


def _fmt_cny(v):
    return f"¥{v:.2f}"


def _dist_line(label, rows, key):
    parts = [f"{r[key]} {r['tasks']} 任务 {_fmt_cny(r['cost_cny'])}" for r in rows]
    return f"{label}: " + (" / ".join(parts) if parts else "—")


def render_text(report):
    """文本周报;empty → 仅「无数据」一行(exit 0 由调用方保证)。"""
    if report.get("empty"):
        return "无数据"
    rate = report["exchange_rate"]
    lines = []
    for w in report["weeks"]:
        # 标题打印覆盖区间:week_end 为半开区间端(次周一),显示到前一天
        end_cover = (datetime.fromisoformat(w["week_end"]) - timedelta(days=1)).strftime("%Y-%m-%d")
        lines.append(f"成本周报 {w['week_start']} ~ {end_cover}")
        t = w["tasks"]
        lines.append(f"任务: 总数 {t['total']} (done {t['done']} / failed {t['failed']} "
                     f"/ timeout {t['timeout']} / 其他 {t['other']})")
        lines.append(f"总成本: {_fmt_cny(w['cost_cny'])} (US${w['cost_usd']:.2f} × {rate:g})"
                     f"   未落账: {w['unbilled_tasks']} 条")
        lines.append(f"失败成本: {_fmt_cny(w['failed_cost_cny'])} (US${w['failed_cost_usd']:.2f})"
                     f"        ← 防隐形浪费")
        lines.append(f"峰谷节省: {_fmt_cny(w['peak_savings_cny'])} "
                     f"(原价 vs 实付，空闲半价任务 {w['peak_savings_tasks']} 条)")
        if w.get("cross_window_tasks"):
            # §1.4.5:跨峰谷任务成本不可拆分,统一按起始窗口计(口径可复现)
            lines.append(f"跨峰谷按起始窗口计 ({w['cross_window_tasks']} 条)")
        lines.append(_dist_line("项目", w["by_project"], "project"))
        lines.append(_dist_line("执行器", w["by_executor"], "executor"))
        lines.append(_dist_line("模型", w["by_model"], "model"))
        if w["untimed_tasks"]:
            lines.append(f"未归周(无时间戳): {w['untimed_tasks']} 条")
        lines.append("")
    return "\n".join(lines).rstrip()


# ── 配置(§1.5) ────────────────────────────────────────────────────────


def _yaml_budget(path):
    """读 YAML 的 budget 段标量(仅 exchange_rate/week_start_dow/week_timezone)。
    文件缺失 → {};解析失败 → CostReportError(fail-fast,配置写错立即暴露)。"""
    if yaml is None or not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except Exception as e:
        raise CostReportError(f"{path}: 配置解析失败: {e}") from e
    if not isinstance(data, dict):
        return {}
    b = data.get("budget")
    if not isinstance(b, dict):
        return {}
    return {k: b[k] for k in ("exchange_rate", "week_start_dow", "week_timezone") if k in b}


def load_budget_config():
    """配置解析(仅 budget 段标量),优先级 env > 用户 config(COLLABFLOW_CONFIG 可覆盖,
    默认 ~/.config/collabflow/config.yaml) > defaults.yaml > 内置默认。
    exchange_rate 非正数/非数字(E7)、week_start_dow 越界(E8) → CostReportError(fail-fast)。"""
    cfg = dict(_DEFAULT_BUDGET)
    if yaml is None:
        warnings.warn("cost-report: 无 pyyaml, 使用内置默认 budget 配置(未读 defaults.yaml)")
    else:
        # defaults.yaml 缺失 → 内置默认 + warning(裸环境不崩,同 window.py 计费缺失口径)
        if not os.path.isfile(_DEFAULTS_YAML):
            warnings.warn(f"cost-report: 默认配置缺失({_DEFAULTS_YAML}), 使用内置默认 budget")
        for src in (_DEFAULTS_YAML,):
            cfg.update({k: v for k, v in _yaml_budget(src).items() if v is not None})
        user_path = os.environ.get("COLLABFLOW_CONFIG") or \
            os.path.expanduser("~/.config/collabflow/config.yaml")
        cfg.update({k: v for k, v in _yaml_budget(user_path).items() if v is not None})

    # env 覆盖(非已设置不覆盖,允许显式清空语义)
    if os.environ.get("COST_EXCHANGE_RATE"):
        cfg["exchange_rate"] = os.environ["COST_EXCHANGE_RATE"]
    if os.environ.get("COST_WEEK_START_DOW"):
        cfg["week_start_dow"] = os.environ["COST_WEEK_START_DOW"]
    if os.environ.get("COST_WEEK_TZ"):
        cfg["week_timezone"] = os.environ["COST_WEEK_TZ"]

    # 校验(fail-fast)
    try:
        rate = float(cfg["exchange_rate"])
    except (TypeError, ValueError):
        raise CostReportError(f"exchange_rate 非法: {cfg['exchange_rate']!r}(需正数)")
    if not rate > 0 or not math.isfinite(rate):
        raise CostReportError(f"exchange_rate 非法: {cfg['exchange_rate']!r}(需正数)")
    try:
        dow = int(cfg["week_start_dow"])
    except (TypeError, ValueError):
        raise CostReportError(f"week_start_dow 非法: {cfg['week_start_dow']!r}(需 0-6)")
    if not 0 <= dow <= 6:
        raise CostReportError(f"week_start_dow 非法: {cfg['week_start_dow']!r}(需 0-6)")

    return {
        "exchange_rate": rate,
        "week_start_dow": dow,
        "week_timezone": str(cfg["week_timezone"]),
        "data_dir": os.environ.get("FLOW_TASK_DIR") or os.path.expanduser("~/.collabflow"),
        "feishu_notify": os.environ.get("FEISHU_NOTIFY") or
                         os.path.expanduser("~/.collabflow/feishu-notify.sh"),
    }


# ── 推送(§2.1/E14/E15/E16/E19) ────────────────────────────────────────


def _truncate(text, max_chars=_PUSH_MAX_CHARS):
    """E16:超长截断 + 末尾「(已截断)」,不崩。"""
    if len(text) <= max_chars:
        return text
    return text[: max_chars - len("(已截断)")] + "(已截断)"


def push_text(text, notify_cmd):
    """调 feishu-notify.sh(stdin 喂文本),以输出含「推送 OK」为唯一成功信号
    (该脚本网络失败仍 exit 0,§1.1/§2.1.5)。脚本缺失/不可执行 → CostReportError(exit 2,E14);
    推送失败/超时 → 返回 (False, reason)(exit 3,E15/E19)。"""
    if not notify_cmd or not os.path.isfile(notify_cmd):
        raise CostReportError(f"feishu-notify.sh 缺失: {notify_cmd or '(未配置 FEISHU_NOTIFY)'}")
    if not os.access(notify_cmd, os.X_OK):
        raise CostReportError(f"feishu-notify.sh 不可执行: {notify_cmd}")
    try:
        proc = subprocess.run(
            [notify_cmd], input=_truncate(text), capture_output=True, text=True, timeout=90)
    except FileNotFoundError as e:  # E14:exec 时才暴露的不可执行(相对路径/解释器缺失)→ exit 2
        raise CostReportError(f"feishu-notify.sh 不可执行: {notify_cmd} ({e})") from e
    except subprocess.TimeoutExpired as e:  # E19:网络超时归 E15
        return False, f"推送超时: {e}"
    out = (proc.stdout or "") + (proc.stderr or "")
    if _PUSH_OK_MARK in out:
        return True, out
    return False, (out.strip() or f"推送失败(退出码 {proc.returncode}, 无「{_PUSH_OK_MARK}」信号)")


# ── CLI(§2.1) ─────────────────────────────────────────────────────────


def main(argv):
    """CLI 编排。返回 0(成功/无数据)/ 2(参数/配置/数据损坏)/ 3(推送失败)。"""
    parser = argparse.ArgumentParser(
        prog="cost-report",
        description="成本周报(只读 ~/.collabflow/tasks.json,零 LLM)。"
                    "峰谷节省唯一权威 = scripts/window.py + config/pricing.yaml。",
        epilog="退出码: 0 成功(含无数据) / 2 参数/配置/数据损坏 / 3 推送失败")
    parser.add_argument("--weeks", type=int, default=1,
                        help="过去 N 个已完整结束的自然周(默认 1 = 上周)")
    parser.add_argument("--push", action="store_true",
                        help="打印 + 调 feishu-notify.sh 推送飞书(成功信号 = 输出「推送 OK」)")
    parser.add_argument("--json", action="store_true",
                        help="stdout 输出机器可读 JSON(测试用;与 --push 可组合,推送仍为文本)")
    args = parser.parse_args(argv)

    if args.weeks is None or args.weeks < 1:  # E17
        parser.error("--weeks N 必须为 ≥1 的整数")

    try:
        cfg = load_budget_config()
    except CostReportError as e:
        print(f"cost-report: {e}", file=sys.stderr)
        return 2

    try:
        registry = load_registry(cfg["data_dir"])
    except CostReportError as e:
        print(f"cost-report: {e}", file=sys.stderr)
        return 2

    now = datetime.now(timezone.utc)
    tz_name = cfg["week_timezone"]
    try:
        bounds = week_bounds(now, tz_name, cfg["week_start_dow"], args.weeks)
    except Exception as e:  # E8:时区不存在 → 回退默认 + warning(不 fail-fast)
        warnings.warn(f"cost-report: 时区 {tz_name!r} 不可用({e}), 回退 Asia/Shanghai")
        bounds = week_bounds(now, _DEFAULT_BUDGET["week_timezone"], cfg["week_start_dow"], args.weeks)

    report = build_report(registry, cfg, bounds)

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(render_text(report))

    if args.push:
        text = render_text(report)  # 推送内容始终为文本(§2.1:--json 与 --push 互不污染,E18)
        try:
            ok, reason = push_text(text, cfg["feishu_notify"])
        except CostReportError as e:  # E14
            print(f"cost-report: {e}", file=sys.stderr)
            return 2
        if not ok:  # E15/E19
            print(f"cost-report: 推送失败: {reason}", file=sys.stderr)
            return 3
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
