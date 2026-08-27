#!/usr/bin/env python3
"""flow-task-core.py —— collab-flow 后台任务队列子系统核心(M1)。

职责(对应 .flow/workitems/flow-task-queue/design.md):
  1. 任务状态机纯函数(§1.3)—— TASK_STATES/terminal_state()/sort_queue()/plan_dispatch()/
     validate_entry()/extract_tail(),零隐藏 I/O 依赖,供单测直接 import;
  2. store 层(§1.4)—— tasks.json 注册表 flock 读-改-写 + 原子写(temp+fsync+replace)
     + DENY_RE 防 secret(沿用 flow-core.py 口径,import 复用而非复制);
  3. 落账 runner(§1.4(3))—— _runner <id> 分离式 wrapper:命令结束 finally 写终态
     (state/exit_code/finished_at/failure_tail),再 dispatch 自续队列,无需常驻 daemon;
  4. CLI(§1.5)—— flow task add/status/list/log/run/reconcile。

用法: flow-task-core.py task <sub> ...
退出码: 0 成功 / 1 运行失败(写盘/运行异常) / 2 用法或前置错误(含幂等拒绝) /
        124 仅任务命令语义(timeout 透传),不进入 task CLI 顶层。

红线: 零个人标识(不取 hostname/用户名);注册表/日志输出过 DENY_RE;
      幂等拒绝不设 --force 绕过;超时→timeout 终态判定不削弱;纯函数零 I/O。
"""

import glob
import importlib.util
import json
import math
import os
import re
import secrets
import shlex
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

try:
    import fcntl
except ImportError:  # 非 POSIX 降级(§1.6.6)
    fcntl = None

try:
    import select
except ImportError:  # 非 POSIX 降级:无 select → 退回阻塞读(接受无读超时)
    select = None

try:
    from zoneinfo import ZoneInfo
    TZ_CN = ZoneInfo("Asia/Shanghai")
except Exception:  # 无 tzdata 兜底(本机 3.11+,理论不走)
    TZ_CN = timezone(timedelta(hours=8), name="Asia/Shanghai")

# ---------------------------------------------------------------------------
# 复用 flow-core.py 的纯工具(只读依赖,不改动该文件;解析/输出口径完全一致)
# ---------------------------------------------------------------------------

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)
import window  # 公共窗口模块(flow-cost-ledger D1:唯一权威实现,零重复)

_spec = importlib.util.spec_from_file_location(
    "flow_core_tools", os.path.join(_SCRIPT_DIR, "flow-core.py"))
_fc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_fc)

DENY_RE = _fc.DENY_RE            # 疑似 secret 正则(与 flow-core 同口径,§0.3)
scan_args = _fc.scan_args        # 参数拆分 (positional, opts)
emit = _fc.emit                  # stdout 单行 JSON
emit_err = _fc.emit_err          # stderr 单行 JSON
fail = _fc.fail                  # 错误输出,返回退出码
now_iso = _fc.now_iso            # ISO8601 UTC(timespec="seconds")

# ---------------------------------------------------------------------------
# 常量(§1.2/§1.3)
# ---------------------------------------------------------------------------

TASK_ID_RE = re.compile(r"^t-[0-9a-f]{12}$")   # 任务 id 白名单,硬编码不进 config
TASK_STATES = ("scheduled", "queued", "running", "done", "failed", "timeout")
NON_TERMINAL = ("scheduled", "queued", "running")   # 非终态(幂等去重/容量 cap 按此计数)
TERMINAL_STATES = ("done", "failed", "timeout")
PRUNE_STATES = ("done", "failed", "timeout", "killed")  # prune --state 白名单(killed 预留,无匹配幂等)
KINDS = ("design", "execute")  # 任务类型白名单(2026-08-22 收窄:只留 LLM 设计/coding;verify/review/batch/reminder 拒)
PRIORITIES = ("P0", "P1", "P2")
PRIO_ORDER = {"P0": 0, "P1": 1, "P2": 2}
SCHEMA_VERSION = 1
RECONCILE_GRACE_S = 60   # pid=None(dispatch 提升后 runner 尚未写 pid 的窗口)的回收宽限秒数

# 命令模板白名单(flow-cost-ledger §1.4,编译期常量硬编码不进 config;--force 仅撬本步)
FLOW_WORKITEM_RE = re.compile(
    r"^(?:\./|/)?[^\s]*flow\s+workitem\s+(design|execute)"
    r"\s+([A-Za-z0-9][\w.-]*)(?:\s+--[\w-]+(?:[=\s]\S+)*)*$")
RX_RE = re.compile(r"^(?:\./|/)?[^\s]*rx\s+\S+\s+\S+.*$")
SCRIPT_RE = re.compile(r"^(?:bash|python|python3)\s+(?:\./|/)?[^\s]*/scripts/[^\s]+(?:\s+.*)?$")
_PROJ_PATH_RE = re.compile(r"(?:^|\s)FLOW_WORKDIR=[^\s]*/projects/[^/\s]+|/projects/[^/\s]+")
_HHMM_RE = re.compile(r"^\d{1,2}:\d{2}(?::\d{2})?$")

# ---------------------------------------------------------------------------
# W-B:任务终态钩子 + 超时收口(stale gate)常量(design task-terminal-hooks §1.4)
# ---------------------------------------------------------------------------

STALE_STATES = ("designed", "reviewed", "translated", "executed", "verified")
# created/accepted/retrospected 不参与 stale gate(§1.4.1)
STALE_REMIND_HOURS = 2    # remind_hours 键缺失 → 硬编码默认(§1.7)
STALE_WARN_HOURS = 24     # warn_hours 键缺失 → 硬编码默认
STALE_FORCE_HOURS = 48    # force_hours 键缺失 → 硬编码默认
STALE_DEDUP_COOLDOWN_S = 86400  # remind/escalate 去重 ≤1/天(§1.4.6)
STALE_TAIL_SCAN_BYTES = 65536  # audit.jsonl 尾部反查字节上限(有界扫描)
STALE_TERMINAL_AUDIT_STREAM = "stale"   # events/audit.jsonl stream 标识
TERMINAL_AUDIT_STREAM = "terminal"      # 终态钩子 audit stream 标识

# ---------------------------------------------------------------------------
# W-S2:execute timeout/failed 自动抢救 + token 预警常量(design executor-timeout-recovery §2.7)
# 硬编码兜底(与 STALE_* 同模式);env FLOW_TASK_RESCUE_MAX_RETRIES /
# FLOW_TASK_TOKEN_WARN_THRESHOLD 或 user config task.* 可覆盖,defaults.yaml 不新增键
# ---------------------------------------------------------------------------

RESCUE_MAX_RETRIES = 2            # 自动重跑限次(超限 rescue_frozen 冻结 + 升级人工)
TOKEN_WARN_THRESHOLD = 300000     # reasonix 日志 token 峰值告警阈值
RESCUE_AUDIT_STREAM = "rescue"    # events/audit.jsonl stream 标识
RESCUE_STATES = ("timeout", "failed")   # 抢救触发终态(execute)
# ocr medium F1:锁内预检测试(_run_tests)有界超时秒——assess_execute_completion 在
# with_workitem_lock 临界区内跑测试,无超时会阻塞该 workitem 全部转移;config
# rescue.test_timeout_s 可覆盖(缺失/非法回退本常量 300)
RESCUE_TEST_TIMEOUT_S = 300
TOKEN_PEAK_RE = re.compile(r"·\s*([0-9][0-9_,]*)\s*tok\s*·")  # usage 峰值行(容忍千分位逗号)
TOKEN_PEAK_SCAN_BYTES = 1 << 20  # ocr F4:reasonix run-log 峰值扫描字节上限(超限跳过,防 runaway 全量读吃内存)
# ocr7-M1:git diff HEAD 输出字节上限(workitem 锁内跑;超限截断 + 告警,防大 patch
# 全量进内存拖慢并发写;截断 → diff.patch 不完整,verify diff gate fail-closed 收口)
RESCUE_DIFF_MAX_BYTES = 1 << 20
# ocr F6:run_rescue_hook 返回这些动作时已发过 rescue 通知(单通知原则:终态失败
# 通知不再重复发,失败上下文由 rescue 通知 terminal_failure 字段承载)
RESCUE_NOTIFY_ACTIONS = frozenset(
    {"rescued", "verify_fail", "verify_error", "write_fail", "frozen"})

# ---------------------------------------------------------------------------
# W-V1:execute 基线快照常量(design verify-baseline-snapshot §2.4)
# ---------------------------------------------------------------------------

SNAPSHOT_SCHEMA_VERSION = 1       # tasks/<id>.snapshot.json schema 版本
SNAPSHOT_GIT_TIMEOUT_S = 5        # flock 内 git 只读短超时:失败即降级,不拖慢并发写

USAGE = """用法: flow task <sub> ...
  flow task add       --command CMD --kind design|execute
                      [--workitem W] --priority P0|P1|P2 --expected-seconds N
                      --why REASON [--at ISO|HH:MM] [--kill-on-timeout]
                      [--workdir DIR] [--model M] [--force --force-reason R] [--json]
  flow task status    <id> [--json]
  flow task list      [--state S] [--workitem W] [--json]
  flow task log       <id> [--tail N] [--json]
  flow task run       [<id>] [--max-parallel N] [--json]
  flow task pump      [--json]
  flow task reschedule <id> --at ISO|HH:MM [--json]
  flow task snapshot  <id> [--json]
  flow task reconcile [--json]
  flow task wake-text <id> [--json]
  flow task prune     [--state done|failed|timeout|killed] [--older-than N]
                      [--force] [--json]
退出码: 0 成功 / 1 运行失败 / 2 用法或前置错误(含门禁拒绝)/
        124 仅任务命令语义(不进 CLI 顶层)。
"""


class UsageError(Exception):
    """用法/前置错误(→ exit 2)。"""


class TaskError(Exception):
    """任务不存在(→ exit 2)。"""


class DuplicateWorkitem(Exception):
    """同 workitem 已有非终态任务(幂等拒绝,→ exit 2)。args=(已有 id, state)。"""


class QueueFull(Exception):
    """队列非终态达容量上限 queue_cap(→ exit 2)。"""


class GateReject(Exception):
    """门禁校验拒绝(flow-cost-ledger §1.4,fail-closed)。args=(error_code, 友好文案)。"""


class StoreError(Exception):
    """注册表解析/写盘失败或疑似 secret(→ exit 2)。"""


class CommandTimeout(Exception):
    """无 timeout 二进制的 Python 降级路径:命令超时。"""


# ---------------------------------------------------------------------------
# 状态机纯函数(§1.3,零 I/O)
# ---------------------------------------------------------------------------

def terminal_state(exit_code, timed_out):
    """命令结束 → 终态 + 原因码。零 I/O。"""
    if timed_out or exit_code == 124:
        return ("timeout", "timeout_exceeded")
    if exit_code == 0:
        return ("done", None)
    return ("failed", f"exit_code={exit_code}")


def sort_queue(tasks):
    """排队优先级:P0 < P1 < P2;同级 FIFO(created_at);再按 id 字典序(确定性)。"""
    q = [t for t in tasks if t.get("state") == "queued"]
    q.sort(key=lambda t: (PRIO_ORDER.get(t.get("priority"), 99),
                          t.get("created_at", ""), t.get("id", "")))
    return q


# ---------------------------------------------------------------------------
# cost-opt-cache-dispatch:调度亲和门控纯函数(design §1.4,全部零 I/O)
# 同仓串行(保 prefix cache 命中 ×30 省钱)+ pro 全局串行(压成本峰值);
# _select_gated 为唯一门控入口;双开关全关 → ordered[:free] 快路径(逐字节等价旧切片)。
# ---------------------------------------------------------------------------

def _serial_enabled(cfg, key):
    """串行开关读取:cfg=None → False(旧两参切片兼容);键缺失 → 默认 1(开);
    值损坏 → 默认 1(成本保守)。"""
    if cfg is None:
        return False
    v = (cfg.get("task") or {}).get(key, 1)
    try:
        return int(v) != 0
    except (TypeError, ValueError):
        return True


def _pro_models(cfg):
    """pro 模型名集合(从 config 读,不硬编码):designer.model.pro +
    executor.size.large.model 去重;块缺失 → 空集(fail-open)。"""
    pro = set()
    d = ((cfg or {}).get("roles") or {}).get("designer") or {}
    m = (d.get("model") or {}).get("pro")
    if m:
        pro.add(m)
    ex = ((cfg or {}).get("executor") or {}).get("size") or {}
    m2 = (ex.get("large") or {}).get("model")
    if m2:
        pro.add(m2)
    return pro


def _is_pro(t, cfg):
    """任务 model 字段 ∈ pro 模型集合 → pro;老条目缺 model → False(fail-open)。"""
    m = (t or {}).get("model")
    return bool(m) and m in _pro_models(cfg)


def _exempt(t):
    """P0 或 --force(audit 非空,force=True 时 add_task 才写 audit)→ 豁免串行等待。
    只跳过串行亲和门,不越 max_parallel 硬上限。"""
    return (t or {}).get("priority") == "P0" or bool((t or {}).get("audit"))


def _wd_key(t):
    """workdir 归一键(normpath+abspath 纯字符串运算,不触磁盘);缺 workdir → None。"""
    wd = (t or {}).get("workdir")
    return os.path.normpath(os.path.abspath(wd)) if wd else None


def _model_from_command(command):
    """命令串 shlex 解析 --model 后一 token(flow-core 已把 size→model 固化为
    `flow workitem execute … --model <X>`);无 --model / 解析失败 → None。"""
    try:
        toks = shlex.split(command or "")
    except ValueError:
        return None
    for i, tok in enumerate(toks):
        if tok == "--model" and i + 1 < len(toks) \
                and toks[i + 1] and not toks[i + 1].startswith("--"):
            return toks[i + 1]
    return None


def _default_model_for(command, kind, cfg):
    """kind 缺省 model 推导(§1.3):design → designer.model.pro;execute → 命令串
    --model 优先,否则 executor.size.medium.model(flash);其它 → None(fail-open)。"""
    if kind == "design":
        d = ((cfg or {}).get("roles") or {}).get("designer") or {}
        return (d.get("model") or {}).get("pro")
    if kind == "execute":
        m = _model_from_command(command)
        if m:
            return m
        ex = ((cfg or {}).get("executor") or {}).get("size") or {}
        return (ex.get("medium") or {}).get("model")
    return None


def _in_flight_wd(running):
    """running 中任务的 workdir 归一集合(缺 workdir 剔除,不参与同仓判定)。"""
    return {wd for wd in (_wd_key(t) for t in running) if wd is not None}


def _pro_running(running, cfg):
    """running 中是否有 pro 任务(pro 全局串行的门控源)。"""
    return any(_is_pro(t, cfg) for t in running)


def _select_gated(ordered, running, free, cfg):
    """单遍贪心:按既有排序取最多 free 个;同仓串行 + pro 串行 + P0/force 豁免。
    阻塞者 continue(留在队列),绝不 head-of-line 阻塞后续跨仓/flash 任务;
    被选(含豁免)者仍占用资源,供后续候选判定。双开关全关 → 旧切片快路径。"""
    if free <= 0:
        return []
    same_on = _serial_enabled(cfg, "same_workdir_serial")
    pro_on = _serial_enabled(cfg, "pro_serial")
    if not same_on and not pro_on:
        return ordered[:free]          # 双开关全关 → 与旧切片逐字节等价
    in_flight_wd = _in_flight_wd(running)
    pro_running_flag = _pro_running(running, cfg)
    selected = []
    for t in ordered:
        if len(selected) >= free:
            break
        wd = _wd_key(t)
        if _exempt(t):
            selected.append(t)
        elif same_on and wd is not None and wd in in_flight_wd:
            continue                     # 同仓:留在 queued/scheduled 等同类完成
        elif pro_on and pro_running_flag and _is_pro(t, cfg):
            continue                     # pro 全局串行:后续 pro 排队
        else:
            selected.append(t)
        if wd is not None:
            in_flight_wd.add(wd)
        if _is_pro(t, cfg):
            pro_running_flag = True
    return selected


def plan_dispatch(reg, max_parallel, cfg=None):
    """纯函数:空闲槽数 = max_parallel − running;返回应提升的任务列表(不改 reg)。
    只取 queued(绝不碰 scheduled;与 plan_pump 源分离)。
    cfg=None(两参调用)→ 旧切片行为;cfg 非空 → 走 _select_gated 亲和门控。"""
    running = [t for t in reg["tasks"].values() if t.get("state") == "running"]
    free = max(int(max_parallel) - len(running), 0)
    ordered = sort_queue(reg["tasks"].values())
    if cfg is None:
        return ordered[:free]            # 完全现状(两参调用 = 旧行为)
    return _select_gated(ordered, running, free, cfg)


def plan_pump(reg, max_parallel, now, cfg=None):
    """纯函数(pump,flow-cost-ledger §1.5(2)):到期 scheduled 排序 P0<P1<P2 +
    scheduled_at 升序 + id 字典序;只取 scheduled(绝不碰 queued)。P0 无窗口判断,仅靠排序置前。

    空槽 = max_parallel − running;无窗口硬门控(D4):到点即升,槽满留 scheduled 下轮再试。
    cfg=None(三参调用)→ 旧切片行为;cfg 非空 → 走 _select_gated 亲和门控。
    """
    def _due(t):
        sa = t.get("scheduled_at")
        if not sa:
            return False
        try:
            return datetime.fromisoformat(sa) <= now
        except (TypeError, ValueError):
            return False  # 时间戳损坏 → 保守不入

    due = [t for t in reg["tasks"].values()
           if t.get("state") == "scheduled" and _due(t)]
    due.sort(key=lambda t: (PRIO_ORDER.get(t.get("priority"), 99),
                            t.get("scheduled_at", ""), t.get("id", "")))
    running = [t for t in reg["tasks"].values() if t.get("state") == "running"]
    free = max(int(max_parallel) - len(running), 0)
    if cfg is None:
        return due[:free]
    return _select_gated(due, running, free, cfg)


# ---------------------------------------------------------------------------
# scheduled_at 解析 + 门禁校验链(flow-cost-ledger §1.4,fail-closed)
# ---------------------------------------------------------------------------

def parse_scheduled_at(s, now=None):
    """--at 解析(纯函数,零 I/O):ISO8601 带偏移 → 归一 UTC;HH:MM[:SS] 简写 →
    Asia/Shanghai 今日该时刻(已过则次日);naive 拒绝;过去时刻拒绝。
    返回 UTC ISO(timespec="seconds")。"""
    if s is None or str(s).strip() == "":
        raise UsageError("--at 不能为空")
    s = str(s).strip()
    now = now or datetime.now(timezone.utc)
    m = _HHMM_RE.fullmatch(s)
    if m:
        parts = [int(x) for x in s.split(":")]
        hh, mm = parts[0], parts[1]
        ss = parts[2] if len(parts) > 2 else 0
        if not (0 <= hh <= 23 and 0 <= mm <= 59 and 0 <= ss <= 59):
            raise UsageError("HH:MM[:SS] 时间非法")
        cn_now = now.astimezone(TZ_CN)
        cand = cn_now.replace(hour=hh, minute=mm, second=ss, microsecond=0)
        if cand <= cn_now:  # 已过(含恰好)→ 次日
            cand += timedelta(days=1)
        return cand.astimezone(timezone.utc).isoformat(timespec="seconds")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        raise UsageError("时间格式非法:须 ISO8601 带偏移(如 2026-08-22T12:00:00+08:00)或 HH:MM 简写")
    if dt.tzinfo is None:
        raise UsageError("时间缺少时区(naive),请补 +08:00 偏移")
    dt = dt.astimezone(timezone.utc)
    if dt < now:
        raise UsageError("时间为过去时刻,请改期或去掉 --at")
    return dt.isoformat(timespec="seconds")


def _gate_resolve_wi(wi_id, cfg):
    """锚定解析:workitem 目录存在(.flow/workitems/<id> + status.yaml)→ True。"""
    try:
        _fc.resolve_wi_dir(wi_id, dict(cfg, workitem={
            "id_max_len": 64, "plane_id": "control"}))
        return True
    except (_fc.UsageError, _fc.WorkitemError, KeyError, TypeError):
        return False


def _command_in_whitelist(command, cfg):
    """命令模板白名单(§1.4 第 10 步):3 正则任一命中 + 存在性校验。"""
    m = FLOW_WORKITEM_RE.match(command)
    if m:
        return _gate_resolve_wi(m.group(2), cfg)  # <id> 需 resolve_wi_dir 存在
    if RX_RE.match(command):
        return True
    m = SCRIPT_RE.match(command)
    if m:
        try:
            toks = shlex.split(command)
        except ValueError:
            return False
        if len(toks) < 2:
            return False
        # 只校验脚本参数(命令后第一个 token)在磁盘解析存在;输出路径等其余参数不参与
        script_arg = toks[1]
        return "/scripts/" in script_arg and os.path.isfile(os.path.abspath(script_arg))
    return False


def gate_validate(opts, cfg):
    """门禁校验链(§1.4,fail-closed 顺序 10 步)。任一不过 → 抛 GateReject(code, 文案)。
    不猜默认:priority/expected/why 全部强制显式。audit.force_reason 由 add_task 统一维护
    (单一来源防分叉),本函数不构造 patch。--force 仅跳过第 10 步模板白名单,其余 9 步不可绕过(G11 锁死)。"""
    task_cfg = (cfg or {}).get("task") or {}
    command = str(opts.get("command") or "").strip()
    kind = opts.get("kind")
    # 1) kind 白名单(2026-08-22 收窄:仅 design/execute 入队;verify/review/batch 拒,文案区分)
    if kind not in KINDS:
        if kind in ("verify", "review"):
            raise GateReject("invalid_kind",
                             f"kind={kind} 不入队:verify/review 走 flow workitem 同步命令,不入队。")
        if kind == "batch":
            raise GateReject("invalid_kind",
                             "kind=batch 不入队:batch 是触发动作非任务。")
        raise GateReject("invalid_kind",
                         f"kind={kind} 不在任务白名单({'/'.join(KINDS)})。提醒请走 cron。")
    # 2) command 非空
    if not command:
        raise GateReject("empty_command", "--command 为空。")
    # 3) bash -n 语法(bash 缺失同拒,fail-closed)。
    #    注意:runner 实际经 sh -c 执行,门禁是「防脏」下限而非等价校验(风险表 §3);
    #    语法通过但 sh 语义不同 → 命令失败走 failed 终态 + failure_tail,安全侧不扩大。
    try:
        proc = subprocess.run(["bash", "-n", "-c", command], check=False,
                              stdin=subprocess.DEVNULL,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except OSError:
        raise GateReject("command_syntax_error", "command 语法非法(bash 不可用)。")
    if proc.returncode != 0:
        raise GateReject("command_syntax_error", "command 语法非法(bash -n 失败)。")
    # 4) 锚定:--workitem 存在 或 command 含项目路径
    wi_id = opts.get("workitem")
    anchored = (wi_id is not None and _gate_resolve_wi(wi_id, cfg)) \
        or _PROJ_PATH_RE.search(command) is not None
    if not anchored:
        raise GateReject("not_anchored",
                         "既无 --workitem 锚点(workitem 不存在)也无可解析项目路径。")
    # 5) --priority 必填(不猜默认)
    pri = opts.get("priority")
    if pri not in PRIORITIES:
        raise GateReject("priority_required", "--priority 必填,须为 P0/P1/P2。")
    # 6) --expected-seconds 必填(正整数)
    exp_raw = opts.get("expected-seconds")
    try:
        exp = int(exp_raw) if exp_raw is not None and str(exp_raw).strip() != "" else None
    except (TypeError, ValueError):
        exp = None
    if exp is None or exp <= 0:
        raise GateReject("expected_required",
                         "--expected-seconds 必填(正整数,调度/窗口建议依赖)。")
    # 7) 幂等 + 8) 容量(无锁前置检查;add_task 锁内再原子判定,防 TOCTOU)
    reg = load_registry()
    if wi_id is not None:
        for t in reg["tasks"].values():
            if t.get("workitem") == wi_id and t.get("state") in NON_TERMINAL:
                raise GateReject("duplicate_workitem",
                                 f"workitem={wi_id} 已有非终态任务 {t['id']}({t['state']})。")
    cap = int(task_cfg.get("queue_cap") or 50)
    if sum(1 for t in reg["tasks"].values() if t.get("state") in NON_TERMINAL) >= cap:
        raise GateReject("queue_full",
                         f"队列非终态已达上限 {cap},请先清理/完成后重试。")
    # 9) --why 必填
    why = opts.get("why")
    if not why or not str(why).strip():
        raise GateReject("why_required", "--why 必填(审计理由:这是否真是一个任务?)。")
    # 10) 命令模板白名单(仅无 --force 时)
    if not opts.get("force"):
        if not _command_in_whitelist(command, cfg):
            raise GateReject("command_not_whitelisted",
                             "command 不在命令模板白名单(flow workitem …/rx …/bash|python …/scripts/…)。"
                             "确需自由命令请显式 --force 并附 --force-reason。")
    # audit.force_reason 由 add_task 统一维护(940 行),gate 不构造 patch(单一来源防分叉)


def _now_ms():
    """毫秒级 ISO 时间戳：任务队列调度精度（同秒出队可区分，C26 优先级断言依赖）。"""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def validate_entry(entry):
    """fail-closed:state/priority/id 形状非法 → StoreError;未知字段忽略(向前兼容)。"""
    st = entry.get("state")
    if st not in TASK_STATES:
        raise StoreError(f"非法 state: {st!r}")
    prio = entry.get("priority")
    if prio not in PRIORITIES:
        raise StoreError(f"非法 priority: {prio!r}")
    tid = entry.get("id")
    if not TASK_ID_RE.fullmatch(str(tid)):
        raise StoreError(f"非法 id 形状: {tid!r}")
    return entry


def extract_tail(path, max_bytes):
    """日志尾部抽取(§1.4(5)):缺失→空串、超长→截断、非 UTF-8→errors=replace、secret→[REDACTED]。"""
    if not os.path.isfile(path):
        return ""
    with open(path, "rb") as f:
        f.seek(0, 2)
        size = f.tell()
        f.seek(max(0, size - max_bytes))
        raw = f.read(max_bytes)
    text = raw.decode("utf-8", errors="replace")
    return DENY_RE.sub("[REDACTED]", text)


# ---------------------------------------------------------------------------
# 配置加载(§1.2:defaults 的 task 块 + user 合并 + env 覆盖)
# ---------------------------------------------------------------------------

def _merge(a, b):
    out = dict(a)
    for k, v in b.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge(out[k], v)
        else:
            out[k] = v
    return out


def _env_int(name):
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def load_task_config():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    defaults_path = os.environ.get("COLLABFLOW_DEFAULTS") or os.path.join(
        script_dir, "..", "config", "defaults.yaml")
    defaults = _fc.parse_yaml(_fc.read_file(defaults_path), defaults_path)
    task = dict(defaults.get("task") or {})
    host = dict(defaults.get("host") or {})
    chain = dict(defaults.get("chain") or {})  # W-B §1.7:追加 chain 块(键缺失由调用方硬编码兜底)
    roles = dict(defaults.get("roles") or {})     # cost-opt-cache-dispatch §1.2:透传 roles/executor
    executor = dict(defaults.get("executor") or {})  # 供 pro 判定与 model 缺省解析(旧消费方只取 task/host/chain)
    user_path = os.environ.get("COLLABFLOW_CONFIG") or os.path.expanduser(
        "~/.config/collabflow/config.yaml")
    if os.path.isfile(user_path):
        user = _fc.parse_yaml(_fc.read_file(user_path), user_path)
        task = _merge(task, dict(user.get("task") or {}))
        host = _merge(host, dict(user.get("host") or {}))
        chain = _merge(chain, dict(user.get("chain") or {}))
        roles = _merge(roles, dict(user.get("roles") or {}))
        executor = _merge(executor, dict(user.get("executor") or {}))
    mp = _env_int("FLOW_TASK_MAX_PARALLEL")
    if mp is not None:
        task["max_parallel"] = mp
    lb = _env_int("FLOW_TASK_LOG_TAIL_BYTES")
    if lb is not None:
        task["log_tail_bytes"] = lb
    for env_key, cfg_key in (
        ("FLOW_TASK_PRUNE_DONE_DAYS", "prune_done_days"),
        ("FLOW_TASK_PRUNE_FAILED_DAYS", "prune_failed_days"),
        ("FLOW_TASK_TASK_CAP", "task_cap"),
        ("FLOW_TASK_STALE_AFTER_S", "stale_after_s"),
        ("FLOW_TASK_QUEUE_CAP", "queue_cap"),
    ):
        v = _env_int(env_key)
        if v is not None:
            task[cfg_key] = v
    # W-B §1.7:stale 分级阈值环境覆盖(键缺失 → classify_stale_age 内硬编码默认 2/24/48)
    for env_key, cfg_key in (
        ("FLOW_STALE_REMIND_HOURS", "remind_hours"),
        ("FLOW_STALE_WARN_HOURS", "warn_hours"),
        ("FLOW_STALE_FORCE_HOURS", "force_hours"),
    ):
        v = _env_int(env_key)
        if v is not None:
            chain[cfg_key] = v
    # W-S2 §2.7:rescue/token 阈值环境覆盖(键缺失 → _rescue_max_retries/_token_warn_threshold
    # 内硬编码默认 2/300000;user config task 块同名键亦可覆盖,defaults.yaml 不新增键)
    for env_key, cfg_key in (
        ("FLOW_TASK_RESCUE_MAX_RETRIES", "rescue_max_retries"),
        ("FLOW_TASK_TOKEN_WARN_THRESHOLD", "token_warn_threshold"),
    ):
        v = _env_int(env_key)
        if v is not None:
            task[cfg_key] = v
    # cost-opt-cache-dispatch §1.2:串行亲和开关(默认 1=开;置 0 恢复现状;非整数 → 默认 1;
    # env 优先覆盖,user config 显式值保留,仅键缺失时硬编码兜底 1)
    for env_key, cfg_key, default in (
        ("FLOW_TASK_SAME_WORKDIR_SERIAL", "same_workdir_serial", 1),
        ("FLOW_TASK_PRO_SERIAL", "pro_serial", 1),
    ):
        v = _env_int(env_key)
        if v is not None:
            task[cfg_key] = v
        elif cfg_key not in task:
            task[cfg_key] = default
    return {"task": task, "host": host, "chain": chain,
            "roles": roles, "executor": executor}


# ---------------------------------------------------------------------------
# store 层(§1.4:flock 注册表 + 原子写)
# ---------------------------------------------------------------------------

def task_dir():
    """数据根目录;env FLOW_TASK_DIR 可覆盖(测试隔离必用),默认 ~/.collabflow。"""
    return os.environ.get("FLOW_TASK_DIR") or os.path.expanduser("~/.collabflow")


def registry_path():
    return os.path.join(task_dir(), "tasks.json")


def lock_path():
    return registry_path() + ".lock"


def logs_dir():
    return os.path.join(task_dir(), "logs")


def log_path(tid):
    return os.path.join(logs_dir(), f"{tid}.log")


# ---------------------------------------------------------------------------
# W-V1:execute 基线快照层(design verify-baseline-snapshot §2.3–§2.5)
# 快照 = 任务启动瞬间的 git 状态(时间锚点),供 W-V2 verify 差分消费;
# 独立文件 <task_dir>/tasks/<id>.snapshot.json,不挤 registry 并发写热点。
# ---------------------------------------------------------------------------

def snapshots_dir():
    """快照目录(design D1:与 registry/logs 同根,prune 同生命周期)。"""
    return os.path.join(task_dir(), "tasks")


def snapshot_path(tid):
    """快照文件路径(单点封装,路径细节改动只改此处)。"""
    return os.path.join(snapshots_dir(), f"{tid}.snapshot.json")


def _git_run(workdir, *args, _git="git"):
    """git 只读子命令执行(短超时);OSError/超时 → None(零抛,降级由调用方处理)。
    -c core.quotePath=false:diff/ls-files 输出原始 UTF-8 路径而非八进制转义,
    hash-object 才能按真实文件名取 hash;errors="replace":非 UTF-8 文件名不抛。"""
    try:
        return subprocess.run([_git, "-c", "core.quotePath=false", "-C", workdir, *args],  # noqa: PLW1510
                              capture_output=True, text=True, errors="replace",
                              timeout=SNAPSHOT_GIT_TIMEOUT_S)
    except (OSError, subprocess.TimeoutExpired, UnicodeDecodeError):
        return None


def _git_hash_batch(workdir, paths, _git="git"):
    """批量 content-hash(ocr7-M2):一次 `git hash-object --stdin-paths` 拿全部
    M 文件 hash,替代逐文件 spawn(输出按输入路径顺序每行一个 40-hex)。

    批量失败(含已删文件 E4 / 非 UTF-8 文件名)→ 逐文件回退,保持原语义
    (已删 → None;非 UTF-8 经 argv 字节透传仍可取 hash);空路径 → []。"""
    if not paths:
        return []
    try:
        proc = subprocess.run(  # noqa: PLW1510
            [_git, "-c", "core.quotePath=false", "-C", workdir, "hash-object",
             "--stdin-paths"],
            input="\n".join(paths) + "\n", capture_output=True, text=True,
            errors="replace", timeout=SNAPSHOT_GIT_TIMEOUT_S)
    except (OSError, subprocess.TimeoutExpired, UnicodeDecodeError):
        proc = None
    if proc is None or proc.returncode != 0:
        return [_git_file_hash(workdir, p, _git=_git) for p in paths]
    lines = (proc.stdout or "").splitlines()
    if len(lines) != len(paths):
        # 理论情况:输出行数与输入路径数不一致(防 hash 错位)→ 逐文件回退
        return [_git_file_hash(workdir, p, _git=_git) for p in paths]
    out = []
    for i in range(len(paths)):
        ln = lines[i].strip() if i < len(lines) else ""
        out.append(ln or None)                        # 缺失行 → None(E4)
    return out


def _git_file_hash(workdir, path, _git="git"):
    """单文件 content-hash(逐文件回退路径;文件已删 → None)。"""
    h = _git_run(workdir, "hash-object", "--", path, _git=_git)
    return h.stdout.strip() if (h is not None and h.returncode == 0) else None


def capture_git_snapshot(workdir, _git="git"):
    """拍 git 基线快照(design §2.4):head + M 文件 content-hash + untracked 清单。
    零抛异常:任何 git 异常 → {git:false, error:...} 降级(不阻断任务启动);
    _git 为测试 seam(可注入缺失二进制/失败命令)。"""
    workdir = workdir or os.getcwd()  # E10 老任务 workdir 缺失回退
    if shutil.which(_git) is None:    # E2 git 二进制缺失
        return {"schema_version": SNAPSHOT_SCHEMA_VERSION, "git": False,
                "error": "git_missing"}
    inside = _git_run(workdir, "rev-parse", "--is-inside-work-tree", _git=_git)
    if inside is None or inside.returncode != 0:  # E1 非 git 工作树
        return {"schema_version": SNAPSHOT_SCHEMA_VERSION, "git": False,
                "error": "not_a_git_repo"}
    head = _git_run(workdir, "rev-parse", "HEAD", _git=_git)  # E3 空仓无 commit → 降级
    if head is None or head.returncode != 0:
        return {"schema_version": SNAPSHOT_SCHEMA_VERSION, "git": False,
                "error": "rev_parse_failed"}
    tracked = {}
    names = _git_run(workdir, "diff", "--name-only", _git=_git)  # 与 executor .diff.names 同源(D2)
    if names is not None and names.returncode == 0:
        paths = [p.strip() for p in names.stdout.splitlines() if p.strip()]
        # ocr7-M2:批量一次 git hash-object --stdin-paths 拿全部 M 文件 hash
        # (替代逐文件 spawn N 个子进程;execute dispatch 的 registry flock 内
        # 少起 N-1 次进程);含已删/非 UTF-8 文件名 → 批量失败 → 逐文件回退
        for p, h in zip(paths, _git_hash_batch(workdir, paths, _git=_git)):
            tracked[p] = h
    untracked = []
    ls = _git_run(workdir, "ls-files", "-o", "--exclude-standard", _git=_git)  # 文件级,排除 ignored(D3)
    if ls is not None and ls.returncode == 0:
        untracked = [l.strip() for l in ls.stdout.splitlines() if l.strip()]
    return {"schema_version": SNAPSHOT_SCHEMA_VERSION, "git": True,
            "head": head.stdout.strip(), "tracked_modified": tracked,
            "untracked": untracked}


def write_snapshot(tid, snap):
    """原子写快照(temp + fsync + os.replace,同 save_registry_atomic);
    失败 → 返回 False + warning,不抛(E8);命中 DENY_RE → 拒写(E9)。"""
    path = snapshot_path(tid)
    try:
        text = json.dumps(snap, ensure_ascii=False, indent=2) + "\n"
        if DENY_RE.search(text):
            print(f"告警: 快照含疑似 secret, 拒写 {tid}", file=sys.stderr)
            return False
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.tmp.{os.getpid()}.{secrets.token_hex(4)}"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        return True
    except OSError as e:
        print(f"告警: 快照写入失败 {tid}: {e}", file=sys.stderr)
        return False
    finally:
        if 'tmp' in locals() and os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def load_snapshot(tid):
    """读取接口(W-V2 唯一消费入口):缺失/损坏/形状非法 → None(降级,不抛)。

    契约(design §2.1,W-V2 消费):
      - git:false → 非 git 仓/异常快照,含 error 字段(not_a_git_repo/git_missing/...)
      - git:true → 含 head/tracked_modified/untracked;tracked_modified 值为
        `git hash-object <path>` 的 40-hex blob sha,None 表示基线时该文件已被删除;
        untracked 为文件级 repo-root-relative 清单(已排除 .gitignore)。
    """
    path = snapshot_path(tid)
    if not os.path.isfile(path):
        return None  # E5
    try:
        data = json.loads(_fc.read_file(path))
    except (ValueError, OSError):
        return None  # E6
    if not isinstance(data, dict) or data.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
        return None  # E7
    git = data.get("git")
    if git is True:
        ok = (isinstance(data.get("head"), str) and data["head"]
              and isinstance(data.get("tracked_modified"), dict)
              and isinstance(data.get("untracked"), list))
        if not ok:
            return None  # E7
    elif git is not False:
        return None  # E7
    return data


def capture_and_write_snapshot(t):
    """提升路径统一挂钩(W-V1):仅 execute 拍快照并原子落盘;绝不抛,失败仅告警。
    三入口(dispatch/run/pump)在 save_registry_atomic 前调用,保证
    「registry 落盘 running ⇒ 快照已落盘」不变式(E12)。"""
    if not isinstance(t, dict) or t.get("kind") != "execute":  # D5 仅 execute
        return
    tid = t.get("id")
    if not tid:
        return
    snap = capture_git_snapshot(t.get("workdir"))
    snap["task_id"] = tid
    snap["captured_at"] = _now_ms()
    snap["workdir"] = t.get("workdir") or os.getcwd()
    ok = write_snapshot(tid, snap)
    if not ok:
        print(f"告警: 快照未落盘 {tid}, 任务降级启动(verify 无法差分)", file=sys.stderr)


def _remove_snapshot_best_effort(tid):
    """删快照文件;不存在/失败静默(同 _remove_log_best_effort 语义,幂等,E13)。"""
    try:
        os.remove(snapshot_path(tid))
    except OSError:
        pass


def empty_registry():
    return {"schema_version": SCHEMA_VERSION, "tasks": {}}


def load_registry():
    """读注册表:缺失 → 空注册表;非 JSON / 顶层非 dict / schema_version 非法 / 条目非法 → StoreError。"""
    path = registry_path()
    if not os.path.isfile(path):
        return empty_registry()
    try:
        data = json.loads(_fc.read_file(path))
    except ValueError:
        raise StoreError(f"{path}: 注册表损坏(非 JSON)")
    if not isinstance(data, dict):
        raise StoreError(f"{path}: 注册表顶层必须是对象")
    if data.get("schema_version") != SCHEMA_VERSION:
        raise StoreError(f"{path}: schema_version 非法: {data.get('schema_version')!r}")
    tasks = data.get("tasks")
    if not isinstance(tasks, dict):
        raise StoreError(f"{path}: tasks 必须是对象")
    for tid, entry in tasks.items():
        if not isinstance(entry, dict):
            raise StoreError(f"{path}: 任务 {tid} 非法(非对象)")
        validate_entry(dict(entry, id=entry.get("id", tid)))
    return data


def save_registry_atomic(reg):
    """原子写(§4.1 同款):temp + fsync + os.replace;整表写入前过 DENY_RE。"""
    path = registry_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    text = json.dumps(reg, ensure_ascii=False, indent=2) + "\n"
    if DENY_RE.search(text):
        raise StoreError("注册表含疑似 secret, 拒绝写入")
    tmp = f"{path}.tmp.{os.getpid()}.{secrets.token_hex(4)}"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def with_registry_flock(fn):
    """flock sidecar 串行化读-改-写(§1.4);无 fcntl(非 POSIX)降级为无锁直执行。"""
    if fcntl is None or not hasattr(fcntl, "flock"):
        return fn()
    os.makedirs(task_dir(), exist_ok=True)
    fd = os.open(lock_path(), os.O_CREAT | os.O_RDWR, 0o644)
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        return fn()
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def read_log(path, tail_bytes=None):
    """读统一日志:缺失 → "";tail_bytes=None → 全量;输出过 DENY_RE(红线:日志不泄 secret)。"""
    if not os.path.isfile(path):
        return ""
    with open(path, "rb") as f:
        if tail_bytes is None:
            raw = f.read()
        else:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - tail_bytes))
            raw = f.read(tail_bytes)
    text = raw.decode("utf-8", errors="replace")
    return DENY_RE.sub("[REDACTED]", text)


# ---------------------------------------------------------------------------
# 事件层(§1.3:只增不删、可重放;best-effort,注册表是唯一权威终态)
# ---------------------------------------------------------------------------

EVENT_SCHEMA_VERSION = 1


def events_dir():
    return os.path.join(task_dir(), "events")


def event_path(tid):
    return os.path.join(events_dir(), f"{tid}.jsonl")


def event_lock_path(tid):
    return event_path(tid) + ".lock"


def count_lines(path):
    """事件文件行数(= 下一条 seq);缺失 → 0。"""
    if not os.path.isfile(path):
        return 0
    n = 0
    with open(path, "rb") as f:
        for _ in f:
            n += 1
    return n


def _append_jsonl_locked(path, lockp, record):
    """通用 jsonl flock 追加(事件/审计共用);seq=行数+1;只增不删;DENY_RE fail-closed。"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    text = json.dumps(record, ensure_ascii=False)
    if DENY_RE.search(text):
        raise StoreError("事件含疑似 secret, 拒绝写入")
    fd = os.open(lockp, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        if fcntl is not None and hasattr(fcntl, "flock"):
            fcntl.flock(fd, fcntl.LOCK_EX)
        seq = count_lines(path) + 1
        rec = dict(record)
        rec["seq"] = seq
        line = json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n"
        with open(path, "a", encoding="utf-8") as f:  # 只增不删,无截断
            f.write(line)
            f.flush()
            os.fsync(f.fileno())
    finally:
        if fcntl is not None and hasattr(fcntl, "flock"):
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
    return seq


def append_task_event(tid, record):
    """events/<tid>.jsonl flock 追加;seq=行数+1;只增不删(open "a" 无截断路径);
    写入前过 DENY_RE(fail-closed)。失败=best-effort(注册表仍是权威)。"""
    return _append_jsonl_locked(event_path(tid), event_lock_path(tid), record)


def append_audit_event(record):
    """审计事件 events/pruned.jsonl 追加(§5.1/§5.2 清理/僵尸标记落账;无 tid 维度)。"""
    return _append_jsonl_locked(
        os.path.join(events_dir(), "pruned.jsonl"),
        os.path.join(events_dir(), "pruned.jsonl.lock"),
        record)


def emit_audit_best_effort(record):
    """审计事件 best-effort:失败仅告警,不影响清理主流程。"""
    try:
        append_audit_event(record)
    except (StoreError, OSError) as e:
        print(f"告警: 审计事件写入失败: {e}", file=sys.stderr)


def _emit_task_event_best_effort(tid, record):
    """事件写入 best-effort:失败仅告警,不改任务终态。"""
    try:
        append_task_event(tid, record)
    except (StoreError, OSError) as e:
        print(f"告警: 事件写入失败 {tid}: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# duration→expected_seconds 种子(§1.4(2):EMA + last[N] 截断 + flock 原子写)
# ---------------------------------------------------------------------------

SEED_SCHEMA_VERSION = 1


def seed_path():
    return os.path.join(task_dir(), "duration-seed.json")


def seed_lock_path():
    return seed_path() + ".lock"


def empty_seed():
    return {"schema_version": SEED_SCHEMA_VERSION, "kinds": {}}


def load_seed():
    """读种子库:缺失 → 空;非 JSON/顶层非 dict → 空(回退 fallback,不 crash,§E5)。"""
    path = seed_path()
    if not os.path.isfile(path):
        return empty_seed()
    try:
        data = json.loads(_fc.read_file(path))
    except ValueError:
        return empty_seed()
    if not isinstance(data, dict):
        return empty_seed()
    return data


def save_seed_atomic(seed):
    """原子写(同注册表):temp + fsync + os.replace;整表写入前过 DENY_RE。"""
    path = seed_path()
    os.makedirs(task_dir(), exist_ok=True)
    text = json.dumps(seed, ensure_ascii=False, indent=2) + "\n"
    if DENY_RE.search(text):
        raise StoreError("种子库含疑似 secret, 拒绝写入")
    tmp = f"{path}.tmp.{os.getpid()}.{secrets.token_hex(4)}"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def with_seed_flock(fn):
    """种子库 flock 串行化(同注册表模式);非 POSIX 降级无锁直执行。"""
    if fcntl is None or not hasattr(fcntl, "flock"):
        return fn()
    os.makedirs(task_dir(), exist_ok=True)
    fd = os.open(seed_lock_path(), os.O_CREAT | os.O_RDWR, 0o644)
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        return fn()
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def seed_expected(history, fallback):
    """纯函数:history=[int>0...](chronological);空→fallback;
    否则 max(fallback, ceil(EMA(α=0.5)*1.5))。"""
    if not history:
        return int(fallback)
    ema = float(history[0])
    for d in history[1:]:
        ema = 0.5 * float(d) + 0.5 * ema
    return max(int(fallback), math.ceil(ema * 1.5))


def update_seed(store, kind, duration_s, max_len=8):
    """纯函数:duration 入种子库,维护 last[N] + EMA;非法 duration 忽略。"""
    if not isinstance(duration_s, int) or duration_s <= 0:
        return store
    k = store.setdefault("kinds", {}).setdefault(
        kind, {"ema": 0.0, "count": 0, "last": []})
    k["count"] += 1
    k["last"] = (k["last"] + [duration_s])[-max_len:]
    k["ema"] = float(duration_s) if k["count"] == 1 else 0.5 * duration_s + 0.5 * k["ema"]
    return store


def _load_seed_kind(kind):
    """读某 kind 的历史 duration 样本(纯正数,chronological)。"""
    seed = load_seed()
    k = (seed.get("kinds") or {}).get(kind) or {}
    hist = k.get("last") or []
    return [int(d) for d in hist if isinstance(d, int) and d > 0]


def _record_duration_best_effort(kind, duration_s, max_len):
    """runner 终态回灌种子(flock 内读-改-写);失败仅告警。"""
    if not kind or duration_s is None:
        return
    try:
        with_seed_flock(lambda: save_seed_atomic(
            update_seed(load_seed(), kind, duration_s, max_len)))
    except (StoreError, OSError) as e:
        print(f"告警: 种子更新失败 kind={kind}: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# 运行层(§1.4(2)/§1.6:reconcile / dispatch / spawn / run_command)
# ---------------------------------------------------------------------------

def pid_alive(pid):
    """探活:仅一处 os.kill(pid, 0);非 POSIX 降级恒真(依赖超时兜底)。"""
    if pid is None:
        return False
    if fcntl is None:
        return True
    try:
        os.kill(int(pid), 0)
        return True
    except (ProcessLookupError, ValueError, TypeError):
        return False
    except PermissionError:
        return True


def reconcile_running(reg):
    """失联回收(§1.4(4)):running + 死 pid → failed;running + pid=None 仅在陈旧后回收。

    pid=None 同时覆盖两个场景:dispatch 刚提升、runner 尚未写 pid 的窗口(正常,不能回收),
    与 spawn 崩溃残留(该回收)。以 started_at 年龄区分(RECONCILE_GRACE_S 宽限),
    避免并发 dispatch 误杀刚提升的任务;真正失联的终会被回收,能力不削弱。
    """
    now = datetime.now(timezone.utc)
    reaped = []
    for tid, t in reg["tasks"].items():
        if t.get("state") != "running":
            continue
        pid = t.get("pid")
        if pid is not None and pid_alive(pid):
            continue
        if pid is None:
            started = t.get("started_at")
            if started:
                try:
                    if (now - datetime.fromisoformat(started)).total_seconds() < RECONCILE_GRACE_S:
                        continue
                except (TypeError, ValueError):
                    pass  # 时间戳非法 → 按陈旧处理,回收
        t["state"] = "failed"
        t["exit_code"] = None
        t["finished_at"] = _now_ms()
        t["failure_tail"] = "runner 失联(进程不存在)"
        reaped.append(tid)
    return reaped


def _runner_env(t, base_env=None):
    """任务命令执行 env(任务书 §4/设计 §1.6:跨仓 workitem 实测必修)。

    显式跨仓(workdir ≠ 当前进程 cwd)时注入 FLOW_DATA_DIR=<workdir>/.flow +
    FLOW_WORKDIR=<workdir>,使 workitem 解析落到任务归属仓而非 collab-flow
    默认目录;默认归属(workdir==cwd,add 未显式 --workdir)保持 base_env 现状
    (FLOW_DATA_DIR 未设 → cwd/.flow,与 flow-core.data_dir 口径一致)。
    纯函数,零 I/O。"""
    env = dict(base_env if base_env is not None else os.environ)
    wd = (t or {}).get("workdir")
    if wd and os.path.abspath(wd) != os.path.abspath(os.getcwd()):
        env["FLOW_DATA_DIR"] = os.path.join(wd, ".flow")
        env["FLOW_WORKDIR"] = wd
    else:
        env.setdefault("FLOW_DATA_DIR", os.path.join(os.getcwd(), ".flow"))
    return env


def run_command(t, cfg):
    """执行任务命令(§1.6.1):expected_seconds 用 timeout(coreutils)包裹;无该二进制走 Python 降级。
    子进程 env 经 _runner_env 注入 workdir 的 FLOW_DATA_DIR/FLOW_WORKDIR(任务书 §4)。"""
    expected = t.get("expected_seconds")
    env = _runner_env(t)
    if expected:
        bin_ = shutil.which("timeout")
        if bin_:
            # design §1.6.1：超时优雅 TERM 终止（默认）；--kill-on-timeout 追加 KILL 兜底
            base = [bin_, "--signal=TERM"]
            if t.get("kill_on_timeout"):
                base += ["--kill-after", str(cfg["task"].get("kill_grace_s", 5))]
            base += [str(expected), "sh", "-c", t["command"]]
            proc = subprocess.run(base, env=env, check=False)
            rc = proc.returncode
            # kill-on-timeout 且 SIGTERM 被忽略 → --kill-after 触发 SIGKILL,timeout 报
            # 128+SIGKILL=137(而非 124);统一为超时语义,保证「超时→timeout」终态不削弱。
            if t.get("kill_on_timeout") and rc == 137:
                return 124
            return rc
        return _run_with_py_timeout(t)  # 降级:超时 SIGKILL(≈kill-on-timeout)
    proc = subprocess.run(["sh", "-c", t["command"]], env=env, check=False)
    return proc.returncode


def _run_with_py_timeout(t):
    """无 timeout 二进制降级(§1.6.6):subprocess.run(timeout=...) → 超时抛 CommandTimeout。"""
    try:
        proc = subprocess.run(["sh", "-c", t["command"]], timeout=t["expected_seconds"], check=False,
                              env=_runner_env(t))
        return proc.returncode
    except subprocess.TimeoutExpired:
        raise CommandTimeout()


def spawn_runner(tid):
    """分离式 spawn 落账 runner(§1.4(2) 锁外):stdout/stderr → logs/<id>.log,setssid 孤儿。
    子进程 env 注入任务 workdir 的 FLOW_DATA_DIR/FLOW_WORKDIR(任务书 §4:跨仓 workitem 解析);
    entry 读取失败/缺失 → 回退环境默认(不阻断 spawn)。"""
    os.makedirs(logs_dir(), exist_ok=True)
    with open(log_path(tid), "ab") as log_fd:
        popen_kw = {}
        if os.name == "posix":
            popen_kw["start_new_session"] = True
        env = os.environ
        try:
            entry = load_registry()["tasks"].get(tid)
            if entry is not None:
                env = _runner_env(entry)
        except (StoreError, OSError):
            pass  # 注册表读失败 → 回退 env/默认(不阻断 spawn)
        subprocess.Popen(
            [sys.executable, os.path.abspath(__file__), "_runner", tid],
            stdin=subprocess.DEVNULL, stdout=log_fd, stderr=subprocess.STDOUT,
            env=env, close_fds=True, **popen_kw)


def dispatch(cfg, max_parallel=None):
    """调度(§1.4(2)):flock 内 reconcile → plan_dispatch(优先级+空闲槽) → 提升保存;锁外写 running 事件 + spawn。"""
    m = max_parallel if max_parallel is not None else int(cfg["task"]["max_parallel"])

    def _do():
        reg = load_registry()
        reconcile_running(reg)
        promoted = plan_dispatch(reg, m, cfg)
        now = _now_ms()
        for t in promoted:
            capture_and_write_snapshot(t)  # W-V1:先拍快照(execute),后写 registry running(E12)
            t["state"] = "running"
            t["started_at"] = now
            t["pid"] = None  # runner 启动后自填(§1.4(2))
        save_registry_atomic(reg)
        return [(t["id"], dict(t)) for t in promoted]

    promoted = with_registry_flock(_do)
    for tid, t in promoted:
        _emit_task_event_best_effort(tid, {
            "schema_version": EVENT_SCHEMA_VERSION, "ts": now_iso(),
            "task_id": tid, "workitem": t.get("workitem"), "kind": t.get("kind"),
            "state": "running", "exit_code": None, "duration_s": None,
            "expected_seconds": t.get("expected_seconds"),
            "started_at": t.get("started_at"), "finished_at": None,
            "diagnostic": None, "partial_complete": False,
        })
        spawn_runner(tid)
    return [tid for tid, _ in promoted]


def add_task(cfg, command, workitem=None, priority="P2",
             expected_seconds=None, kill_on_timeout=False, kind=None, workdir=None,
             why=None, scheduled_at=None, force=False, force_reason=None, model=None,
             executor=None):
    """幂等入队(§1.4(1)):flock 内去重(同 workitem 非终态 → DuplicateWorkitem) + 追加 + 自动 dispatch。

    kind 非空时:缺省 expected_seconds → 种子回退(config 默认);注册表条目带 kind 字段。
    workdir 默认 os.getcwd(),仅作跨项目归属记录(§1.2),不改变 runner 执行 cwd;
    目录不存在也接受字符串不校验。
    scheduled_at 非 None → state=scheduled 且不 auto-dispatch(交给 pump);否则 queued + auto-dispatch。
    why 必填(非空白);force 落 audit.force_reason(仅模板白名单可撬,由 cmd_add gate 保证)。
    完整 gate_validate 由 cmd_add(CLI 入口)强制;本函数保留形状校验 + 锁内原子幂等/容量
    (防程序化误用与并发 TOCTOU 的双保险)。
    queued 事件在注册表追加后 best-effort 写入(§1.3)。
    """
    if command is None or command.strip() == "":
        raise UsageError("--command 不能为空")
    if priority not in PRIORITIES:
        raise UsageError(f"--priority 必须为 {'/'.join(PRIORITIES)}: {priority}")
    if kind is not None and kind not in KINDS:
        raise UsageError(f"--kind 必须为 {'/'.join(KINDS)}: {kind}")
    if expected_seconds is not None:
        if not isinstance(expected_seconds, int) or expected_seconds <= 0:
            raise UsageError(f"--expected-seconds 必须是正整数: {expected_seconds}")
    if why is not None and (not isinstance(why, str) or not why.strip()):
        raise UsageError("--why 必填(审计理由:这是否真是一个任务?)")
    if DENY_RE.search(command):
        raise StoreError("command 含疑似 secret, 拒绝写入")
    # cost-opt-cache-dispatch §1.3:model 缺省推导(显式 --model 优先;老条目/推导失败 → None,非 pro)
    model = model or _default_model_for(command, kind, cfg)

    def _do():
        reg = load_registry()
        if workitem is not None:
            for t in reg["tasks"].values():
                if t.get("workitem") == workitem and t["state"] in NON_TERMINAL:
                    raise DuplicateWorkitem(t["id"], t["state"])
        cap = int((cfg["task"].get("queue_cap") if cfg.get("task") else None) or 50)
        if sum(1 for t in reg["tasks"].values() if t["state"] in NON_TERMINAL) >= cap:
            raise QueueFull(cap)
        # 写时自动清理 + 僵尸标记(§5.1/§5.2):同锁内跑,审计事件 best-effort
        pruned = auto_prune(reg, cfg)
        stale = mark_stale_running(reg, cfg)
        if pruned or stale:
            emit_audit_best_effort({
                "schema_version": EVENT_SCHEMA_VERSION, "ts": now_iso(),
                "kind": "prune", "pruned": pruned, "stale": stale,
            })
        tid = "t-" + secrets.token_hex(6)
        while tid in reg["tasks"]:  # 冲突则重试(§1.4(1))
            tid = "t-" + secrets.token_hex(6)
        exp = expected_seconds
        if exp is None and kind is not None:
            fallback = int((cfg["task"].get("expected_seconds_seed") or {}).get(kind, 480))
            exp = seed_expected(_load_seed_kind(kind), fallback)
        reg["tasks"][tid] = {
            "id": tid, "workitem": workitem, "command": command,
            "priority": priority, "state": "scheduled" if scheduled_at else "queued",
            "kind": kind, "model": model,
            "executor": executor,  # 2026-08-27 ocr medium：记录执行器（script/local 零成本落账消费）
            "workdir": workdir or os.getcwd(), "expected_seconds": exp,
            "kill_on_timeout": bool(kill_on_timeout),
            "scheduled_at": scheduled_at, "why": why, "cost_usd": None,
            "audit": {"force_reason": force_reason or why or "--force"} if force else None,
            "created_at": _now_ms(), "started_at": None, "finished_at": None,
            "exit_code": None, "failure_tail": None, "pid": None, "heartbeat_at": None,
        }
        save_registry_atomic(reg)
        # queued/scheduled 事件在注册表锁内写:保证先于任何并发 dispatch/pump 的事件落盘(事件流不倒挂)
        _emit_task_event_best_effort(tid, {
            "schema_version": EVENT_SCHEMA_VERSION, "ts": now_iso(),
            "task_id": tid, "workitem": workitem, "kind": kind,
            "state": reg["tasks"][tid]["state"], "exit_code": None, "duration_s": None,
            "expected_seconds": exp, "started_at": None, "finished_at": None,
            "diagnostic": None, "partial_complete": False,
        })
        return tid, reg["tasks"][tid]

    tid, _entry = with_registry_flock(_do)
    if not scheduled_at:  # 仅 queued 走既有 auto-dispatch;scheduled 交给 pump(§1.5(2))
        try:
            dispatch(cfg)  # best-effort 自动出队;失败不回溯,任务留在 queued,run 可补
        except (StoreError, OSError):
            pass
    return tid


# ---------------------------------------------------------------------------
# 落账 runner(§1.4(3):分离式 wrapper,命令结束 finally 自动状态流转)
# ---------------------------------------------------------------------------

def classify_event_state(rc, timed_out, partial_complete=False):
    """纯函数:命令结束 → 事件终态;timeout 且 partial → "partial-complete"。"""
    st, _ = terminal_state(rc, timed_out)
    return "partial-complete" if (st == "timeout" and partial_complete) else st


def _executor_result_path(t):
    """execute 任务对应的 <FLOW_DATA_DIR>/workitems/<wi>/executor/result.json。"""
    data_dir = os.environ.get("FLOW_DATA_DIR") or os.path.join(os.getcwd(), ".flow")
    return os.path.join(data_dir, "workitems", t["workitem"], "executor", "result.json")


def _is_partial_complete(t, rc):
    """I/O:execute 且 rc==124 时读 result.json(status=partial-complete/partial_complete)。
    缺失/损坏 → False(降级 timeout,安全侧,§E14)。"""
    if rc != 124 or t.get("kind") != "execute" or not t.get("workitem"):
        return False
    p = _executor_result_path(t)
    if not os.path.isfile(p):
        return False
    try:
        r = json.loads(_fc.read_file(p))
    except (ValueError, OSError):
        return False
    return isinstance(r, dict) and (
        r.get("status") == "partial-complete" or r.get("partial_complete") is True)


def _diagnostic_for(event_state, t, tail):
    """终态诊断串(均须已 redact):done → null;partial-complete → result.json redacted_logs;
    failed/timeout → failure_tail(tail)。"""
    if event_state == "done":
        return None
    if event_state == "partial-complete" and t.get("workitem"):
        try:
            r = json.loads(_fc.read_file(_executor_result_path(t)))
        except (ValueError, OSError):
            return tail
        if isinstance(r, dict) and r.get("redacted_logs"):
            return r["redacted_logs"]
    return tail


def _wallclock_seconds(started_at, finished_at):
    """墙钟耗时(秒);任一时间缺失/非法 → None。"""
    if not started_at or not finished_at:
        return None
    try:
        delta = datetime.fromisoformat(finished_at) - datetime.fromisoformat(started_at)
        return max(0, int(delta.total_seconds()))
    except (TypeError, ValueError):
        return None


def _heartbeat_loop(task_id, stop_event, interval=30):
    """运行中心跳(§1.1):每 interval 秒用 flock 更新当前任务 heartbeat_at。

    daemon 后台线程:写 registry 失败(锁冲突等)静默重试,不影响主流程;
    stop_event.wait(interval) 同时承担节拍与停止等待(被 set 立即退出)。
    """
    while not stop_event.wait(interval):
        try:
            def _beat():
                reg = load_registry()
                t = reg["tasks"].get(task_id)
                if t is None or t["state"] != "running":
                    return  # 已终态/已删 → 不再写
                t["heartbeat_at"] = _now_ms()
                save_registry_atomic(reg)
            with_registry_flock(_beat)
        except (StoreError, OSError):
            pass  # 静默重试,不影响主流程
        except Exception:
            pass  # 兜底:任何未预期异常不杀 daemon 线程,下个节拍重试


def _runner(task_id, cfg, heartbeat_interval=30):
    """单任务落账(内部子命令,隐藏不入 help)。命令结束后 finally 写终态,再 dispatch 自续。

    heartbeat_interval 可注入(测试用小间隔验证 heartbeat_at 前进;生产默认 30s)。
    """
    log_path_ = log_path(task_id)

    def _boot():
        reg = load_registry()
        t = reg["tasks"].get(task_id)
        if t is None or t["state"] != "running":
            return None  # 已被 reconcile 或已终态 → no-op(幂等,§R10/E22)
        t["pid"] = os.getpid()
        t["heartbeat_at"] = _now_ms()
        save_registry_atomic(reg)
        return t

    t = with_registry_flock(_boot)
    if t is None:
        return 0
    print(f"=== {task_id} start pid={os.getpid()} ===", flush=True)
    stop_event = threading.Event()
    hb_thread = threading.Thread(
        target=_heartbeat_loop, args=(task_id, stop_event),
        kwargs={"interval": heartbeat_interval}, daemon=True,
        name=f"flow-heartbeat-{task_id}")
    hb_thread.start()
    rc, timed_out = None, False
    try:
        rc = run_command(t, cfg)
    except CommandTimeout:
        rc, timed_out = 124, True
    except OSError:
        rc, timed_out = 127, False  # 命令无法启动 → failed
    finally:
        stop_event.set()            # 停心跳线程(§1.1)
        hb_thread.join(timeout=5)   # daemon 线程:join 超时不等,随进程退出
        state, _reason = terminal_state(rc, timed_out)
        tail = extract_tail(log_path_, int(cfg["task"].get("log_tail_bytes", 2000))) \
            if state != "done" else None
        finished_at = _now_ms()

        def _settle():
            nonlocal finished_at
            reg = load_registry()
            t2 = reg["tasks"].get(task_id)
            if t2 is not None and t2["state"] == "running":  # 二次守卫:落账幂等
                t2["state"] = state
                t2["exit_code"] = rc
                t2["finished_at"] = finished_at
                t2["failure_tail"] = tail
                save_registry_atomic(reg)

        with_registry_flock(_settle)
        print(f"=== {task_id} end state={state} exit={rc} ===", flush=True)
        event_record = None  # 终态事件记录(构造失败时通知一并跳过,保持原 fail-safe 语义)
        # ── M2:注册表终态提交后,锁外 best-effort(事件/种子,注册表是唯一权威) ──
        try:
            partial = _is_partial_complete(t, rc)
            event_state = classify_event_state(rc, timed_out, partial)
            dur = _wallclock_seconds(t.get("started_at"), finished_at)
            event_record = {
                "schema_version": EVENT_SCHEMA_VERSION, "ts": now_iso(),
                "task_id": task_id, "workitem": t.get("workitem"), "kind": t.get("kind"),
                "state": event_state, "exit_code": rc, "duration_s": dur,
                "expected_seconds": t.get("expected_seconds"),
                "started_at": t.get("started_at"), "finished_at": finished_at,
                "diagnostic": _diagnostic_for(event_state, t, tail),
                "partial_complete": partial,
            }
            _record_duration_best_effort(
                t.get("kind"), dur, int(cfg["task"].get("seed_history_len", 8)))
            _emit_task_event_best_effort(task_id, event_record)
        except (StoreError, OSError) as e:
            print(f"告警: 终态事件/种子处理失败 {task_id}: {e}", file=sys.stderr)
        # flow-cost-ledger §1.5(4):终态提交后锁外 best-effort 成本落账(失败留 null,不阻断流转)
        _settle_cost(task_id, t, finished_at)
        # ── W-B §1.3:任务终态钩子(best-effort,不改任务终态;异常仅告警不阻断 dispatch) ──
        try:
            run_terminal_hooks(t, cfg, state)
        except Exception as e:  # noqa: BLE001
            print(f"告警: 终态钩子失败 {task_id}: {e}", file=sys.stderr)
        # ── W-S2:execute timeout/failed 自动抢救 + token 峰值预警(best-effort,同 W-B 纪律) ──
        rescue_action = "skipped"
        terminal = None
        try:
            if event_record is not None and t.get("kind") == "execute" \
                    and event_record.get("state") in ("failed", "timeout", "partial-complete"):
                # ocr F6:失败上下文随 rescue 通知携带(单通知原则下不再重复发 execute_failure)
                terminal = {"state": event_record.get("state"),
                            "exit_code": event_record.get("exit_code"),
                            "failure_tail": event_record.get("diagnostic")}
            rescue_action = run_rescue_hook(t, cfg, state, rc, terminal=terminal)
        except Exception as e:  # noqa: BLE001
            print(f"告警: 抢救钩子失败 {task_id}: {e}", file=sys.stderr)
        try:
            run_token_warning(t, cfg, finished_at)
        except Exception as e:  # noqa: BLE001
            print(f"告警: token 预警失败 {task_id}: {e}", file=sys.stderr)
        # ── M2 §1.3 终态通知(ocr F6 单通知):execute 失败/超时终态且 rescue 已发通知
        # (rescued/verify_fail/verify_error/write_fail/frozen)→ 失败上下文已并入 rescue
        # 通知 terminal_failure,不再重复发 execute_failure;其余终态照旧 ──
        try:
            if event_record is not None and not (
                    t.get("kind") == "execute"
                    and event_record.get("state") in ("failed", "timeout", "partial-complete")
                    and rescue_action in RESCUE_NOTIFY_ACTIONS):
                _notify_terminal(cfg, t, event_record)
        except Exception as e:  # noqa: BLE001
            print(f"告警: 终态通知失败 {task_id}: {e}", file=sys.stderr)
    try:
        dispatch(cfg)  # 释放槽位 → 触发下一批(自续队列)
    except (StoreError, OSError):
        pass
    return 0


# ---------------------------------------------------------------------------
# cost 摘取(§1.5(4):best-effort,价目表不硬编码;摘不到 → null,不阻断流转)
# ---------------------------------------------------------------------------

def _data_dir():
    return os.environ.get("FLOW_DATA_DIR") or os.path.join(os.getcwd(), ".flow")


def _reasonix_runs_dir():
    """reasonix 运行日志目录(~/.reasonix/runs;测试经 HOME 隔离,不触真实目录)。"""
    return os.path.join(os.path.expanduser("~"), ".reasonix", "runs")


def extract_reasonix_cost(executor_dir, started_at, finished_at):
    """reasonix 成本(§D6):1) executor/result.json.cost 优先;2) 退回 ~/.reasonix/runs/*.log
    按 started_at±120s 就近匹配 + 累加所有 $[0-9]+.[0-9]+ 金额。失败/无匹配 → None。"""
    rp = os.path.join(executor_dir, "result.json")
    if os.path.isfile(rp):
        try:
            r = json.loads(_fc.read_file(rp))
        except (ValueError, OSError):
            r = None
        if isinstance(r, dict):
            c = r.get("cost")
            if isinstance(c, (int, float)) and not isinstance(c, bool):
                return float(c)
    if not started_at:
        return None
    try:
        st = datetime.fromisoformat(started_at).astimezone(TZ_CN)
    except (TypeError, ValueError):
        return None
    runs_dir = _reasonix_runs_dir()
    if not os.path.isdir(runs_dir):
        return None
    matches = []
    try:
        names = os.listdir(runs_dir)
    except OSError:
        return None
    for name in names:
        m = re.match(r"^(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2})(\d{2})", name)
        if not m:
            continue
        p = os.path.join(runs_dir, name)
        if not os.path.isfile(p):  # 同名目录(非日志)不参与匹配
            continue
        y, mo, d, h, mi, s = (int(x) for x in m.groups())
        try:
            dt = datetime(y, mo, d, h, mi, s, tzinfo=TZ_CN)
        except ValueError:
            continue
        delta = abs((dt - st).total_seconds())
        if delta <= 120:
            matches.append((delta, p))
    if not matches:
        return None
    matches.sort(key=lambda x: x[0])  # 就近唯一化(多个候选取 started_at 最近者)
    try:
        text = _fc.read_file(matches[0][1])
    except OSError:
        return None
    total = 0.0
    found = False
    for m in re.finditer(r"\$([0-9]+\.[0-9]+)", text):
        total += float(m.group(1))
        found = True
    return total if found else None


def extract_dsh_cost(wi_dir):
    """dsh 成本(§D6):designs/.dsh-design/manifest.jsonl 找 cost_usd 字段;
    现无 dollar 字段 → None(绝不擅自乘价目表,test_C4 锁死)。"""
    if not wi_dir:
        return None
    manifest = os.path.join(os.path.dirname(os.path.dirname(wi_dir)),
                            "designs", ".dsh-design", "manifest.jsonl")
    if not os.path.isfile(manifest):
        return None
    try:
        text = _fc.read_file(manifest)
    except OSError:
        return None
    for line in text.splitlines():
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if isinstance(rec, dict):
            c = rec.get("cost_usd")
            if isinstance(c, (int, float)) and not isinstance(c, bool):
                return float(c)
    return None


def extract_cost_usd(t, started_at, finished_at):
    """按 kind 摘取成本(§1.5(4)):execute → reasonix 主路径;design → dsh 后补;
    其余 kind(已不入队,仅防御)无独立 dollar 产物 → None。"""
    kind = t.get("kind")
    wi = t.get("workitem")
    if kind == "execute" and wi:
        executor_dir = os.path.join(_data_dir(), "workitems", wi, "executor")
        return extract_reasonix_cost(executor_dir, started_at, finished_at)
    if kind == "design" and wi:
        wi_dir = os.path.join(_data_dir(), "workitems", wi)
        return extract_dsh_cost(wi_dir)
    return None


def _settle_cost(task_id, t, finished_at):
    """终态后锁外 best-effort 回写 cost_usd(§1.5(4)):摘不到 → 保持 null,写失败静默。"""
    try:
        # 2026-08-27：script/local 执行器零 LLM 成本 → 明确落账 0（flowq 显示 💰0）
        if t.get("executor") in ("script", "local"):
            cost = 0.0
        else:
            cost = extract_cost_usd(t, t.get("started_at"), finished_at)
    except (StoreError, OSError):
        return
    if cost is None:
        return  # 摘不到/不适用 → 保持 null,不阻断流转(E21)

    def _do():
        reg = load_registry()
        t2 = reg["tasks"].get(task_id)
        if t2 is not None and t2.get("cost_usd") is None:
            t2["cost_usd"] = cost
            save_registry_atomic(reg)  # 写盘前 DENY_RE 兜底(既有)

    try:
        with_registry_flock(_do)
    except (StoreError, OSError):
        pass


def runner_main(argv):
    """_runner <id> 子命令入口(spawn 直接调用,不经过 task 分发)。"""
    if len(argv) != 1 or not TASK_ID_RE.fullmatch(argv[0]):
        return 2
    try:
        cfg = load_task_config()
    except (StoreError, OSError) as e:
        return fail(1, str(e), None, False)
    return _runner(argv[0], cfg)


# ---------------------------------------------------------------------------
# CLI 命令(§1.5)
# ---------------------------------------------------------------------------

def cmd_add(args, cfg):
    pos, opts = scan_args(
        args, {"command", "workitem", "priority", "expected-seconds", "kind", "workdir",
               "at", "why", "force-reason", "model"})
    json_mode = bool(opts.get("json"))
    if pos:
        return fail(2, "add 不接受位置参数", None, json_mode)
    command = opts.get("command")
    if command is None or command.strip() == "":
        return fail(2, "missing command", "add 需要 --command CMD", json_mode)
    workitem = opts.get("workitem")
    workdir = opts.get("workdir")  # 可选;默认 os.getcwd()(add_task 内),不校验目录存在(§3 错误表 #2)
    priority = opts.get("priority")
    kind = opts.get("kind")
    # cost-opt-cache-dispatch §1.3:显式 --model 优先;缺省由 add_task 内推导(此处预计算供 JSON emit)
    model = opts.get("model") or _default_model_for(command, kind, cfg)
    # ── 门禁校验链(fail-closed,先于一切写盘;任一不过 → 拒绝:<文案>,exit 2) ──
    try:
        gate_validate(opts, cfg)
    except GateReject as e:
        return fail(2, e.args[0], f"拒绝：{e.args[1]}", json_mode)
    except StoreError as e:
        return fail(2, str(e), None, json_mode)
    expected_raw = opts.get("expected-seconds")
    try:
        expected = int(expected_raw)
    except (TypeError, ValueError):
        return fail(2, "invalid expected-seconds", expected_raw, json_mode)
    # gate_validate 第 6 步已强制 expected 为正整数,此处仅 int 转换(gate 后必成功)
    # ── --at 解析(scheduled 分支) ──
    scheduled = None
    if opts.get("at"):
        try:
            scheduled = parse_scheduled_at(opts["at"])
        except UsageError as e:
            return fail(2, "invalid scheduled-at", f"拒绝：{e}", json_mode)
    try:
        tid = add_task(cfg, command, workitem=workitem, priority=priority,
                       expected_seconds=expected, kind=kind, model=model,
                       kill_on_timeout=bool(opts.get("kill-on-timeout")),
                       workdir=workdir, why=opts.get("why"),
                       scheduled_at=scheduled, force=bool(opts.get("force")),
                       force_reason=opts.get("force-reason"))
    except DuplicateWorkitem as e:
        return fail(2, "duplicate_workitem", f"已有非终态任务 {e.args[0]} ({e.args[1]})", json_mode)
    except QueueFull as e:
        cap = e.args[0] if e.args else 50
        return fail(2, "queue_full", f"队列非终态已达上限 {cap},请先清理/完成后重试", json_mode)
    except UsageError as e:
        return fail(2, str(e), None, json_mode)
    except StoreError as e:
        return fail(2, str(e), None, json_mode)
    state = "scheduled" if scheduled else "queued"
    # ── suggest_wake:dt = scheduled or now(复用 window.window_suggest,D4 窗口仅建议) ──
    try:
        sug_dt = datetime.fromisoformat(scheduled) if scheduled else datetime.now(timezone.utc)
        sug = window.window_suggest({"priority": priority, "expected_seconds": expected}, sug_dt)
    except (TypeError, ValueError):
        sug = None
    if json_mode:
        emit({"status": "ok", "id": tid, "state": state,  # 入队确认(§1.5 契约)
              "workitem": workitem, "priority": priority, "kind": kind,
              "model": model, "workdir": workdir or os.getcwd(),
              "expected_seconds": expected, "scheduled_at": scheduled,
              "why": opts.get("why"), "suggest_wake": sug})
    else:
        print(f"task {tid}: {state}"
              + (f" scheduled_at={scheduled}" if scheduled else "")
              + (f" suggest_wake={sug}" if sug else ""))
    return 0


def cmd_status(args, cfg):
    pos, opts = scan_args(args, set())
    json_mode = bool(opts.get("json"))
    if len(pos) != 1:
        return fail(2, "status 需要 <id>", None, json_mode)
    tid = pos[0]
    if not TASK_ID_RE.fullmatch(tid):
        return fail(2, "invalid task id", tid, json_mode)
    try:
        reg = load_registry()
    except StoreError as e:
        return fail(2, str(e), None, json_mode)
    t = reg["tasks"].get(tid)
    if t is None:
        return fail(2, "task not found", tid, json_mode)
    if json_mode:
        emit({"status": "ok", "task": t})
    else:
        print(f"{tid} state={t['state']} priority={t['priority']} "
              f"workitem={t.get('workitem') or '-'} exit_code={t.get('exit_code') if t.get('exit_code') is not None else '-'} "
              f"created_at={t.get('created_at') or '-'} finished_at={t.get('finished_at') or '-'}")
        if t.get("failure_tail"):
            print(f"failure_tail: {t['failure_tail']}")
    return 0


def cmd_list(args, cfg):
    pos, opts = scan_args(args, {"state", "workitem"})
    json_mode = bool(opts.get("json"))
    if pos:
        return fail(2, "list 不接受位置参数", None, json_mode)
    state_f = opts.get("state")
    wi_f = opts.get("workitem")
    if state_f is not None and state_f not in TASK_STATES:
        return fail(2, "invalid state filter", state_f, json_mode)
    try:
        reg = load_registry()
    except StoreError as e:
        return fail(2, str(e), None, json_mode)
    tasks = sorted(reg["tasks"].values(),
                   key=lambda t: (t.get("created_at", ""), t.get("id", "")), reverse=True)
    if state_f is not None:
        tasks = [t for t in tasks if t["state"] == state_f]
    if wi_f is not None:
        tasks = [t for t in tasks if t.get("workitem") == wi_f]
    if json_mode:
        emit({"status": "ok", "count": len(tasks), "tasks": tasks})
    else:
        for t in tasks:
            print(f"{t['id']} {t['state']:8s} {t.get('workitem') or '-':4s} "
                  f"{t['priority']} exit={t.get('exit_code') if t.get('exit_code') is not None else '-'}")
    return 0


def cmd_log(args, cfg):
    pos, opts = scan_args(args, {"tail"})
    json_mode = bool(opts.get("json"))
    if len(pos) != 1:
        return fail(2, "log 需要 <id>", None, json_mode)
    tid = pos[0]
    if not TASK_ID_RE.fullmatch(tid):
        return fail(2, "invalid task id", tid, json_mode)
    try:
        reg = load_registry()
    except StoreError as e:
        return fail(2, str(e), None, json_mode)
    if tid not in reg["tasks"]:
        return fail(2, "task not found", tid, json_mode)
    tail_raw = opts.get("tail")
    tail = None
    if tail_raw is not None:
        try:
            tail = int(tail_raw)
        except (TypeError, ValueError):
            return fail(2, "invalid tail", tail_raw, json_mode)
        if tail < 0:
            return fail(2, "invalid tail", "必须 >= 0", json_mode)
    text = read_log(log_path(tid), tail)
    if json_mode:
        emit({"status": "ok", "id": tid, "content": text})
    else:
        sys.stdout.write(text)
        if text and not text.endswith("\n"):
            sys.stdout.write("\n")
    return 0


def cmd_run(args, cfg):
    pos, opts = scan_args(args, {"max-parallel"})
    json_mode = bool(opts.get("json"))
    if pos:
        # flow-cost-ledger §1.7:位置参数 <id> → 单任务强制触发(scheduled/queued→running,绕过窗口)
        if len(pos) > 1:
            return fail(2, "run 位置参数过多", None, json_mode)
        tid = pos[0]
        if not TASK_ID_RE.fullmatch(tid):
            return fail(2, "invalid task id", tid, json_mode)

        def _do():
            reg = load_registry()
            t = reg["tasks"].get(tid)
            if t is None:
                raise TaskError(tid)
            if t["state"] not in ("queued", "scheduled"):
                raise UsageError(f"仅 queued/scheduled 可强制触发,当前 state={t['state']}")
            capture_and_write_snapshot(t)  # W-V1:先拍快照(execute),后写 registry running(E12)
            t["state"] = "running"
            t["started_at"] = _now_ms()
            t["pid"] = None  # runner 启动后自填(§1.4(2))
            save_registry_atomic(reg)
            return dict(t)

        try:
            t = with_registry_flock(_do)
        except TaskError as e:
            return fail(2, "task not found", e.args[0], json_mode)
        except UsageError as e:
            return fail(2, str(e), None, json_mode)
        except StoreError as e:
            return fail(2, str(e), None, json_mode)
        except OSError as e:
            return fail(1, f"写盘失败: {e}", None, json_mode)
        _emit_task_event_best_effort(tid, {
            "schema_version": EVENT_SCHEMA_VERSION, "ts": now_iso(),
            "task_id": tid, "workitem": t.get("workitem"), "kind": t.get("kind"),
            "state": "running", "exit_code": None, "duration_s": None,
            "expected_seconds": t.get("expected_seconds"),
            "started_at": t.get("started_at"), "finished_at": None,
            "diagnostic": None, "partial_complete": False,
        })
        spawn_runner(tid)
        if json_mode:
            emit({"status": "ok", "promoted": [tid]})
        else:
            print(f"promoted: 1 ({tid})")
        return 0
    mp_raw = opts.get("max-parallel")
    mp = None
    if mp_raw is not None:
        try:
            mp = int(mp_raw)
        except (TypeError, ValueError):
            return fail(2, "invalid max-parallel", mp_raw, json_mode)
        if mp <= 0:
            return fail(2, "invalid max-parallel", "必须为正整数", json_mode)
    try:
        promoted = dispatch(cfg, max_parallel=mp)
    except StoreError as e:
        return fail(2, str(e), None, json_mode)
    except OSError as e:
        return fail(1, f"写盘失败: {e}", None, json_mode)
    if json_mode:
        emit({"status": "ok", "promoted": promoted})
    else:
        print(f"promoted: {len(promoted)}")
    return 0


_NO_FCNTL_LOCK = -1  # 非 POSIX 降级哨兵(无 flock → 无锁直执行,非「被占」)


def _pump_lock_acquire():
    """pump 非阻塞 flock(pump.json.lock):已被占 → None(幂等跳过,E14)。
    非 POSIX(无 fcntl)降级为恒获锁(返回哨兵 -1,不阻塞 pump)。"""
    if fcntl is None or not hasattr(fcntl, "flock"):
        return _NO_FCNTL_LOCK
    os.makedirs(task_dir(), exist_ok=True)
    fd = os.open(pump_lock_path(), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        return None
    return fd


def _pump_lock_release(fd):
    if fd is None or fd == _NO_FCNTL_LOCK:
        return
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def pump_path():
    """pump 心跳文件(独立于任务账本,D5:不污染 tasks.json)。"""
    return os.path.join(task_dir(), "pump.json")


def pump_lock_path():
    return pump_path() + ".lock"


def _pump_heartbeat(phase, promoted=None, last_error=None):
    """写 pump.json 心跳(§1.5(2)/D5);best-effort:失败仅告警,不阻断提升(E15)。
    写盘前过 DENY_RE(红线:落盘 JSON 兜底)。"""
    try:
        rec = {"schema_version": 1, "heartbeat_at": now_iso(), "last_run_at": now_iso(),
               "phase": phase, "promoted": promoted or [], "pid": os.getpid(),
               "last_error": last_error}
        os.makedirs(task_dir(), exist_ok=True)
        text = json.dumps(rec, ensure_ascii=False, indent=2) + "\n"
        if DENY_RE.search(text):
            raise StoreError("pump.json 含疑似 secret, 拒绝写入")
        tmp = f"{pump_path()}.tmp.{os.getpid()}.{secrets.token_hex(4)}"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, pump_path())
    except (StoreError, OSError) as e:
        print(f"告警: pump 心跳写入失败: {e}", file=sys.stderr)


def cmd_pump(args, cfg):
    """pump(§1.5(2)):flock 防并发(被占 → 直接返回幂等)→ 心跳 start → reconcile →
    plan_pump(到期 scheduled,受 max_parallel 空槽)→ scheduled→running → 锁外 spawn → 心跳 end。
    P0 立即、无窗口硬门控(D4)。"""
    pos, opts = scan_args(args, set())
    json_mode = bool(opts.get("json"))
    if pos:
        return fail(2, "pump 不接受位置参数", None, json_mode)
    m = int(cfg["task"].get("max_parallel") or 2)
    lock_fd = _pump_lock_acquire()
    if lock_fd is None:
        return 0  # 并发 pump 已持锁 → 跳过本轮(幂等,E14)
    promoted = []
    try:
        _pump_heartbeat("start")
        try:
            def _do():
                reg = load_registry()
                reconcile_running(reg)
                chosen = plan_pump(reg, m, datetime.now(timezone.utc), cfg)
                now = _now_ms()
                for t in chosen:
                    capture_and_write_snapshot(t)  # W-V1:先拍快照(execute),后写 registry running(E12)
                    t["state"] = "running"
                    t["started_at"] = now
                    t["pid"] = None
                save_registry_atomic(reg)
                return [(t["id"], dict(t)) for t in chosen]

            promoted = with_registry_flock(_do)
        except (StoreError, OSError) as e:
            _pump_heartbeat("end", last_error=str(e))
            raise
        for tid, t in promoted:  # 锁外 spawn(Popen 立即返回,同 dispatch)
            _emit_task_event_best_effort(tid, {
                "schema_version": EVENT_SCHEMA_VERSION, "ts": now_iso(),
                "task_id": tid, "workitem": t.get("workitem"), "kind": t.get("kind"),
                "state": "running", "exit_code": None, "duration_s": None,
                "expected_seconds": t.get("expected_seconds"),
                "started_at": t.get("started_at"), "finished_at": None,
                "diagnostic": None, "partial_complete": False,
            })
            spawn_runner(tid)
        _pump_heartbeat("end", promoted=[tid for tid, _ in promoted])
    except StoreError as e:
        return fail(2, str(e), None, json_mode)
    except OSError as e:
        return fail(1, f"写盘失败: {e}", None, json_mode)
    finally:
        _pump_lock_release(lock_fd)
    # ── W-B §1.4:超时收口 stale gate(锁已释放;自包含 best-effort,不影响 pump 退出码) ──
    try:
        run_stale_gate(cfg)
    except Exception as e:  # noqa: BLE001
        print(f"告警: stale gate 失败: {e}", file=sys.stderr)
    if json_mode:
        emit({"status": "ok", "promoted": [tid for tid, _ in promoted]})
    else:
        print(f"promoted: {len(promoted)}")
    return 0


def cmd_reschedule(args, cfg):
    """改期(§1.5(3)):仅 scheduled 可改期;naive/过去/非法 --at → 拒绝(E12/E13/E20)。"""
    pos, opts = scan_args(args, {"at"})
    json_mode = bool(opts.get("json"))
    if len(pos) != 1:
        return fail(2, "reschedule 需要 <id>", None, json_mode)
    tid = pos[0]
    if not TASK_ID_RE.fullmatch(tid):
        return fail(2, "invalid task id", tid, json_mode)
    at_raw = opts.get("at")
    if not at_raw:
        return fail(2, "reschedule 需要 --at", None, json_mode)
    try:
        new_at = parse_scheduled_at(at_raw)
    except UsageError as e:
        return fail(2, "invalid scheduled-at", f"拒绝：{e}", json_mode)

    def _do():
        reg = load_registry()
        t = reg["tasks"].get(tid)
        if t is None:
            raise TaskError(tid)
        if t["state"] != "scheduled":
            raise UsageError(f"仅 scheduled 可改期,当前 state={t['state']}")
        t["scheduled_at"] = new_at
        save_registry_atomic(reg)
        return dict(t)

    try:
        with_registry_flock(_do)
    except TaskError as e:
        return fail(2, "task not found", e.args[0], json_mode)
    except UsageError as e:
        return fail(2, str(e), None, json_mode)
    except StoreError as e:
        return fail(2, str(e), None, json_mode)
    if json_mode:
        emit({"status": "ok", "id": tid, "scheduled_at": new_at})
    else:
        print(f"task {tid}: scheduled_at={new_at}")
    return 0


def cmd_reconcile(args, cfg):
    pos, opts = scan_args(args, set())
    json_mode = bool(opts.get("json"))
    if pos:
        return fail(2, "reconcile 不接受位置参数", None, json_mode)

    def _do():
        reg = load_registry()
        reaped = reconcile_running(reg)
        save_registry_atomic(reg)
        return reaped

    try:
        reaped = with_registry_flock(_do)
    except StoreError as e:
        return fail(2, str(e), None, json_mode)
    except OSError as e:
        return fail(1, f"写盘失败: {e}", None, json_mode)
    if json_mode:
        emit({"status": "ok", "reaped": reaped})
    else:
        print(f"reaped: {len(reaped)}")
    return 0


def _prune_match(reg, states=None, older_than_days=None, now=None):
    """纯函数:返回应删任务 id 列表(不改 reg);running/queued 绝不删(§1.3)。

    states=None → 默认全部终态;older_than_days 按 finished_at 只删 N 天前的;
    终态但 finished_at 缺失/非法 → 保守不删。无匹配 → 空列表(幂等)。
    """
    states = tuple(states) if states is not None else TERMINAL_STATES
    now = now or datetime.now(timezone.utc)
    out = []
    for tid, t in reg["tasks"].items():
        st = t.get("state")
        if st in NON_TERMINAL:  # 红线:非终态(含 scheduled)绝不删(双保险)
            continue
        if st not in states:
            continue
        if older_than_days is not None:
            finished = t.get("finished_at")
            if not finished:
                continue
            try:
                finished_dt = datetime.fromisoformat(finished)
                age_s = (now - finished_dt).total_seconds()
            except (TypeError, ValueError):
                continue  # 时间戳缺失/非法/naive(无时区) → 保守不删
            if age_s < older_than_days * 86400:
                continue
        out.append(tid)
    return out


def prune_tasks(reg, states=None, older_than_days=None, now=None):
    """删除终态任务;返回被删 id 列表(§1.3)。连带删 logs/<tid>.log 与快照(§5.3/§2.8,幂等)。"""
    removed = _prune_match(reg, states, older_than_days, now)
    for tid in removed:
        del reg["tasks"][tid]
        _remove_log_best_effort(tid)
        _remove_snapshot_best_effort(tid)  # W-V1 §2.8:快照随任务记录清理
    return removed


def _remove_log_best_effort(tid):
    """删 logs/<tid>.log;不存在/失败静默(§5.3,幂等)。"""
    try:
        os.remove(log_path(tid))
    except OSError:
        pass


def auto_prune(reg, cfg, now=None):
    """写时自动清理(§5.1):done 保留 prune_done_days,failed/timeout/killed 保留 prune_failed_days;
    仍超 task_cap → 按 finished_at 升序删最老终态;running/queued 永不删。
    有副作用:删 registry 条目 + best-effort 连带删 logs/<id>.log 与快照;返回被删 id 列表。
    """
    task_cfg = (cfg or {}).get("task") or {}
    done_days = int(task_cfg.get("prune_done_days") or 3)
    failed_days = int(task_cfg.get("prune_failed_days") or 7)
    cap = int(task_cfg.get("task_cap") or 100)
    now = now or datetime.now(timezone.utc)
    removed = []

    def _match(states, older):
        return _prune_match(reg, states=states, older_than_days=older, now=now)

    removed += _match(("done",), done_days)
    removed += _match(("failed", "timeout", "killed"), failed_days)
    for tid in set(removed):
        del reg["tasks"][tid]
        _remove_log_best_effort(tid)
        _remove_snapshot_best_effort(tid)  # W-V1 §2.8:快照随任务记录清理
    removed = list(dict.fromkeys(removed))
    # 容量上限:仍超 → 删最老终态(finished_at 升序),双保险保 running/queued
    terminal = [(t.get("finished_at") or "", tid) for tid, t in reg["tasks"].items()
                if t.get("state") in ("done", "failed", "timeout", "killed")]
    over = len(reg["tasks"]) - cap
    if over > 0 and terminal:
        terminal.sort()
        for _, tid in terminal[:over]:
            if tid not in removed:
                removed.append(tid)
            del reg["tasks"][tid]
            _remove_log_best_effort(tid)
            _remove_snapshot_best_effort(tid)  # W-V1 §2.8:快照随任务记录清理
    return removed


def mark_stale_running(reg, cfg, now=None):
    """写时僵尸标记(§5.2):running 心跳 heartbeat_at 静默 > stale_after_s → 标 timeout。
    有副作用:改 registry 条目 state(保留条目与诊断,不删);返回被标 id 列表。
    心跳缺失 → 保守不动(交 reconcile)。
    """
    task_cfg = (cfg or {}).get("task") or {}
    stale_after_s = int(task_cfg.get("stale_after_s") or 300)
    now = now or datetime.now(timezone.utc)
    out = []
    for tid, t in reg["tasks"].items():
        if t.get("state") != "running":
            continue
        hb = t.get("heartbeat_at")
        if not hb:
            continue  # 无心跳记录(旧任务/未知) → 保守不动
        try:
            hb_dt = datetime.fromisoformat(hb)
            age_s = (now - hb_dt).total_seconds()
        except (TypeError, ValueError):
            continue  # 心跳非法 → 保守不动
        if age_s <= stale_after_s:
            continue
        t["state"] = "timeout"
        t["finished_at"] = now.isoformat()
        t["failure_tail"] = f"stale: heartbeat 静默 {int(age_s)}s > {stale_after_s}s"
        out.append(tid)
    return out


def cmd_prune(args, cfg):
    """终态任务清理(§1.3):--state/--older-than/--force;非 tty 自动 --force;无匹配幂等 exit 0。

    注册表条目 + logs/<id>.log 连带删除(§5.3)。
    """
    pos, opts = scan_args(args, {"state", "older-than"})  # force 为纯 flag,不入 value_opts
    json_mode = bool(opts.get("json"))
    if pos:
        return fail(2, "prune 不接受位置参数", None, json_mode)
    states = None
    state_raw = opts.get("state")
    if state_raw is not None:
        states = tuple(s.strip() for s in state_raw.split(","))
        for s in states:
            if s not in PRUNE_STATES:
                return fail(2, "invalid state", s, json_mode)
    older = None
    older_raw = opts.get("older-than")
    if older_raw is not None:
        try:
            older = int(older_raw)
        except (TypeError, ValueError):
            return fail(2, "invalid older-than", older_raw, json_mode)
        if older < 0:
            return fail(2, "invalid older-than", "必须 >= 0", json_mode)
    force = bool(opts.get("force"))
    if not force and not sys.stdin.isatty():
        force = True  # 非 tty 自动 --force(§1.3)
    if not force:  # tty 交互确认
        try:
            reg0 = load_registry()
        except StoreError as e:
            return fail(2, str(e), None, json_mode)
        n = len(_prune_match(reg0, states, older))
        if n == 0:
            if json_mode:
                emit({"status": "ok", "pruned": []})
            else:
                print("无匹配")
            return 0
        answer = input(f"确认删除 {n} 个任务? [y/N] ").strip().lower()
        if answer not in ("y", "yes"):
            if json_mode:
                emit({"status": "ok", "pruned": [], "cancelled": True})
            else:
                print("已取消")
            return 0

    def _do():
        reg = load_registry()
        removed = prune_tasks(reg, states, older)
        save_registry_atomic(reg)
        return removed

    try:
        removed = with_registry_flock(_do)
    except StoreError as e:
        return fail(2, str(e), None, json_mode)
    except OSError as e:
        return fail(1, f"写盘失败: {e}", None, json_mode)
    if not removed:
        if json_mode:
            emit({"status": "ok", "pruned": []})
        else:
            print("无匹配")  # 幂等:无匹配 exit 0(§3 错误表 #4)
    else:
        if json_mode:
            emit({"status": "ok", "pruned": removed})
        else:
            print(f"pruned: {len(removed)}")
            for tid in removed:
                print(f"  {tid}")
    return 0


def cmd_snapshot(args, cfg):
    """读取 execute 基线快照(W-V1 §2.7):`flow task snapshot <id> [--json]`。
    文件缺失 → exit2 snapshot_missing;存在但 load_snapshot 返回 None(损坏/形状非法)
    → exit2 snapshot_corrupt;否则输出快照(JSON 全量 / 文本摘要)。"""
    pos, opts = scan_args(args, set())
    json_mode = bool(opts.get("json"))
    if len(pos) != 1:
        return fail(2, "snapshot 需要 <id>", None, json_mode)
    tid = pos[0]
    if not TASK_ID_RE.fullmatch(tid):
        return fail(2, "invalid task id", tid, json_mode)
    snap = load_snapshot(tid)
    if snap is None:
        if not os.path.isfile(snapshot_path(tid)):
            return fail(2, "snapshot_missing", tid, json_mode)   # E5
        return fail(2, "snapshot_corrupt", tid, json_mode)       # E6/E7
    if json_mode:
        emit({"status": "ok", "id": tid, "snapshot": snap})
    else:
        if snap.get("git"):
            print(f"{tid} git=true head={snap['head']} "
                  f"tracked_modified={len(snap['tracked_modified'])} "
                  f"untracked={len(snap['untracked'])} "
                  f"captured_at={snap.get('captured_at') or '-'}")
        else:
            print(f"{tid} git=false error={snap.get('error') or '-'} "
                  f"captured_at={snap.get('captured_at') or '-'}")
    return 0


# ---------------------------------------------------------------------------
# 宿主集成(§1.5:wake 自包含文本 + notify 命令钩子;均 best-effort)
# ---------------------------------------------------------------------------

WAKE_VARS = ("task_id", "workitem", "kind", "state", "exit_code", "elapsed_seconds",
             "expected_seconds", "log_path", "event_path", "next_command")

DEFAULT_WAKE_TEMPLATE = (
    "任务 {task_id}（workitem={workitem} kind={kind}）当前 state={state} exit_code={exit_code}\n"
    "已耗时 {elapsed_seconds}s / 预估 {expected_seconds}s\n"
    "统一日志: {log_path}   事件流: {event_path}\n"
    "下一步: {next_command}\n"
    "失败处理: 见 {log_path} 尾部与事件流 diagnostic；"
    "design 可 design --check，execute 可 execute --sync --force 重跑"
)


def render_wake_text(template, ctx):
    """纯函数:null → 内置默认模板;对 WAKE_VARS 做替换;未知占位符保留字面量。"""
    tpl = template or DEFAULT_WAKE_TEMPLATE
    for k in WAKE_VARS:
        tpl = tpl.replace("{" + k + "}", str(ctx.get(k, "")))
    return tpl


def _wake_ctx(t):
    """由注册表条目构造 wake 渲染上下文(elapsed 由 started/finished 派生,next_command 按 kind)。"""
    tid = t["id"]
    kind = t.get("kind")
    wi = t.get("workitem") or ""
    if kind == "design":
        next_command = f"flow workitem design {wi} --check"
    elif kind == "execute":
        next_command = f"flow workitem status {wi}"
    else:
        next_command = f"flow task status {tid}"
    if t.get("finished_at"):
        elapsed = _wallclock_seconds(t.get("started_at"), t.get("finished_at"))
    else:
        elapsed = _wallclock_seconds(t.get("started_at"), _now_ms())
    return {
        "task_id": tid, "workitem": wi, "kind": kind or "",
        "state": t.get("state"),
        "exit_code": t.get("exit_code") if t.get("exit_code") is not None else "",
        "elapsed_seconds": elapsed if elapsed is not None else "",
        "expected_seconds": (t.get("expected_seconds")
                             if t.get("expected_seconds") is not None else ""),
        "log_path": os.path.abspath(log_path(tid)),
        "event_path": os.path.abspath(event_path(tid)),
        "next_command": next_command,
    }


def cmd_wake_text(args, cfg):
    pos, opts = scan_args(args, set())
    json_mode = bool(opts.get("json"))
    if len(pos) != 1:
        return fail(2, "wake-text 需要 <id>", None, json_mode)
    tid = pos[0]
    if not TASK_ID_RE.fullmatch(tid):
        return fail(2, "invalid task id", tid, json_mode)
    try:
        reg = load_registry()
    except StoreError as e:
        return fail(2, str(e), None, json_mode)
    t = reg["tasks"].get(tid)
    if t is None:
        return fail(2, "task not found", tid, json_mode)
    host = cfg.get("host") or {}
    text = render_wake_text(host.get("wake_template"), _wake_ctx(t))
    if json_mode:
        emit({"status": "ok", "id": tid, "wake_text": text})
    else:
        print(text)
    return 0


def _notify_if_configured(cfg, event_record):
    """终态通知钩子(§1.5):委托 _pipe_notify(复用管道逻辑)。"""
    _pipe_notify(cfg, event_record)


# ---------------------------------------------------------------------------
# W-B:任务终态钩子 + 超时收口(stale gate)
# (design .flow/workitems/task-terminal-hooks/design.md §1.3-§1.7;
#  只读调用 _fc.*,不重复实现 flow-core.py 既有逻辑)
# ---------------------------------------------------------------------------

def _hours_to_s(hours, default):
    """小时 → 秒;None/非法/负数 → 回退 default(硬编码)。纯函数。"""
    try:
        v = int(hours)
    except (TypeError, ValueError):
        v = int(default)
    if v < 0:
        v = int(default)
    return v * 3600


def _chain_enabled(cfg):
    """chain.enabled 总闸(§1.4.1 E5);缺省 → True(与 defaults.yaml 一致)。"""
    return bool((cfg.get("chain") or {}).get("enabled", True))


def stale_action_for_state(state, chain_enabled):
    """状态 → 强制收口动作映射(§1.4.1,纯函数零 I/O)。"""
    if not chain_enabled:
        return "notify_only"          # chain 关闭 → 只提醒, 不做任何强制动作(E5)
    return {
        "designed": "auto_review",     # R1
        "reviewed": "auto_translate",  # R2
        "translated": "auto_enqueue",  # R3
        "executed": "archive",         # R4
        "verified": "archive",         # R5
    }.get(state)                        # None → created/accepted/retrospected 不参与


def classify_stale_age(age_s, cfg):
    """时间分级(§1.4.2,纯函数):<remind 静默 / <warn 提醒 / <force 升级 / ≥force 强制。"""
    c = cfg.get("chain") or {}
    remind = _hours_to_s(c.get("remind_hours"), STALE_REMIND_HOURS)
    warn = _hours_to_s(c.get("warn_hours"), STALE_WARN_HOURS)
    force = _hours_to_s(c.get("force_hours"), STALE_FORCE_HOURS)
    if age_s < remind:
        return "silent"
    if age_s < warn:
        return "remind"
    if age_s < force:
        return "escalate"
    return "force"


def learn_expected_seconds(reg, workdir, kind, fallback=1500):
    """registry(tasks.json) 同仓同类任务历史实际时长 → 预估(§1.5 纯函数)。
    样本 = tasks 中 state∈TERMINAL 且 workdir==workdir 且 kind==kind 的
    finished_at−started_at 正整数秒;EMA(α=0.5) × 1.5,floor fallback;无样本 → fallback。"""
    if not workdir or not kind:
        return int(fallback)                                   # E16
    tasks = reg.get("tasks") if isinstance(reg, dict) else {}
    if not isinstance(tasks, dict):
        return int(fallback)
    samples = []
    for t in tasks.values():
        if not isinstance(t, dict):
            continue
        if t.get("state") not in TERMINAL_STATES:
            continue
        if t.get("workdir") != workdir or t.get("kind") != kind:
            continue
        d = _wallclock_seconds(t.get("started_at"), t.get("finished_at"))  # 非法 → None(E15)
        if d and d > 0:
            samples.append(d)
    if not samples:
        return int(fallback)                                   # E14
    ema = float(samples[0])
    for d in samples[1:]:
        ema = 0.5 * d + 0.5 * ema
    return max(int(fallback), int(math.ceil(ema * 1.5)))  # noqa: RUF046


def _atomic_write_local(path, text):
    """本地原子写(temp + fsync + os.replace),与 flow-core._atomic_write 同款。"""
    tmp = f"{path}.tmp.{os.getpid()}.{secrets.token_hex(4)}"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def _pipe_notify(cfg, record):
    """管道通知核心(§1.3/§1.5):host.notify 非空才调;JSON 走 stdin(不落 argv);
    模板含控制字符 → 拒绝执行(fail-closed);失败仅告警不影响终态。"""
    if not isinstance(cfg, dict):
        return
    host = cfg.get("host") or {}
    template = host.get("notify")
    if not template or not isinstance(template, str):
        return
    if re.search(r"[\x00-\x1f]", template):
        print("告警: host.notify 含控制字符, 拒绝执行", file=sys.stderr)
        return
    try:
        proc = subprocess.run(["sh", "-c", template],
                              input=json.dumps(record, ensure_ascii=False),
                              capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            print(f"告警: notify 命令 exit={proc.returncode}", file=sys.stderr)
    except OSError as e:
        print(f"告警: notify 调用失败: {e}", file=sys.stderr)


def _notify_terminal(cfg, t, event_record):
    """L1114 原 _notify_if_configured 改由此路由(§1.3):execute 失败态 → 失败报告;
    其他(design 终态等)→ 泛化通知(既有行为不变)。"""
    if t.get("kind") == "execute" and event_record.get("state") in (
            "failed", "timeout", "partial-complete"):
        _notify_failure(cfg, t, event_record)
    else:
        _notify_if_configured(cfg, event_record)


def _notify_failure(cfg, t, event_record):
    """execute 失败报告(§1.3):含 failure_tail + 指引(源自复盘 §3.3/§3.5)。
    ocr 修复(2026-08-25):exit_code/failure_tail 改从 event_record 取——
    t 是 runner 启动时快照(running 态)不含终态字段;event_record 在 _settle
    落账后构造(diagnostic=tail/redacted_logs 路径,exit_code=rc),字段齐全。"""
    rec = {
        "kind": "execute_failure", "task_id": t.get("id"),
        "workitem": t.get("workitem"), "state": event_record.get("state"),
        "exit_code": event_record.get("exit_code"),
        "failure_tail": event_record.get("diagnostic"),
        "guidance": [
            "看 failure_tail 定位根因; exit 2 且 started≈finished = 前置校验失败"
            + "(command_not_whitelisted / duplicate_workitem / 前置状态不符)",
            "flow workitem status <id> 确认 state=translated 且 taskbook.md 非空(execute 门禁)",
            "修复后 flow workitem execute <id> 重跑; partial-complete 可 rx --continue 续收尾",
        ],
    }
    _pipe_notify(cfg, rec)


def _fc_call(name, *args):
    """getattr 探测 + 调用 _fc.<name>;缺失 → 抛 AttributeError(调用方 try/except 兜底)。"""
    fn = getattr(_fc, name, None)
    if fn is None:
        raise AttributeError(f"_fc.{name} 缺失")
    return fn(*args)


def _fc_hook_cfg(cfg):
    """获取 flow-core 完整配置(含 workitem/gates/executor/task/chain)供 _fc.* 钩子使用。
    W-B 的 load_task_config 只读 task/host/chain;W-A 钩子需要 workitem/gates/executor。
    getattr 探测 _fc.load_config;缺失/失败 → 回退传入 cfg(钩子内部 try/except 兜底)。"""
    try:
        loader = getattr(_fc, "load_config", None)
        if loader is not None:
            full = loader()
            if isinstance(full, dict):
                return full
    except Exception:  # noqa: BLE001, S110
        pass
    return cfg


def _read_wi_state_safe(wi_id, cfg):
    """读 workitem status.yaml.state(§1.3);缺失/非法/目录不存在 → None(E4/E17)。"""
    try:
        wi_dir = _fc_call("resolve_wi_dir", wi_id, cfg)
        st = _fc_call("load_status", wi_dir)
    except Exception:  # noqa: BLE001
        return None
    return st.get("state") if isinstance(st, dict) else None


def _trigger_chain(wi_id, cfg, event, to):
    """触发 W-A post-transition 钩子链(契约适配 §1):
    优先 _fc._run_post_transition_hooks(wi_dir, {"event","to"}, full_cfg);
    退化直接调 _fc._hook_auto_review(design)/_hook_auto_verify(execute)。
    getattr 探测 + try/except 兜底;缺失/异常 → {"result":"error"} 不阻断。"""
    try:
        wi_dir = _fc_call("resolve_wi_dir", wi_id, cfg)
    except Exception as e:  # noqa: BLE001
        return {"result": "error", "detail": f"resolve_wi_dir: {type(e).__name__}: {e}"}
    full_cfg = _fc_hook_cfg(cfg)
    dispatcher = getattr(_fc, "_run_post_transition_hooks", None)
    if dispatcher is not None:
        try:
            dispatcher(wi_dir, {"event": event, "to": to}, full_cfg)
            return {"result": "executed", "detail": f"post_transition:{event}"}
        except Exception:  # noqa: BLE001, S110
            pass  # 分发入口失败 → 退化到具体钩子(E19)
    hook = "_hook_auto_review" if event == "design" else "_hook_auto_verify"
    fn = getattr(_fc, hook, None)
    if fn is None:
        return {"result": "error", "detail": f"_fc.{hook} 缺失"}
    try:
        fn(wi_dir, full_cfg, False)
        return {"result": "executed", "detail": hook}
    except Exception as e:  # noqa: BLE001
        return {"result": "error", "detail": f"{hook}: {type(e).__name__}: {e}"}


def run_terminal_hooks(t, cfg, state):
    """W-B: 任务终态 → workitem 链式推进(§1.3)。best-effort, 不改任务终态。
    返回 {"action", "detail", "result"};非 skipped 动作写 events/audit.jsonl。"""
    res = _terminal_hooks_action(t, cfg, state)
    if res.get("action") != "skipped":      # ocr5-M3:skipped 不审计(与 docstring 一致)
        _append_terminal_audit_best_effort(t, res, state)
    return res


def _terminal_hooks_action(t, cfg, state):
    kind, wi = t.get("kind"), t.get("workitem")
    if not kind or not wi:
        return {"action": "skipped", "detail": "no kind/workitem"}          # E16
    chain = cfg.get("chain") or {}
    if chain.get("enabled") is False:
        return {"action": "skipped", "detail": "chain disabled"}           # E5
    if kind == "design" and state == "done":
        cur = _read_wi_state_safe(wi, cfg)          # 读 status.yaml.state; 缺失 → None
        if cur == "designed":
            r = _trigger_chain(wi, cfg, "design", "designed")  # W-A 幂等, 二次触发 no-op
            return {"action": "design_done", "detail": "on_designed", "result": r}
        return {"action": "design_done_no_transition", "detail": cur}      # E17
    if kind == "execute" and state == "done":
        cur = _read_wi_state_safe(wi, cfg)          # 读 status.yaml.state; 缺失 → None
        # ocr medium F2:与 design 分支对称的状态守门——workitem 已被转移
        # (state != executed,如 verify 已推进到 verified)时不再触发 chain,
        # 防重复 auto_verify(重复 LLM/测试)+ spurious audit
        if cur == "executed":
            r = _trigger_chain(wi, cfg, "execute", "executed")  # W-A 幂等, 二次触发 no-op
            return {"action": "execute_done", "detail": "on_executed", "result": r}
        return {"action": "execute_done_no_transition", "detail": cur}      # E17
    if kind == "execute" and state in ("failed", "timeout", "partial-complete"):
        # 失败报告由 _notify_terminal 统一触发(避免与泛化通知双发, design §1.3 末注)
        return {"action": "execute_failed", "detail": state}
    return {"action": "skipped", "detail": f"{kind}/{state}"}


def _append_terminal_audit_best_effort(t, res, state):
    """终态钩子结果写 events/audit.jsonl(§1.6, best-effort;失败仅告警不阻断)。

    task_state 用真实终态 state 参数:runner 启动快照 t 的 state=="running",
    会污染审计链(ocr F1)。"""
    try:
        _append_jsonl_locked(
            os.path.join(events_dir(), "audit.jsonl"),
            os.path.join(events_dir(), "audit.jsonl.lock"),
            {"schema_version": EVENT_SCHEMA_VERSION, "ts": now_iso(),
             "stream": TERMINAL_AUDIT_STREAM, "task_id": t.get("id"),
             "workitem": t.get("workitem"), "kind": t.get("kind"),
             "task_state": state, "action": res.get("action"),
             "detail": res.get("detail"), "result": res.get("result")})
    except (StoreError, OSError) as e:
        print(f"告警: 终态审计写入失败: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# W-S2:execute timeout/failed 终态自动抢救 + token 峰值预警
# (design .flow/workitems/executor-timeout-recovery/design.md §2-§3;
#  只读调用 _fc.* / window.*,不重复实现既有逻辑;全部 best-effort,不改任务终态)
# ---------------------------------------------------------------------------

def rescue_decision(result_status, tests_pass):
    """纯函数:execute timeout/failed 终态的抢救分流(§2.2 决策表,零 I/O)。

    result_status:executor/result.json 顶层 status(缺失/损坏已归一 None);
    tests_pass:预检测试是否通过(result 已存在时忽略)。返回
    "skip_ok" / "skip_partial" / "rescue" / "requeue"。
    """
    if result_status == "ok":
        return "skip_ok"          # 执行器已产完整产物 → 异常终态不补写不重跑
    if result_status == "partial-complete":
        return "skip_partial"     # 已有 partial 产出 → 交 rx --continue,不重跑(不丢半成品)
    return "rescue" if tests_pass else "requeue"   # 缺失/损坏 → 以测试结果机械判定


def _read_executor_result(wi_dir):
    """读 executor/result.json → (status|None, raw|None);缺失/损坏/非 dict/status
    非法 → status None(归一缺失,fail-closed,E2)。"""
    p = os.path.join(wi_dir, "executor", "result.json")
    if not os.path.isfile(p):
        return None, None
    try:
        r = json.loads(_fc.read_file(p))
    except (ValueError, OSError):
        return None, None
    if not isinstance(r, dict):
        return None, r
    st = r.get("status")
    if st not in ("ok", "partial-complete"):
        return None, r
    return st, r


def assess_execute_completion(wi_dir, full_cfg):
    """完成度机械判定(§2.2,I/O 包装):result.json 存在 → 直接分流;
    缺失/损坏 → resolve_test_command + _run_tests 预检分流(双跑为机械判定优先,
    verify 仍为权威门)。返回 (decision, result, tests)。"""
    status, raw = _read_executor_result(wi_dir)
    if status is not None:
        return rescue_decision(status, None), raw, None
    tc = _fc.resolve_test_command(wi_dir, _fc.workdir(), None, None)
    if tc["command"] is None:      # 无测试命令 → 无法确认「绿」→ 不达标(E1)
        return "requeue", None, {"pass": False, "reason": "command_unresolved"}
    # ocr medium F1:锁内测试必须有界——full_cfg.rescue.test_timeout_s 可覆盖,
    # 缺失/非法/非 dict 回退 RESCUE_TEST_TIMEOUT_S(300),防测试挂起阻塞该 workitem 全部转移
    timeout = RESCUE_TEST_TIMEOUT_S
    rescue_cfg = (full_cfg or {}).get("rescue")
    if isinstance(rescue_cfg, dict):
        try:
            timeout = int(rescue_cfg.get("test_timeout_s", RESCUE_TEST_TIMEOUT_S))
        except (TypeError, ValueError):
            pass
    if timeout <= 0:
        timeout = RESCUE_TEST_TIMEOUT_S
    tests = _fc._run_tests(_fc.workdir(), tc["command"], timeout=timeout)
    return rescue_decision(None, tests["pass"]), None, tests


def build_rescue_result(test_command, changed_files, original_state, original_exit):
    """构造抢救版 result.json(与 executor wrapper 产出结构同形,§2.3 ①),
    确保 verify 的 resolve_test_command / check_diff_scope / _collect_changed_files
    正常消费;executor="rescue" + rescued=true + note 标注来源(绝不伪装真实 executor)。"""
    return {
        "schema_version": 1,
        "executor": "rescue",                       # 区分真实 executor 产出
        "status": "ok",
        "exit_code": 0,
        "test_command": test_command,
        "rescued": True,
        "note": f"自动抢救，原始 {original_state}",
        "original_state": original_state,           # timeout / failed
        "original_exit_code": original_exit,        # 124 等
        "duration_s": None,
        "diff": {"files_changed": len(changed_files), "insertions": 0, "deletions": 0,
                 "changed_files": changed_files, "untracked_files": [],
                 "patch": "executor/diff.patch"},
        "cost": None, "redacted_logs": None,
        "started_at": None, "finished_at": None,
    }


def _git_rescue_diff(workdir):
    """git diff 未提交改动(§2.3 ②,Python 侧等价 wrapper 的 git 段)。

    返回 {"patch", "changed_files", "untracked_files"};非 git 仓 / git 不可用 /
    无改动 → 空 patch + 空列表(不 crash,E3,verify 侧 fail-closed)。"""
    def _run(args):
        # ocr F4:与 _git_run 对齐加 -c core.quotePath=false——含非 ASCII 文件名
        # (如中文翻译 workitem)的仓库,diff --name-only/status 路径若被八进制转义
        # ("\344\270\255..."),下游 changed_files/untracked 解析错;errors="replace"
        # 同 _git_run:非 UTF-8 文件名不抛(UnicodeDecodeError 归零抛)
        try:
            return subprocess.run(  # noqa: PLW1510
                ["git", "-c", "core.quotePath=false", "-C", workdir] + args,
                capture_output=True, text=True, errors="replace", timeout=30)
        except (OSError, subprocess.TimeoutExpired, UnicodeDecodeError):
            return None
    head = _run(["rev-parse", "--verify", "HEAD"])
    if head is None or head.returncode != 0:
        return {"patch": "", "changed_files": [], "untracked_files": []}
    # ocr7-M1:diff HEAD 走有界流式读(上限 RESCUE_DIFF_MAX_BYTES),超限截断 +
    # 告警;--name-only/status 为路径清单(量级小),维持 capture_output 全量读
    patch, truncated = _git_diff_bounded(workdir, RESCUE_DIFF_MAX_BYTES)
    names = _run(["diff", "--name-only"])
    st = _run(["status", "--porcelain"])
    changed = [ln for ln in (names.stdout or "").splitlines() if ln.strip()] \
        if names is not None else []
    untracked = []
    if st is not None:
        for ln in st.stdout.splitlines():
            if ln.startswith("??"):
                untracked.append(ln[3:].strip())
    if truncated:
        print(f"告警: git diff HEAD 输出超过 {RESCUE_DIFF_MAX_BYTES} 字节上限,"
              "diff.patch 已截断(verify diff gate 将 fail-closed)",
              file=sys.stderr)
    return {"patch": patch, "changed_files": changed, "untracked_files": untracked}


def _git_diff_bounded(workdir, max_bytes):
    """流式读 git diff HEAD 输出至多 max_bytes(防大 patch 全量进内存)。

    返回 (patch_text, truncated):超限时提前关管道(读侧停止,git 写侧 EPIPE 退出),
    patch 尾部追加截断标记;git 缺失/失败 → ("", False)(零抛,同 _run 降级)。"""
    _TRUNC_MARK = "\n[...git diff 输出超上限截断...]\n"
    try:
        proc = subprocess.Popen(
            ["git", "-c", "core.quotePath=false", "-C", workdir, "diff", "HEAD"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    except OSError:
        return "", False
    chunks, total = [], 0
    truncated = False
    # ocr7-M1:读循环整体 30s deadline(对齐旧 _run 的 timeout=30)——git 挂起
    # 不产出(如 NFS 卡住)时 select 超时截断退出,不无限阻塞 rescue
    deadline = time.monotonic() + 30
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                truncated = True
                break
            # ocr9-F1:select 在 Windows 也能 import 成功,但 select.select 只支持
            # WinSock socket,对管道 fd 抛 OSError(WinError 10038);须以 os.name 门禁
            # POSIX,非 POSIX 直接退回阻塞读(接受无读超时),不把误判 OSError 传给
            # rescue 侧错标 rescue_write_fail。
            if select is not None and os.name == "posix":
                r, _, _ = select.select([proc.stdout], [], [], remaining)
                if not r:
                    truncated = True
                    break
            chunk = proc.stdout.read(min(65536, max_bytes - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total >= max_bytes:
                truncated = True
                break
    finally:
        proc.stdout.close()
        try:
            proc.wait(timeout=30)
        except (subprocess.TimeoutExpired, OSError):
            proc.kill()
            proc.wait()
    text = b"".join(chunks).decode("utf-8", errors="replace")
    if truncated:
        return text + _TRUNC_MARK, True
    return text, False


def _rescue_max_retries(cfg):
    """自动重跑限次(§2.7):config task.rescue_max_retries;缺失/非法/非正 → 2。"""
    try:
        v = int((cfg.get("task") or {}).get("rescue_max_retries") or RESCUE_MAX_RETRIES)
    except (TypeError, ValueError):
        v = RESCUE_MAX_RETRIES
    return v if v > 0 else RESCUE_MAX_RETRIES


def _token_warn_threshold(cfg):
    """token 峰值告警阈值(§2.7):config task.token_warn_threshold;缺失/非法/非正 → 300000。"""
    try:
        v = int((cfg.get("task") or {}).get("token_warn_threshold") or TOKEN_WARN_THRESHOLD)
    except (TypeError, ValueError):
        v = TOKEN_WARN_THRESHOLD
    return v if v > 0 else TOKEN_WARN_THRESHOLD


def _append_rescue_audit_best_effort(t, decision, action, result, note=None,
                                     original_state=None, original_exit_code=None):
    """抢救审计写 events/audit.jsonl(§2.5,stream=rescue,actor=flow:rescue;
    best-effort:失败仅告警,不影响任务终态与 dispatch)。"""
    try:
        _append_jsonl_locked(
            os.path.join(events_dir(), "audit.jsonl"),
            os.path.join(events_dir(), "audit.jsonl.lock"),
            {"schema_version": EVENT_SCHEMA_VERSION, "ts": now_iso(),
             "stream": RESCUE_AUDIT_STREAM, "actor": "flow:rescue",  # 零个人标识
             "task_id": t.get("id"), "workitem": t.get("workitem"), "kind": t.get("kind"),
             "original_state": original_state, "original_exit_code": original_exit_code,
             "decision": decision, "action": action, "result": result, "note": note})
    except (StoreError, OSError) as e:
        print(f"告警: rescue 审计写入失败: {e}", file=sys.stderr)


def _notify_rescue(cfg, kind, t, wi_id, terminal=None, **extra):
    """rescue 通知(§2.3/§2.4,host.notify 通道;未配置 → _pipe_notify no-op)。
    可 accept 报告摘要取自 _fc.acceptance_summary(v["verify"]);绝不调用任何 accept。
    ocr F6:terminal 为 execute 终态失败上下文({"state","exit_code","failure_tail"}),
    rescue 发通知时随通知携带——单通知原则下终态失败通知不再重复发,信息不丢。"""
    rec = {"kind": kind, "task_id": t.get("id"), "workitem": wi_id}
    rec.update(extra)
    if terminal is not None:
        rec["terminal_failure"] = terminal
    if kind == "rescue_accept_pending" and extra.get("verify"):
        rec["summary"] = _fc.acceptance_summary(extra["verify"])
        rec["guidance"] = [
            "execute 终态自动抢救完成:已补 executor/result.json + diff.patch 并通过质量门,"
            + "可人工 accept(accept 永远人工)",
        ]
    elif kind == "rescue_verify_fail":
        try:
            rec["gate"] = _fc._three_item_report(extra["verify"])
        except Exception:  # noqa: BLE001, S110
            pass
        rec["guidance"] = [
            "verify 未通过(fail-closed,不 accept):检查 taskbook diff_scope / design.md "
            + "错误表后人工处理",
        ]
    elif kind == "rescue_verify_error":
        rec["guidance"] = ["verify 前置失败(产物缺失/损坏):请人工检查 workitem 后处理"]
    elif kind == "rescue_frozen":
        rec["guidance"] = [
            "execute 自动重跑已达上限已冻结:请人工介入(修复根因后手动 execute 或解除 "
            + "rescue_frozen)",
        ]
    elif kind == "rescue_write_fail":
        rec["guidance"] = ["抢救写 executor 产物失败:请人工补 result.json + diff.patch"]
    _pipe_notify(cfg, rec)


def do_rescue(t, wi_id, wi_dir, cfg, full_cfg, original_state, original_exit,
              terminal=None):
    """decision=="rescue":补 result.json + diff.patch → verify → 可 accept 报告(§2.3)。

    只读复用 _fc.run_verify_auto_core(与正常 execute 同一质量门,绝不旁路);
    写失败 → audit rescue_write_fail + notify(E8);verify 前置缺失 → rescue_verify_error(E9);
    gate 不过 → rescue_verify_fail(不 accept,fail-closed);通过 → 可 accept 报告
    (rescue_fail_count 归零由调用方在锁外执行,避免嵌套 flock)。
    (ocr5-L2:原 result_raw/tests 死参数已删——函数体自行 resolve_test_command/
    _git_rescue_diff 重建产物,不依赖调用方预读值。)
    """
    try:
        tc = _fc.resolve_test_command(wi_dir, _fc.workdir(), None, None)
        d = _git_rescue_diff(_fc.workdir())
        changed = list(d["changed_files"]) + list(d["untracked_files"])
        result = build_rescue_result(tc["command"], changed, original_state, original_exit)
        executor_dir = os.path.join(wi_dir, "executor")
        os.makedirs(executor_dir, exist_ok=True)
        _atomic_write_local(os.path.join(executor_dir, "result.json"),
                            json.dumps(result, ensure_ascii=False, separators=(",", ":")))
        _atomic_write_local(os.path.join(executor_dir, "diff.patch"), d["patch"])
    except OSError as e:
        _append_rescue_audit_best_effort(
            t, "rescue", "rescue_write_fail", {"error": str(e)},
            original_state=original_state, original_exit_code=original_exit)
        _notify_rescue(cfg, "rescue_write_fail", t, wi_id, terminal=terminal, error=str(e))
        return "write_fail"
    v = _fc.run_verify_auto_core(wi_id, wi_dir, {}, full_cfg)
    if not v["ok"]:
        _append_rescue_audit_best_effort(
            t, "rescue", "rescue_verify_error", {"error": v.get("error")},
            original_state=original_state, original_exit_code=original_exit)
        _notify_rescue(cfg, "rescue_verify_error", t, wi_id, terminal=terminal, error=v.get("error"))
        return "verify_error"
    if v["gate_pass"]:
        _append_rescue_audit_best_effort(
            t, "rescue", "rescue_success", {"gate_pass": True},
            original_state=original_state, original_exit_code=original_exit)
        _notify_rescue(cfg, "rescue_accept_pending", t, wi_id, terminal=terminal, verify=v["verify"])
        return "rescued"
    _append_rescue_audit_best_effort(
        t, "rescue", "rescue_verify_fail", {"gate_pass": False},
        original_state=original_state, original_exit_code=original_exit)
    _notify_rescue(cfg, "rescue_verify_fail", t, wi_id, terminal=terminal, verify=v["verify"])
    return "verify_fail"


def _reset_rescue_fail_count(wi_dir):
    """成功抢救后计数归零(§2.3 ⑤);rescue_frozen 保持粘性不清。锁内读-改-写。"""
    def _do():
        st = _fc.load_status(wi_dir)
        st["rescue_fail_count"] = 0
        _fc.save_status_atomic(wi_dir, st)
    try:
        _fc.with_workitem_lock(wi_dir, _do)
    except (StoreError, OSError):
        pass


def do_requeue(t, wi_id, wi_dir, cfg, full_cfg, original_state, original_exit, terminal=None):
    """decision=="requeue":风暴计数 → 未冻结则重入队 execute 下一空闲窗口(§2.4)。

    复用 chain-on-transition frozen 机制但独立键 rescue_fail_count/rescue_frozen
    (不触碰 verify 链 chain_fail_count 语义);已冻结 → 仅 audit rescue_skip_frozen(E6);
    超限 → notify rescue_frozen + audit 升级人工;重入队遇并发非终态冲突 → 回滚计数 +
    audit rescue_requeue_conflict(E7,非任务本身失败不递增);QueueFull 同处理。
    """
    def _bump():
        st = _fc.load_status(wi_dir)
        before = {k: v for k, v in st.items()
                  if k in ("rescue_fail_count", "rescue_frozen")}
        if st.get("rescue_frozen"):
            return before, st, "frozen_skip"          # 已冻结 → 不再重入队(E6)
        try:
            cnt = int(st.get("rescue_fail_count", 0)) + 1
        except (TypeError, ValueError):
            cnt = 1                                   # 计数损坏 → 归一 1(不 crash,不提前冻结)
        st["rescue_fail_count"] = cnt
        if cnt >= _rescue_max_retries(cfg):
            st["rescue_frozen"] = True
        _fc.save_status_atomic(wi_dir, st)
        return before, st, ("frozen" if st.get("rescue_frozen") else "requeue")
    before, st, action = _fc.with_workitem_lock(wi_dir, _bump)
    if action == "frozen_skip":
        _append_rescue_audit_best_effort(
            t, "requeue", "rescue_skip_frozen", None,
            original_state=original_state, original_exit_code=original_exit)
        return "frozen_skip"                            # 不重复 notify(仅入冻临界通知一次)
    if action == "frozen":
        _append_rescue_audit_best_effort(
            t, "requeue", "rescue_frozen",
            {"rescue_fail_count": st.get("rescue_fail_count")},
            original_state=original_state, original_exit_code=original_exit)
        _notify_rescue(cfg, "rescue_frozen", t, wi_id, terminal=terminal,
                       rescue_fail_count=st.get("rescue_fail_count"))
        return "frozen"                                 # 超限冻结 + 升级人工
    try:
        scheduled = window.next_offpeak_start(
            datetime.now(timezone.utc)).isoformat(timespec="seconds")
        # W-S1 §6.2/§6.3:build_execute_command 首参为 wi_dir(size 判定需完整目录);
        # expected-seconds 地板 = max(原 expected, timeout+115),防队列先杀 large rescue。
        # ocr medium F3:复用 _p 避免二次 resolve;executor/tb_model 先按 workitem
        # 声明解析(与 enqueue_execute_retry 同构),保证 _p 与命令串 executor 自洽
        # (script/local 任务不会拿到 reasonix 链的模型/超时)。
        _executor, _tb_model = _fc._resolve_executor_decl(wi_dir, full_cfg)
        _p = _fc.resolve_execute_params(wi_dir, full_cfg, force=True,
                                        executor=_executor, tb_model=_tb_model)
        command = _fc.build_execute_command(wi_dir, full_cfg, force=True, params=_p)  # --force 仅越过前置状态检查
        exp = t.get("expected_seconds")
        exp = max(int(exp) if exp else 0, _p["timeout_s"] + 115)
        add_task(cfg, command, workitem=wi_id, priority="P2", kind="execute",
                 expected_seconds=exp,
                 workdir=t.get("workdir"),
                 scheduled_at=scheduled, why=f"rescue requeue {wi_id}")
    except Exception as e:  # 入队失败全兜底:回滚计数,绝不泄漏(E7/QueueFull/其他)  # noqa: BLE001
        def _rollback():
            s2 = _fc.load_status(wi_dir)
            # ocr F1:条件式回滚——仅当相关键仍等于 _bump 保存值(st,即未被并发
            # rescue 流修改)才恢复;任一不符则跳过,避免 stale before 覆盖
            # 并发 bump/冻结(TOCTOU:before 于 _bump 锁内快照,本函数重新持锁)。
            for k in ("rescue_fail_count", "rescue_frozen"):
                if k in st:
                    if s2.get(k) != st[k]:
                        return
                elif k in s2:
                    return
            for k in ("rescue_fail_count", "rescue_frozen"):
                if k in before:
                    s2[k] = before[k]
                else:
                    s2.pop(k, None)
            _fc.save_status_atomic(wi_dir, s2)
        try:
            _fc.with_workitem_lock(wi_dir, _rollback)
        except Exception:  # 回滚本身失败仅告警,不影响任务终态  # noqa: BLE001, S110
            pass
        if isinstance(e, DuplicateWorkitem):
            action = "rescue_requeue_conflict"
        elif isinstance(e, QueueFull):
            action = "rescue_requeue_queue_full"
        else:
            action = "rescue_requeue_error"
        _append_rescue_audit_best_effort(
            t, "requeue", action, {"error": str(e)},
            original_state=original_state, original_exit_code=original_exit)
        return "conflict"
    _append_rescue_audit_best_effort(
        t, "requeue", "rescue_requeue", {"scheduled_at": scheduled},
        original_state=original_state, original_exit_code=original_exit)
    return "requeued"


def run_rescue_hook(t, cfg, state, rc=None, terminal=None):
    """W-S2:execute timeout/failed 终态自动抢救入口(§2.1 数据流,§3 错误表)。

    best-effort,不改任务终态;触发条件:chain.enabled + kind=execute +
    state∈RESCUE_STATES + workitem 非空;不满足首行返回 "skipped"(E12/E13)。
    resolve_wi_dir 失败 → audit rescue_error(E4)。rescue 分支的读判定与写产物/
    verify 各占一次 with_workitem_lock 临界区(均与 verify/execute 同一把 .lock
    防竞争写;两次临界区之间锁已释放,不连续持锁);计数归零在锁外执行
    (避免嵌套 flock)。返回动作串供测试断言。
    ocr F6:terminal 为 execute 终态失败上下文,透传进 rescue 通知(单通知)。
    """
    if not _chain_enabled(cfg):
        return "skipped"                              # E12:零动作、零 audit
    if t.get("kind") != "execute" or state not in RESCUE_STATES or not t.get("workitem"):
        return "skipped"                              # E13:非 execute 不触发
    wi_id = t["workitem"]
    full_cfg = _fc_hook_cfg(cfg)
    try:
        wi_dir = _fc.resolve_wi_dir(wi_id, dict(full_cfg, workitem={
            "id_max_len": 64, "plane_id": "control"}))
    except Exception as e:  # noqa: BLE001
        _append_rescue_audit_best_effort(
            t, None, "rescue_error", {"error": f"{type(e).__name__}: {e}"},
            original_state=state, original_exit_code=rc)
        return "error"                                # E4:跳过抢救,不改任务终态
    decision, result, _tests = _fc.with_workitem_lock(
        wi_dir, lambda: assess_execute_completion(wi_dir, full_cfg))
    if decision == "rescue":
        # 写产物 + verify 另行持锁(design §5:与 verify/execute 同一把 .lock
        # 防竞争写;与读判定为两次独立临界区,中间锁已释放)
        def _rescue_locked():
            return do_rescue(t, wi_id, wi_dir, cfg, full_cfg, state, rc,
                             terminal=terminal)
        action = _fc.with_workitem_lock(wi_dir, _rescue_locked)
        if action == "rescued":                       # gate_pass → 锁外归零计数(避免嵌套 flock)
            _reset_rescue_fail_count(wi_dir)
        return action
    if decision == "requeue":
        return do_requeue(t, wi_id, wi_dir, cfg, full_cfg, state, rc, terminal=terminal)
    # skip_ok / skip_partial:已有产物/半成品 → 仅 audit,不补写不重跑
    _append_rescue_audit_best_effort(
        t, decision, decision,
        {"status": result.get("status") if isinstance(result, dict) else None},
        original_state=state, original_exit_code=rc)
    return decision


def extract_reasonix_token_peak(started_at, finished_at):
    """reasonix 日志 token 峰值(§2.6):按任务窗口 [started_at-120s, finished_at]
    匹配 runs/*.log(下界保留 ±120s 就近容差,与 extract_reasonix_cost 同口径;
    finished_at 约束上界,排除任务结束后启动的 run;finished_at 缺失/非法 →
    上界回退 started_at+120s,维持原 ±120s 行为),对匹配日志全文 finditer
    TOKEN_PEAK_RE → 全局 max(容忍千分位)。无匹配/目录缺失/时间非法 → None(E10,
    静默跳过)。"""
    if not started_at:
        return None
    try:
        st = datetime.fromisoformat(started_at).astimezone(TZ_CN)
    except (TypeError, ValueError):
        return None
    lo = st - timedelta(seconds=120)
    hi = st + timedelta(seconds=120)          # finished_at 缺失/非法时维持原 ±120s 口径
    if finished_at:
        try:
            ft = datetime.fromisoformat(finished_at).astimezone(TZ_CN)
        except (TypeError, ValueError):
            pass
        else:
            hi = ft
    runs_dir = _reasonix_runs_dir()
    if not os.path.isdir(runs_dir):
        return None
    try:
        names = os.listdir(runs_dir)
    except OSError:
        return None
    peak = None
    for name in names:
        m = re.match(r"^(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2})(\d{2})", name)
        if not m:
            continue
        p = os.path.join(runs_dir, name)
        if not os.path.isfile(p):                     # 同名目录(非日志)不参与匹配
            continue
        y, mo, d, h, mi, s = (int(x) for x in m.groups())
        try:
            dt = datetime(y, mo, d, h, mi, s, tzinfo=TZ_CN)
        except ValueError:
            continue
        if dt < lo or dt > hi:
            continue
        try:
            # ocr F4:runaway 大日志超上限跳过(仿 STALE_TAIL_SCAN_BYTES 有界扫描,
            # 不全量读入内存;不超限仍全文 finditer 保峰值口径)
            if os.path.getsize(p) > TOKEN_PEAK_SCAN_BYTES:
                continue
            text = _fc.read_file(p)
        except OSError:
            continue
        for mt in TOKEN_PEAK_RE.finditer(text):
            try:
                v = int(str(mt.group(1)).replace(",", "").replace("_", ""))
            except ValueError:
                continue
            if peak is None or v > peak:
                peak = v
    return peak


def run_token_warning(t, cfg, finished_at):
    """execute 终态 token 峰值预警(§2.6):峰值 > 阈值 → 通知 + audit;无匹配/未超
    → 跳过。仅通知 + audit,不改任何状态;host.notify 未配置 → _pipe_notify no-op。"""
    if t.get("kind") != "execute":
        return "skipped"
    peak = extract_reasonix_token_peak(t.get("started_at"), finished_at)
    if peak is None:
        return "skipped"                              # E10:不 crash
    threshold = _token_warn_threshold(cfg)
    if peak <= threshold:
        return "below_threshold"
    _pipe_notify(cfg, {"kind": "token_warning", "task_id": t.get("id"),
                       "workitem": t.get("workitem"), "peak_tokens": peak,
                       "threshold": threshold,
                       "guidance": ["上下文过大，下次建议拆分任务"]})
    _append_rescue_audit_best_effort(
        t, None, "token_warning", {"peak_tokens": peak, "threshold": threshold})
    return "alerted"


def resolve_scan_roots(cfg):
    """扫描根解析(§1.4.7,可注入):FLOW_STALE_SCAN_ROOT 优先;否则 chain.scan_projects
    或 glob 全项目;恒含当前仓 data_dir/workitems。
    ocr6-F3:fallback 项目根由 _data_dir() 派生(其父目录的父目录,即
    dirname(dirname(data_dir)) 下的兄弟项目),不再硬编码
    ~/.openclaw/workspace/projects——非默认 FLOW_DATA_DIR 部署时扫对根。"""
    ov = os.environ.get("FLOW_STALE_SCAN_ROOT")
    if ov:
        return [ov]
    c = cfg.get("chain") or {}
    sp = c.get("scan_projects")
    roots = []
    if sp:
        roots += [os.path.join(p, ".flow", "workitems") for p in sp]
    else:
        base = os.path.dirname(os.path.dirname(os.path.abspath(_data_dir())))
        roots += glob.glob(os.path.join(base, "*", ".flow", "workitems"))
    roots.append(os.path.join(_data_dir(), "workitems"))
    return list(dict.fromkeys(roots))


def _peek_state(wi_dir):
    """浅读 workitem status.yaml 的 state;缺失/非法 → None。"""
    sp = os.path.join(wi_dir, "status.yaml")
    if not os.path.isfile(sp):
        return None
    try:
        status = _fc.parse_yaml(_fc.read_file(sp), sp)
    except Exception:  # noqa: BLE001
        return None
    if not isinstance(status, dict):
        return None
    st = status.get("state")
    return st if isinstance(st, str) else None


def iter_stuck_workitems(scan_roots):
    """扫描 roots 下 workitem 目录, 产出 (wi_dir, wi_id, state), 仅 STALE_STATES。
    状态浅读失败 → 跳过(权威校验在 run_stale_gate 内 load_status 复核)。"""
    for root in scan_roots:
        if not root or not os.path.isdir(root):
            continue
        try:
            names = sorted(os.listdir(root))
        except OSError:
            continue
        for name in names:
            wi_dir = os.path.join(root, name)
            if not os.path.isdir(wi_dir):
                continue
            state = _peek_state(wi_dir)
            if state in STALE_STATES:
                yield wi_dir, name, state


def last_event_ts(wi_dir):
    """workitem events.jsonl 最后一行 ts → aware datetime;缺失/非法/naive → None(E1/E2)。

    ocr L2:有界尾部读取(仿 last_notify_ts,seek 到文件末 ≤STALE_TAIL_SCAN_BYTES),
    避免 events.jsonl 无限增长时每个 pump 周期对每个候选 workitem 逐行全量顺序 I/O。
    """
    path = os.path.join(wi_dir, "events.jsonl")
    if not os.path.isfile(path):
        return None                                              # E1
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - STALE_TAIL_SCAN_BYTES))
            raw = f.read(STALE_TAIL_SCAN_BYTES)
    except OSError:
        return None
    lines = [ln for ln in raw.decode("utf-8", errors="replace").split("\n") if ln.strip()]
    if not lines:
        return None
    last = lines[-1]
    try:
        rec = json.loads(last)
    except ValueError:
        return None
    ts = rec.get("ts")
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        return None                                              # E2 naive
    return dt


def last_notify_ts(wi_id, level):
    """events/audit.jsonl 有界尾部扫描, 找最近一条 workitem==wi_id and level==level 的 ts。
    缺失/非法/naive → None。"""
    path = os.path.join(events_dir(), "audit.jsonl")
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - STALE_TAIL_SCAN_BYTES))
            raw = f.read(STALE_TAIL_SCAN_BYTES)
    except OSError:
        return None
    lines = [ln for ln in raw.decode("utf-8", errors="replace").split("\n") if ln.strip()]
    for line in reversed(lines):
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if rec.get("workitem") != wi_id or rec.get("level") != level:
            continue
        ts = rec.get("ts")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts)
        except (TypeError, ValueError):
            continue
        if dt.tzinfo is None:
            continue
        return dt
    return None


def dedup_ok(wi_id, level, now, cooldown_s=STALE_DEDUP_COOLDOWN_S):
    """remind/escalate/force 去重 ≤1/天(§1.4.6,audit 尾部反查)。

    ocr8-M2:force 原无条件放行,假设「force 动作成功即离开 stuck 集」。但
    notify_only(chain 关闭)与 R1 reject 不转移状态,workitem 停留 stuck 集 →
    每个 pump 周期重复通知/audit(甚至重复 LLM 调用)。故 force 也走冷却;
    成功转移的 workitem 会写新 event 重置计时,不会立即再次触发,不受冷却影响。"""
    last = last_notify_ts(wi_id, level)
    return last is None or (now - last).total_seconds() >= cooldown_s


def _notify_stale(cfg, wi_id, state, level):
    """stale 提醒/升级推送(§1.4.4):复用 host.notify;remind 温和 / escalate 明示收口。"""
    rec = {
        "kind": "stale_workitem", "workitem": wi_id, "state": state, "level": level,
        "guidance": (
            [f"workitem {wi_id} 卡在 {state} 已超时, 请及时处理: flow workitem status {wi_id}"]
            if level == "remind" else
            [f"workitem {wi_id} 卡在 {state} 即将自动收口, 请尽快干预: flow workitem status {wi_id}"]
        ),
    }
    _pipe_notify(cfg, rec)
    return {"result": "notified", "level": level, "detail": f"{state} stale"}


def _notify_stale_archived(cfg, wi_id, state, target, error=None):
    """归档推送(§1.4.5):复用 host.notify;move 失败时 error 非空。"""
    _pipe_notify(cfg, {
        "kind": "stale_archived", "workitem": wi_id, "state": state,
        "target": target, "error": error,
    })


def _notify_stale_require_human(cfg, wi_id, state, detail=None):
    """require_human_review 升级通知(§1.4.4,ocr10-M1):标记强制人工介入的
    workitem 不被 stale gate 自动归档——保留原地,升级人工通知等人工处理。"""
    _pipe_notify(cfg, {
        "kind": "stale_require_human", "workitem": wi_id, "state": state,
        "detail": detail,
        "guidance": [
            (f"workitem {wi_id} 标记 require_human_review, 需人工介入且不会自动归档: "
             f"flow workitem status {wi_id}"),
        ],
    })


def _review_outcome(r):
    """从 W-A _hook_auto_review 返回的 audit 记录推断 outcome(契约适配 §2/§5)。
    兼容 {"verdict": ...} / {"result": str} / {"action": "auto_review_*"} 三种形状。"""
    if not isinstance(r, dict):
        return {"result": "executed", "detail": None, "skipped": False}
    if "verdict" in r:
        v = r.get("verdict")
        return {"result": v, "detail": r.get("reason"), "skipped": v == "skipped"}
    if isinstance(r.get("result"), str):
        v = r["result"]
        return {"result": v, "detail": r.get("detail"), "skipped": v == "skipped"}
    action = str(r.get("action") or "")
    if "skip" in action:
        return {"result": "skipped", "detail": r.get("output"), "skipped": True}
    if "reject" in action:
        return {"result": "reject", "detail": r.get("result"), "skipped": False}
    if "pass" in action:
        return {"result": "pass", "detail": r.get("result"), "skipped": False}
    if "error" in action:
        return {"result": "error", "detail": r.get("error"), "skipped": False}
    return {"result": "executed", "detail": None, "skipped": False}


def execute_stale_action(wi_id, wi_dir, state, action, level, cfg, now=None):
    """force 动作执行(§1.4.4)。remind/escalate 只推送;force 按 action 分发 R1-R5。
    所有 _fc.* 调用 getattr 探测 + try/except 兜底(E8/E19)。"""
    if level != "force" or action == "notify_only":
        return _notify_stale(cfg, wi_id, state, level)
    full_cfg = _fc_hook_cfg(cfg)
    if action == "auto_review":                     # R1
        try:
            r = _fc_call("_hook_auto_review", wi_dir, full_cfg, False)
        except Exception as e:  # noqa: BLE001
            return {"result": "error", "detail": f"auto_review: {type(e).__name__}: {e}"}
        oc = _review_outcome(r)
        if oc["skipped"]:                            # E6 require_human_review → 不自动归档,
            _notify_stale_require_human(cfg, wi_id, state, oc["detail"])  # 升级人工通知保留
            return {"result": "notified_skipped", "detail": oc["detail"]}
        return {"result": oc["result"], "detail": oc["detail"]}
    if action == "auto_translate":                  # R2
        try:
            return _fc_call("_hook_auto_translate", wi_dir, full_cfg, False)
        except Exception as e:  # noqa: BLE001
            return {"result": "error", "detail": f"auto_translate: {type(e).__name__}: {e}"}
    if action == "auto_enqueue":                    # R3
        # ocr7-L2:原 learn_expected_seconds → _inject_expected_seconds 写
        # design-result.json 的 expected_seconds 为死字段(下游 _hook_auto_enqueue
        # 自算 max(learn, timeout+115),从不读该字段)→ 删除注入,learn 函数保留
        try:
            return _fc_call("_hook_auto_enqueue", wi_dir, full_cfg, False)
        except Exception as e:  # noqa: BLE001
            return {"result": "error", "detail": f"auto_enqueue: {type(e).__name__}: {e}"}
    if action == "archive":                         # R4/R5
        return archive_stale_workitem(wi_id, wi_dir, state, "stale_>48h", cfg=cfg)
    return {"result": "error", "detail": f"unknown action {action}"}


def archive_stale_workitem(wi_id, wi_dir, state, reason, cfg=None):
    """归档(§1.4.5,不删除、可恢复):shutil.move 到 <project>/.flow/stale/<id>/ +
    stale.json marker(original_path/state/reason/archived_at);
    目标已存在 → 时间戳后缀(E10);move 失败 → 保留原地 + audit failed + 通知(E11)。
    ocr6-F1:move 成功后 marker 写失败(磁盘满等) → 尽力移回原处恢复;恢复也失败
    → workitem 留在 stale 目录,通知携带 target 供人工恢复——不留半归档状态,
    可恢复信息不丢。"""
    stale_root = os.path.abspath(os.path.join(os.path.dirname(wi_dir), "..", "stale"))
    target = os.path.join(stale_root, wi_id)
    if os.path.exists(target):                        # E10 目标已存在 → 后缀避免覆盖
        target = os.path.join(stale_root, f"{wi_id}-{int(time.time())}")
    os.makedirs(stale_root, exist_ok=True)
    try:
        shutil.move(wi_dir, target)
    except OSError as e:
        _notify_stale_archived(cfg, wi_id, state, None, error=str(e))
        return {"result": "error", "detail": f"move failed: {e}"}    # E11
    marker = {"schema_version": 1, "id": wi_id, "state": state, "reason": reason,
              "archived_at": now_iso(), "original_path": wi_dir}
    try:
        _atomic_write_local(os.path.join(target, "stale.json"),
                            json.dumps(marker, ensure_ascii=False))
    except OSError as e:
        try:
            shutil.move(target, wi_dir)               # 尽力恢复:移回原处
        except (OSError, shutil.Error) as e2:          # 恢复失败 → 标记可恢复位置
            _notify_stale_archived(
                cfg, wi_id, state, target,
                error=f"marker write failed: {e}; restore failed: {e2}")
            return {"result": "error",
                    "detail": f"marker write failed: {e}; restore failed: {e2}; "
                              f"recover at {target}"}
        _notify_stale_archived(
            cfg, wi_id, state, None,
            error=f"marker write failed: {e}; restored to {wi_dir}")
        return {"result": "error",
                "detail": f"marker write failed: {e}; workitem restored to {wi_dir}"}
    _notify_stale_archived(cfg, wi_id, state, target)
    return {"result": "archived", "target": target}


def append_stale_audit(wi_id, state, age_s, level, action, res, now=None):
    """stale 收口动作写 events/audit.jsonl(§1.6,复用 _append_jsonl_locked)。

    ocr5-M1:now 为注入时钟 seam——缺省 datetime.now(UTC) 与 now_iso() 同口径;
    测试传入固定 now(与 run_stale_gate 的 now 参数同源),使 audit ts 与
    dedup_ok 的 now 比较共用同一时钟,去重判定确定性。"""
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)  # 防御:与 run_stale_gate 的 now 补全对称
    return _append_jsonl_locked(
        os.path.join(events_dir(), "audit.jsonl"),
        os.path.join(events_dir(), "audit.jsonl.lock"),
        {"schema_version": EVENT_SCHEMA_VERSION, "ts": now.isoformat(timespec="seconds"),
         "stream": STALE_TERMINAL_AUDIT_STREAM, "workitem": wi_id, "state": state,
         "age_s": int(age_s), "level": level, "action": action,
         "result": res.get("result"), "detail": res.get("detail")})


def run_stale_gate(cfg, now=None, scan_roots=None):
    """超时收口编排(§1.4.3):扫描 roots → 计龄分级 → 去重 → 动作 → audit。
    全程 fail-closed(E1/E2/E3/E4),单 workitem 异常不影响其余。"""
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    results = []
    try:
        roots = scan_roots or resolve_scan_roots(cfg)
    except Exception:  # noqa: BLE001
        roots = []
    for wi_dir, wi_id, state in iter_stuck_workitems(roots):
        try:
            st = _fc_call("load_status", wi_dir)     # status 缺失/非法 → 跳过(E4)
        except Exception:  # noqa: BLE001, S112
            continue
        if not isinstance(st, dict):
            continue
        try:
            if _fc_call("is_locked", st, now):       # 有人处理中 → 跳过(E3)
                continue
        except Exception:  # noqa: BLE001, S112
            continue
        last = last_event_ts(wi_dir)                 # events.jsonl 最后 ts;缺失/非法 → 跳过(E1/E2)
        if last is None:
            continue
        age_s = (now - last).total_seconds()
        if age_s < 0:
            continue
        level = classify_stale_age(age_s, cfg)
        if level == "silent":
            continue
        action = stale_action_for_state(state, _chain_enabled(cfg))
        if action is None:
            continue
        if not dedup_ok(wi_id, level, now):          # remind/escalate/force ≤1/天(E-dedup)
            continue
        try:
            res = execute_stale_action(wi_id, wi_dir, state, action, level, cfg, now)
        except Exception as e:  # noqa: BLE001
            res = {"result": "error", "detail": f"{type(e).__name__}: {e}"}
        try:
            append_stale_audit(wi_id, state, age_s, level, action, res, now=now)
        except (StoreError, OSError) as e:
            print(f"告警: stale audit 写入失败: {e}", file=sys.stderr)  # E12
        results.append(res)
    return results


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

SUBCOMMANDS = {
    "add": cmd_add, "status": cmd_status, "list": cmd_list,
    "log": cmd_log, "run": cmd_run, "reconcile": cmd_reconcile,
    "pump": cmd_pump, "reschedule": cmd_reschedule, "snapshot": cmd_snapshot,
    "wake-text": cmd_wake_text, "prune": cmd_prune,
}


def main(argv):
    if argv and argv[0] == "_runner":  # 内部隐藏入口,仅由 spawn_runner 调用
        return runner_main(argv[1:])
    if not argv or argv[0] in ("-h", "--help", "help"):
        sys.stdout.write(USAGE)
        return 0 if argv else 2
    if argv[0] != "task":
        sys.stderr.write(USAGE)
        return 2
    rest = argv[1:]
    if not rest or rest[0] in ("-h", "--help", "help"):
        sys.stdout.write(USAGE)
        return 0 if rest else 2
    sub = rest[0]
    fn = SUBCOMMANDS.get(sub)
    if fn is None:
        sys.stderr.write(f"错误: 未知子命令 {sub}\n")
        return 2
    json_mode = "--json" in rest[1:]
    try:
        cfg = load_task_config()
    except StoreError as e:
        return fail(2, f"配置解析失败: {e}", None, json_mode)
    except OSError as e:
        return fail(2, f"配置不可用: {e}", None, json_mode)
    try:
        return fn(rest[1:], cfg)
    except UsageError as e:
        return fail(2, str(e), None, json_mode)
    except StoreError as e:
        return fail(2, str(e), None, json_mode)
    except OSError as e:
        return fail(1, f"写盘失败: {e}", None, json_mode)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
