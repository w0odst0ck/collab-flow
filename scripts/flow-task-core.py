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
from datetime import datetime, timedelta, timezone

try:
    import fcntl
except ImportError:  # 非 POSIX 降级(§1.6.6)
    fcntl = None

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

USAGE = """用法: flow task <sub> ...
  flow task add       --command CMD --kind design|execute
                      [--workitem W] --priority P0|P1|P2 --expected-seconds N
                      --why REASON [--at ISO|HH:MM] [--kill-on-timeout]
                      [--workdir DIR] [--force --force-reason R] [--json]
  flow task status    <id> [--json]
  flow task list      [--state S] [--workitem W] [--json]
  flow task log       <id> [--tail N] [--json]
  flow task run       [<id>] [--max-parallel N] [--json]
  flow task pump      [--json]
  flow task reschedule <id> --at ISO|HH:MM [--json]
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


def plan_dispatch(reg, max_parallel):
    """纯函数:空闲槽数 = max_parallel − running;返回应提升的任务列表(不改 reg)。
    只取 queued(绝不碰 scheduled;与 plan_pump 源分离)。"""
    running = sum(1 for t in reg["tasks"].values() if t.get("state") == "running")
    free = max(int(max_parallel) - running, 0)
    return sort_queue(reg["tasks"].values())[:free]


def plan_pump(reg, max_parallel, now):
    """纯函数(pump,flow-cost-ledger §1.5(2)):到期 scheduled 排序 P0<P1<P2 +
    scheduled_at 升序 + id 字典序;只取 scheduled(绝不碰 queued)。P0 无窗口判断,仅靠排序置前。

    空槽 = max_parallel − running;无窗口硬门控(D4):到点即升,槽满留 scheduled 下轮再试。
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
    running = sum(1 for t in reg["tasks"].values() if t.get("state") == "running")
    return due[: max(int(max_parallel) - running, 0)]


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
        proc = subprocess.run(["bash", "-n", "-c", command],
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
    return None


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
    user_path = os.environ.get("COLLABFLOW_CONFIG") or os.path.expanduser(
        "~/.config/collabflow/config.yaml")
    if os.path.isfile(user_path):
        user = _fc.parse_yaml(_fc.read_file(user_path), user_path)
        task = _merge(task, dict(user.get("task") or {}))
        host = _merge(host, dict(user.get("host") or {}))
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
    return {"task": task, "host": host}


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
    return max(int(fallback), int(math.ceil(ema * 1.5)))


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
            proc = subprocess.run(base, env=env)
            rc = proc.returncode
            # kill-on-timeout 且 SIGTERM 被忽略 → --kill-after 触发 SIGKILL,timeout 报
            # 128+SIGKILL=137(而非 124);统一为超时语义,保证「超时→timeout」终态不削弱。
            if t.get("kill_on_timeout") and rc == 137:
                return 124
            return rc
        return _run_with_py_timeout(t)  # 降级:超时 SIGKILL(≈kill-on-timeout)
    proc = subprocess.run(["sh", "-c", t["command"]], env=env)
    return proc.returncode


def _run_with_py_timeout(t):
    """无 timeout 二进制降级(§1.6.6):subprocess.run(timeout=...) → 超时抛 CommandTimeout。"""
    try:
        proc = subprocess.run(["sh", "-c", t["command"]], timeout=t["expected_seconds"],
                              env=_runner_env(t))
        return proc.returncode
    except subprocess.TimeoutExpired:
        raise CommandTimeout()


def spawn_runner(tid):
    """分离式 spawn 落账 runner(§1.4(2) 锁外):stdout/stderr → logs/<id>.log,setssid 孤儿。
    子进程 env 注入任务 workdir 的 FLOW_DATA_DIR/FLOW_WORKDIR(任务书 §4:跨仓 workitem 解析);
    entry 读取失败/缺失 → 回退环境默认(不阻断 spawn)。"""
    os.makedirs(logs_dir(), exist_ok=True)
    log_fd = open(log_path(tid), "ab")
    try:
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
    finally:
        log_fd.close()


def dispatch(cfg, max_parallel=None):
    """调度(§1.4(2)):flock 内 reconcile → plan_dispatch(优先级+空闲槽) → 提升保存;锁外写 running 事件 + spawn。"""
    m = max_parallel if max_parallel is not None else int(cfg["task"]["max_parallel"])

    def _do():
        reg = load_registry()
        reconcile_running(reg)
        promoted = plan_dispatch(reg, m)
        now = _now_ms()
        for t in promoted:
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
             why=None, scheduled_at=None, force=False, force_reason=None):
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
            "kind": kind,
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

    tid, entry = with_registry_flock(_do)
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
        # ── M2:注册表终态提交后,锁外 best-effort(事件/种子/notify,注册表是唯一权威) ──
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
            _notify_if_configured(cfg, event_record)
        except (StoreError, OSError) as e:
            print(f"告警: 终态事件/种子处理失败 {task_id}: {e}", file=sys.stderr)
        # flow-cost-ledger §1.5(4):终态提交后锁外 best-effort 成本落账(失败留 null,不阻断流转)
        _settle_cost(task_id, t, finished_at)
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
               "at", "why", "force-reason"})
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
                       expected_seconds=expected, kind=kind,
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
              "workdir": workdir or os.getcwd(),
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
                chosen = plan_pump(reg, m, datetime.now(timezone.utc))
                now = _now_ms()
                for t in chosen:
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
        t = with_registry_flock(_do)
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
    """删除终态任务;返回被删 id 列表(§1.3)。连带删 logs/<tid>.log(§5.3,幂等)。"""
    removed = _prune_match(reg, states, older_than_days, now)
    for tid in removed:
        del reg["tasks"][tid]
        _remove_log_best_effort(tid)
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
    有副作用:删 registry 条目 + best-effort 连带删 logs/<id>.log;返回被删 id 列表。
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
    """终态通知钩子(§1.5):host.notify 非空才调;事件 JSON 走 stdin(不落 argv);
    模板含控制字符 → 拒绝执行(fail-closed);失败仅告警不影响终态。"""
    host = cfg.get("host") or {}
    template = host.get("notify")
    if not template or not isinstance(template, str):
        return
    if re.search(r"[\x00-\x1f]", template):
        print("告警: host.notify 含控制字符, 拒绝执行", file=sys.stderr)
        return
    try:
        proc = subprocess.run(["sh", "-c", template],
                              input=json.dumps(event_record, ensure_ascii=False),
                              capture_output=True, text=True)
        if proc.returncode != 0:
            print(f"告警: notify 命令 exit={proc.returncode}", file=sys.stderr)
    except OSError as e:
        print(f"告警: notify 调用失败: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

SUBCOMMANDS = {
    "add": cmd_add, "status": cmd_status, "list": cmd_list,
    "log": cmd_log, "run": cmd_run, "reconcile": cmd_reconcile,
    "pump": cmd_pump, "reschedule": cmd_reschedule,
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
