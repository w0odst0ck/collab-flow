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
import shutil
import subprocess
import sys
import threading
from datetime import datetime, timezone

try:
    import fcntl
except ImportError:  # 非 POSIX 降级(§1.6.6)
    fcntl = None

# ---------------------------------------------------------------------------
# 复用 flow-core.py 的纯工具(只读依赖,不改动该文件;解析/输出口径完全一致)
# ---------------------------------------------------------------------------

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
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
TASK_STATES = ("queued", "running", "done", "failed", "timeout")
TERMINAL_STATES = ("done", "failed", "timeout")
PRUNE_STATES = ("done", "failed", "timeout", "killed")  # prune --state 白名单(killed 预留,无匹配幂等)
KINDS = ("design", "execute")                  # M2:任务类型(决定事件/种子/partial-complete 语义)
PRIORITIES = ("P0", "P1", "P2")
PRIO_ORDER = {"P0": 0, "P1": 1, "P2": 2}
SCHEMA_VERSION = 1
RECONCILE_GRACE_S = 60   # pid=None(dispatch 提升后 runner 尚未写 pid 的窗口)的回收宽限秒数

USAGE = """用法: flow task <sub> ...
  flow task add       --command CMD [--workitem W] [--priority P0|P1|P2]
                      [--expected-seconds N] [--kill-on-timeout]
                      [--kind design|execute] [--workdir DIR] [--json]
  flow task status    <id> [--json]
  flow task list      [--state S] [--workitem W] [--json]
  flow task log       <id> [--tail N] [--json]
  flow task run       [--max-parallel N] [--json]
  flow task reconcile [--json]
  flow task wake-text <id> [--json]
  flow task prune     [--state done|failed|timeout|killed] [--older-than N]
                      [--force] [--json]
退出码: 0 成功 / 1 运行失败 / 2 用法或前置错误(含幂等拒绝)/
        124 仅任务命令语义(不进 CLI 顶层)。
"""


class UsageError(Exception):
    """用法/前置错误(→ exit 2)。"""


class TaskError(Exception):
    """任务不存在(→ exit 2)。"""


class DuplicateWorkitem(Exception):
    """同 workitem 已有非终态任务(幂等拒绝,→ exit 2)。args=(已有 id, state)。"""


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
    """纯函数:空闲槽数 = max_parallel − running;返回应提升的任务列表(不改 reg)。"""
    running = sum(1 for t in reg["tasks"].values() if t.get("state") == "running")
    free = max(int(max_parallel) - running, 0)
    return sort_queue(reg["tasks"].values())[:free]


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


def run_command(t, cfg):
    """执行任务命令(§1.6.1):expected_seconds 用 timeout(coreutils)包裹;无该二进制走 Python 降级。"""
    expected = t.get("expected_seconds")
    if expected:
        bin_ = shutil.which("timeout")
        if bin_:
            # design §1.6.1：超时优雅 TERM 终止（默认）；--kill-on-timeout 追加 KILL 兜底
            base = [bin_, "--signal=TERM"]
            if t.get("kill_on_timeout"):
                base += ["--kill-after", str(cfg["task"].get("kill_grace_s", 5))]
            base += [str(expected), "sh", "-c", t["command"]]
            proc = subprocess.run(base)
            rc = proc.returncode
            # kill-on-timeout 且 SIGTERM 被忽略 → --kill-after 触发 SIGKILL,timeout 报
            # 128+SIGKILL=137(而非 124);统一为超时语义,保证「超时→timeout」终态不削弱。
            if t.get("kill_on_timeout") and rc == 137:
                return 124
            return rc
        return _run_with_py_timeout(t)  # 降级:超时 SIGKILL(≈kill-on-timeout)
    proc = subprocess.run(["sh", "-c", t["command"]])
    return proc.returncode


def _run_with_py_timeout(t):
    """无 timeout 二进制降级(§1.6.6):subprocess.run(timeout=...) → 超时抛 CommandTimeout。"""
    try:
        proc = subprocess.run(["sh", "-c", t["command"]], timeout=t["expected_seconds"])
        return proc.returncode
    except subprocess.TimeoutExpired:
        raise CommandTimeout()


def spawn_runner(tid):
    """分离式 spawn 落账 runner(§1.4(2) 锁外):stdout/stderr → logs/<id>.log,setssid 孤儿。"""
    os.makedirs(logs_dir(), exist_ok=True)
    log_fd = open(log_path(tid), "ab")
    try:
        popen_kw = {}
        if os.name == "posix":
            popen_kw["start_new_session"] = True
        subprocess.Popen(
            [sys.executable, os.path.abspath(__file__), "_runner", tid],
            stdin=subprocess.DEVNULL, stdout=log_fd, stderr=subprocess.STDOUT,
            env=dict(os.environ), close_fds=True, **popen_kw)
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
             expected_seconds=None, kill_on_timeout=False, kind=None, workdir=None):
    """幂等入队(§1.4(1)):flock 内去重(同 workitem 非终态 → DuplicateWorkitem) + 追加 + 自动 dispatch。

    kind 非空时:缺省 expected_seconds → 种子回退(config 默认);注册表条目带 kind 字段。
    workdir 默认 os.getcwd(),仅作跨项目归属记录(§1.2),不改变 runner 执行 cwd;
    目录不存在也接受字符串不校验。
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
    if DENY_RE.search(command):
        raise StoreError("command 含疑似 secret, 拒绝写入")

    def _do():
        reg = load_registry()
        if workitem is not None:
            for t in reg["tasks"].values():
                if t.get("workitem") == workitem and t["state"] in ("queued", "running"):
                    raise DuplicateWorkitem(t["id"], t["state"])
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
            "priority": priority, "state": "queued", "kind": kind,
            "workdir": workdir or os.getcwd(), "expected_seconds": exp,
            "kill_on_timeout": bool(kill_on_timeout),
            "created_at": _now_ms(), "started_at": None, "finished_at": None,
            "exit_code": None, "failure_tail": None, "pid": None, "heartbeat_at": None,
        }
        save_registry_atomic(reg)
        # queued 事件在注册表锁内写:保证先于任何并发 dispatch 的 running 事件落盘(事件流不倒挂)
        _emit_task_event_best_effort(tid, {
            "schema_version": EVENT_SCHEMA_VERSION, "ts": now_iso(),
            "task_id": tid, "workitem": workitem, "kind": kind,
            "state": "queued", "exit_code": None, "duration_s": None,
            "expected_seconds": exp, "started_at": None, "finished_at": None,
            "diagnostic": None, "partial_complete": False,
        })
        return tid, reg["tasks"][tid]

    tid, entry = with_registry_flock(_do)
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
    try:
        dispatch(cfg)  # 释放槽位 → 触发下一批(自续队列)
    except (StoreError, OSError):
        pass
    return 0


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
        args, {"command", "workitem", "priority", "expected-seconds", "kind", "workdir"})
    json_mode = bool(opts.get("json"))
    if pos:
        return fail(2, "add 不接受位置参数", None, json_mode)
    command = opts.get("command")
    if command is None or command.strip() == "":
        return fail(2, "missing command", "add 需要 --command CMD", json_mode)
    workitem = opts.get("workitem")
    workdir = opts.get("workdir")  # 可选;默认 os.getcwd()(add_task 内),不校验目录存在(§3 错误表 #2)
    priority = opts.get("priority") or str(cfg["task"].get("default_priority", "P2"))
    kind = opts.get("kind")
    expected_raw = opts.get("expected-seconds")
    expected = None
    if expected_raw is not None:
        try:
            expected = int(expected_raw)
        except (TypeError, ValueError):
            return fail(2, "invalid expected-seconds", expected_raw, json_mode)
        if expected <= 0:
            return fail(2, "invalid expected-seconds", "必须为正整数", json_mode)
    try:
        tid = add_task(cfg, command, workitem=workitem, priority=priority,
                       expected_seconds=expected, kind=kind,
                       kill_on_timeout=bool(opts.get("kill-on-timeout")),
                       workdir=workdir)
    except DuplicateWorkitem as e:
        return fail(2, "duplicate_workitem", f"已有非终态任务 {e.args[0]} ({e.args[1]})", json_mode)
    except UsageError as e:
        return fail(2, str(e), None, json_mode)
    except StoreError as e:
        return fail(2, str(e), None, json_mode)
    if json_mode:
        exp = expected
        if exp is None and kind is not None:  # 种子回退的实际落盘值(供入队方透传)
            try:
                entry = load_registry()["tasks"].get(tid)
                if entry is not None:
                    exp = entry.get("expected_seconds")
            except (StoreError, KeyError, TypeError):
                pass
        emit({"status": "ok", "id": tid, "state": "queued",  # 入队确认(§1.5 契约)
              "workitem": workitem, "priority": priority, "kind": kind,
              "workdir": workdir or os.getcwd(),
              "expected_seconds": exp})
    else:
        print(f"task {tid}: queued")
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
        return fail(2, "run 不接受位置参数", None, json_mode)
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
        if st in ("queued", "running"):  # 红线:非终态绝不删(双保险)
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
