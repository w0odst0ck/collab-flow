#!/usr/bin/env python3
"""flow-core.py —— collab-flow work-item 状态机引擎核心(P2)。

职责(对应设计方案 20260814-134840-956887393-P2-workitem.md):
  1. 受限 YAML 子集解析/序列化(§1.7)—— 支持「映射 + list-of-mapping + 标量」,
     零依赖,显式拒绝锚点/标签/块/流式集合/tab 缩进等歧义语法;
  2. 状态机纯函数(§2)—— TRANSITIONS + transition() + evaluate_guard(),零文件 I/O;
  3. store 层(§4)—— 原子写(temp+fsync+rename) + events.jsonl flock 追加(seq=行数+1
     在同一临界区) + 双层锁(本地 flock + 逻辑锁 TTL 过期自动释放);
  4. 命令实现(§3)—— new/status/transition/list/log/lock/unlock/show/guard/verify/decision。

用法: flow-core.py workitem <sub> ...
退出码: 0 成功 / 1 运行失败(守卫拒绝、写盘失败) / 2 用法或前置错误 /
        3 async 设计运行中(design --check,新增,纯增量) / 124 超时。

红线: 本文件零个人标识(actor/owner 用 flow:<plane_id>,不取 hostname/用户名);
      id 白名单正则硬编码;meta 写入前过 deny-list;transition()/evaluate_guard() 零 I/O。
"""

import fnmatch
import json
import os
import re
import secrets
import shlex
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone

try:
    import fcntl
except ImportError:  # 非 POSIX 降级(§4.2)
    fcntl = None

# ---------------------------------------------------------------------------
# 常量(§1.2/§2.1)
# ---------------------------------------------------------------------------

ID_RE = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")   # id 白名单,硬编码不进 config
DENY_RE = re.compile(r"sk-[A-Za-z0-9]{16,}")       # meta 疑似 secret(§1.4;≥16 字符,防 workitem 名误报)

STATES = (
    "created", "designed", "reviewed", "translated",
    "executed", "verified", "accepted", "retrospected",
)
EVENTS = (
    "create", "design", "review", "translate", "feedback", "takeover",
    "execute", "verify", "verify_fail", "accept", "retro",
    "lock", "unlock",  # 非转移事件(仅入日志不改状态)
)

DEFECT_TYPES = (
    "constraint_violation", "missing_scenario", "untestable_acceptance",
    "out_of_scope", "untranslatable",
)
VERDICTS = ("pass", "reject", "takeover")
ROUTES = ("design", "impl")


class UsageError(Exception):
    """用法/前置错误(→ exit 2)。"""


class WorkitemError(Exception):
    """work-item 不存在 / status 缺失(→ exit 2)。"""


class Locked(Exception):
    """workitem 锁被其他持有者占用。"""


class InFlight(Exception):
    """已有进行中的 async 设计（锁内复查命中，TOCTOU 防御）。"""
    """锁被他人持有(→ exit 2)。"""


class StoreError(Exception):
    """写盘/解析失败(→ exit 1 或 2)。"""


# ---------------------------------------------------------------------------
# 受限 YAML 解析(§1.7:映射 + list-of-mapping + 标量)
# ---------------------------------------------------------------------------

def leading_spaces(line):
    return len(line) - len(line.lstrip(" "))


def split_comment(s):
    """剥「值后空格 + #」尾注释:# 前必须是空白且不在引号内。"""
    in_s = in_d = False
    for i, ch in enumerate(s):
        if ch == "'" and not in_d:
            in_s = not in_s
        elif ch == '"' and not in_s:
            in_d = not in_d
        elif ch == "#" and not in_s and not in_d and (i == 0 or s[i - 1] in " \t"):
            return s[:i]
    return s


def find_key_colon(content):
    """找第一个不在引号内的 ':'(键值分隔符);无则返回 None。"""
    in_s = in_d = False
    for i, ch in enumerate(content):
        if ch == "'" and not in_d:
            in_s = not in_s
        elif ch == '"' and not in_s:
            in_d = not in_d
        elif ch == ":" and not in_s and not in_d:
            return i
    return None


def check_rejected(content, src, lineno):
    """显式拒绝受限子集之外的 YAML 语法(fail-closed)。"""
    for ch in ("&", "*", "|", ">"):
        if ch in content:
            raise StoreError(f"{src}:{lineno}: 不支持的 YAML 语法 '{ch}'")
    if "!!" in content:
        raise StoreError(f"{src}:{lineno}: 不支持的 YAML 标签 '!!'")
    i = 0
    n = len(content)
    while i < n:
        ch = content[i]
        if ch == "$" and i + 1 < n and content[i + 1] == "{":
            j = content.find("}", i + 2)
            if j == -1:
                raise StoreError(f"{src}:{lineno}: 未闭合的环境引用 '${{'")
            i = j + 1
            continue
        if ch in ("{", "}", "[", "]"):
            raise StoreError(f"{src}:{lineno}: 不支持的 YAML 语法 '{ch}'(流式集合)")
        i += 1


def parse_scalar(raw, src, lineno):
    """受限标量:null/true/false/int/float/引号字符串/裸字符串。"""
    s = raw.strip()
    if s == "~" or s == "null":
        return None
    if s == "true":
        return True
    if s == "false":
        return False
    if re.fullmatch(r"-?\d+", s):
        return int(s)
    if re.fullmatch(r"-?\d+\.\d+([eE][+-]?\d+)?", s):
        return float(s)
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        body = s[1:-1]
        return body.replace('\\"', '"').replace("\\\\", "\\")
    if len(s) >= 2 and s[0] == "'" and s[-1] == "'":
        return s[1:-1].replace("''", "'")
    return s


def _validate_key(key, node, src, lineno):
    if not re.fullmatch(r"[A-Za-z0-9._-]+", key):
        raise StoreError(f"{src}:{lineno}: 非法键名 '{key}'")
    if isinstance(node, dict) and key in node:
        raise StoreError(f"{src}:{lineno}: 重复键 '{key}'")


def _parse_map(lines, idx, indent, src):
    """解析缩进映射块;返回 (node, 下一行下标)。"""
    n = len(lines)
    node = {}
    while idx < n:
        raw = lines[idx]
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            idx += 1
            continue
        cur = leading_spaces(raw)
        if "\t" in raw[:cur]:
            raise StoreError(f"{src}:{idx + 1}: 不支持 tab 缩进(统一空格)")
        if cur < indent:
            break
        if cur > indent:
            raise StoreError(f"{src}:{idx + 1}: 意外缩进(层级跳变)")
        content = raw[cur:]
        if content.startswith("-"):
            break  # 交给父级 list 处理
        check_rejected(content, src, idx + 1)
        colon = find_key_colon(content)
        if colon is None:
            raise StoreError(f"{src}:{idx + 1}: 缺少 ':'(非映射行)")
        key = content[:colon].strip()
        _validate_key(key, node, src, idx + 1)
        rest = split_comment(content[colon + 1:]).strip()
        if rest == "":
            idx += 1
            if idx < n and leading_spaces(lines[idx]) > cur:
                child, idx = _parse_block(lines, idx, leading_spaces(lines[idx]), src)
                node[key] = child
            else:
                node[key] = None
        else:
            check_rejected(rest, src, idx + 1)
            node[key] = parse_scalar(rest, src, idx + 1)
            idx += 1
    return node, idx


def _parse_list_item(lines, idx, dash_indent, src, first):
    """解析 `- key: value` 内联 mapping 元素;first 为 `- ` 后的首行内容。"""
    n = len(lines)
    node = {}
    colon = find_key_colon(first)
    if colon is None:
        raise StoreError(f"{src}:{idx + 1}: 序列项需为 mapping('- key: value')")
    key = first[:colon].strip()
    _validate_key(key, node, src, idx + 1)
    rest = split_comment(first[colon + 1:]).strip()
    if rest == "":
        raise StoreError(f"{src}:{idx + 1}: 序列项 mapping 值不可为空")
    node[key] = parse_scalar(rest, src, idx + 1)
    idx += 1
    field_indent = dash_indent + 2
    while idx < n:
        raw = lines[idx]
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            idx += 1
            continue
        c = leading_spaces(raw)
        if c <= dash_indent:
            break
        if c != field_indent:
            raise StoreError(f"{src}:{idx + 1}: 意外缩进(序列项字段需对齐)")
        content = raw[c:]
        check_rejected(content, src, idx + 1)
        if content.startswith("-"):
            break
        colon = find_key_colon(content)
        if colon is None:
            raise StoreError(f"{src}:{idx + 1}: 缺少 ':'(序列项字段)")
        key = content[:colon].strip()
        _validate_key(key, node, src, idx + 1)
        rest = split_comment(content[colon + 1:]).strip()
        node[key] = parse_scalar(rest, src, idx + 1)
        idx += 1
    return node, idx


def _parse_list(lines, idx, indent, src):
    """解析 list-of-mapping 或 list-of-scalar;返回 (node, 下一行下标)。"""
    n = len(lines)
    items = []
    while idx < n:
        raw = lines[idx]
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            idx += 1
            continue
        cur = leading_spaces(raw)
        if "\t" in raw[:cur]:
            raise StoreError(f"{src}:{idx + 1}: 不支持 tab 缩进(统一空格)")
        if cur < indent:
            break
        if cur > indent:
            raise StoreError(f"{src}:{idx + 1}: 意外缩进(层级跳变)")
        content = raw[cur:]
        if not content.startswith("-"):
            break
        rest = content[1:]
        if rest == "" or rest == " ":
            raise StoreError(f"{src}:{idx + 1}: 序列项需为 mapping('- key: value') 或标量('- value')")
        if not rest.startswith(" "):
            raise StoreError(f"{src}:{idx + 1}: 序列项需为 mapping('- key: value') 或标量('- value')")
        check_rejected(content, src, idx + 1)
        item_content = rest[1:]
        if find_key_colon(item_content) is None:
            # list-of-scalar(向后兼容扩展,P3):`- 标量`(§4.1 diff_scope allow/deny)
            items.append(parse_scalar(split_comment(item_content).strip(), src, idx + 1))
            idx += 1
        else:
            item, idx = _parse_list_item(lines, idx, cur, src, item_content)
            items.append(item)
    return items, idx


def _parse_block(lines, idx, indent, src):
    """解析缩进块(映射或序列);返回 (node, 下一行下标)。"""
    n = len(lines)
    while idx < n:
        raw = lines[idx]
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            idx += 1
            continue
        break
    if idx >= n:
        return None, idx
    raw = lines[idx]
    cur = leading_spaces(raw)
    if cur < indent:
        return None, idx
    if cur > indent:
        raise StoreError(f"{src}:{idx + 1}: 意外缩进(层级跳变)")
    content = raw[cur:]
    check_rejected(content, src, idx + 1)
    if content.startswith("-"):
        return _parse_list(lines, idx, indent, src)
    return _parse_map(lines, idx, indent, src)


def parse_yaml(text, src):
    """入口:文档指令检查 + 顶层块解析。"""
    lines = text.split("\n")
    if lines and lines[0].startswith("\ufeff"):
        lines[0] = lines[0][1:]
    for i, ln in enumerate(lines):
        ls = ln.lstrip()
        if ls.startswith("%"):
            raise StoreError(f"{src}:{i + 1}: 不支持的文档指令 '%'")
        if ls.startswith("---") or ls.startswith("..."):
            raise StoreError(f"{src}:{i + 1}: 不支持的文档分隔符")
    node, idx = _parse_block(lines, 0, 0, src)
    for j in range(idx, len(lines)):
        if lines[j].strip():
            raise StoreError(f"{src}:{j + 1}: 意外内容(缩进或结构错误)")
    return node


# ---------------------------------------------------------------------------
# YAML 序列化(§1.7:键排序 + 确定性输出)
# ---------------------------------------------------------------------------

_BARE_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_./+-]*$")
_RESERVED = {"null", "true", "false", "~"}


def _dump_scalar(v):
    if v is None:
        return "null"
    if v is True:
        return "true"
    if v is False:
        return "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, str):
        if v == "":
            return '""'
        if v.lower() in _RESERVED:
            return json.dumps(v, ensure_ascii=False)
        if _BARE_RE.fullmatch(v):
            if re.fullmatch(r"-?\d+", v) or re.fullmatch(r"-?\d+\.\d+([eE][+-]?\d+)?", v):
                return json.dumps(v, ensure_ascii=False)
            return v
        return json.dumps(v, ensure_ascii=False)
    return json.dumps(v, ensure_ascii=False)


def _dump_map_entry(key, v, out, indent):
    pad = " " * indent
    if isinstance(v, dict) and v:
        out.append(f"{pad}{key}:\n")
        _dump_node(v, out, indent + 2)
    elif isinstance(v, list) and v:
        out.append(f"{pad}{key}:\n")
        _dump_node(v, out, indent + 2)
    elif isinstance(v, dict) or isinstance(v, list):
        out.append(f"{pad}{key}: []\n")  # 空集合(P2 输出不产生,防御性)
    else:
        out.append(f"{pad}{key}: {_dump_scalar(v)}\n")


def _dump_node(node, out, indent):
    if isinstance(node, dict):
        for k in sorted(node.keys()):
            _dump_map_entry(k, node[k], out, indent)
    elif isinstance(node, list):
        pad = " " * indent
        for item in node:
            if isinstance(item, dict):
                keys = sorted(item.keys())
                for j, k in enumerate(keys):
                    prefix = "- " if j == 0 else "  "
                    if isinstance(item[k], dict) and item[k]:
                        out.append(f"{pad}{prefix}{k}:\n")
                        _dump_node(item[k], out, indent + 4)
                    else:
                        out.append(f"{pad}{prefix}{k}: {_dump_scalar(item[k])}\n")
            else:
                out.append(f"{pad}- {_dump_scalar(item)}\n")
    else:
        out.append(" " * indent + _dump_scalar(node) + "\n")


def dump_yaml(node):
    out = []
    _dump_node(node, out, 0)
    return "".join(out)


# ---------------------------------------------------------------------------
# 状态机纯函数(§2,零 I/O)
# ---------------------------------------------------------------------------

# TRANSITIONS: (from_state, event) -> (to, guard, effects_key)
TRANSITIONS = {
    (None, "create"):          ("created", None, "init"),
    ("created", "design"):     ("designed", "brief_required", None),
    ("designed", "review"):    ("reviewed", "design_required", None),
    ("reviewed", "translate"): ("translated", "quality_pass", "pass"),
    ("reviewed", "feedback"):  ("designed", "quality_reject_retry", "feedback"),
    ("reviewed", "takeover"):  ("translated", "quality_reject_takeover", "takeover"),
    ("translated", "execute"): ("executed", "taskbook_required", None),
    ("executed", "verify"):    ("verified", "test_gate_pass", None),
    ("verified", "accept"):    ("accepted", "approve_confirmed", None),
    ("accepted", "retro"):     ("retrospected", None, None),
}

# verify_fail 双目标(§2.2 #8/#9):route 决定 to 与 guard
VERIFY_FAIL = {
    "design": ("designed", "test_gate_fail_design", "feedback_design"),
    "impl": ("executed", "test_gate_fail_impl", "feedback_impl"),
}

# 每个事件守卫所需的最小 ctx 键(缺失 → bad_guard_input)
EVENT_REQUIRED_CTX = {
    "design": ("brief_nonempty",),
    "review": ("design_nonempty", "decision_present"),
    "translate": ("verdict",),
    "feedback": ("verdict", "same_defect_count", "takeover_after_same_defect"),
    "takeover": ("verdict", "same_defect_count", "takeover_after_same_defect"),
    "execute": ("taskbook_nonempty",),
    "verify": ("tests_pass", "diff_match", "error_table_match"),
    "verify_fail": ("tests_pass", "diff_match", "error_table_match", "route"),
    "accept": ("approve_confirmed",),
}


def _all3(c):
    return bool(c.get("tests_pass")) and bool(c.get("diff_match")) and bool(c.get("error_table_match"))


# 守卫注册表(§5.1)
GUARDS = {
    "brief_required":         lambda c: (bool(c.get("brief_nonempty")), "brief.md 为空或缺失"),
    "design_required":        lambda c: (bool(c.get("design_nonempty")) and bool(c.get("decision_present")),
                                         "design.md 或 decision.yaml 缺失"),
    "quality_pass":           lambda c: (c.get("verdict") == "pass", f"verdict={c.get('verdict')}"),
    "quality_reject_retry":   lambda c: (c.get("verdict") == "reject"
                                         and c.get("same_defect_count", 0) < c.get("takeover_after_same_defect", 2),
                                         "reject 未达接管阈值或非 reject"),
    "quality_reject_takeover": lambda c: (c.get("verdict") == "reject"
                                          and c.get("same_defect_count", 0) >= c.get("takeover_after_same_defect", 2),
                                          "reject 未达接管阈值"),
    "taskbook_required":      lambda c: (bool(c.get("taskbook_nonempty")), "taskbook.md 为空或缺失"),
    "test_gate_pass":         lambda c: (_all3(c), "测试门三条件未全真"),
    "test_gate_fail_design":  lambda c: (not _all3(c) and c.get("route") == "design",
                                         "route 非 design 或测试门通过"),
    "test_gate_fail_impl":    lambda c: (not _all3(c) and c.get("route") == "impl",
                                         "route 非 impl 或测试门通过"),
    "approve_confirmed":      lambda c: (bool(c.get("approve_confirmed")), "缺少 approver 确认"),
}


def evaluate_guard(name, ctx):
    fn = GUARDS.get(name)
    if fn is None:
        return (False, f"未知守卫: {name}")
    ok, reason = fn(ctx)
    return (bool(ok), reason)


def compute_effects(key, ctx):
    """effects 为 status 字段变更:值 '+1' 表示增量 +1,其余为绝对设置(§2.2)。"""
    if key is None:
        return {}
    if key == "init":
        return {"iteration": 0, "same_defect_count": 0, "primary_defect_type": None,
                "takeover": False, "re_execute_count": 0, "event_seq": 1}
    if key == "pass":
        return {"same_defect_count": 0}
    if key == "feedback":
        old_count = ctx.get("same_defect_count", 0)
        old_type = ctx.get("primary_defect_type")
        new_type = ctx.get("defect_type")
        new_count = (old_count + 1) if (new_type is not None and new_type == old_type) else 1
        return {"iteration": "+1", "same_defect_count": new_count, "primary_defect_type": new_type}
    if key == "takeover":
        return {"takeover": True, "same_defect_count": 0}
    if key == "feedback_design":
        return {"iteration": "+1"}
    if key == "feedback_impl":
        return {"re_execute_count": "+1"}
    return {}


def transition(current_state, event, ctx, force=False):
    """状态机核心纯函数(§2.2):零 I/O,输入经 ctx 显式传入。"""
    if event not in EVENTS:
        return {"ok": False, "reason": "unknown_event", "detail": f"未知事件: {event}"}
    if current_state is not None and current_state not in STATES:
        return {"ok": False, "reason": "unknown_state", "detail": f"未知状态: {current_state}"}

    if event == "verify_fail":
        route = ctx.get("route")
        if route not in ROUTES:
            return {"ok": False, "reason": "guard_failed:test_gate_fail_design",
                    "detail": f"route 非 design/impl: {route!r}"}
        to, guard, effect_key = VERIFY_FAIL[route]
    else:
        entry = TRANSITIONS.get((current_state, event))
        if entry is None:
            return {"ok": False, "reason": "illegal_transition",
                    "detail": f"非法转移: {current_state} → {event}"}
        to, guard, effect_key = entry

    if not force:
        required = EVENT_REQUIRED_CTX.get(event, ())
        missing = [k for k in required if k not in ctx]
        if missing:
            return {"ok": False, "reason": "bad_guard_input",
                    "detail": f"缺少守卫输入: {','.join(missing)}"}
        if guard is not None:
            ok, reason = evaluate_guard(guard, ctx)
            if not ok:
                return {"ok": False, "reason": f"guard_failed:{guard}", "detail": reason}

    effects = compute_effects(effect_key, ctx)
    return {"ok": True, "event": event, "from": current_state, "to": to,
            "guard": guard, "effects": effects}


# ---------------------------------------------------------------------------
# store 层(§4)
# ---------------------------------------------------------------------------

def now_dt():
    return datetime.now(timezone.utc)


def now_iso():
    return now_dt().isoformat(timespec="seconds")


def parse_iso(s):
    return datetime.fromisoformat(s)


def read_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def file_nonempty(path):
    return os.path.isfile(path) and os.path.getsize(path) > 0


def count_lines(path):
    if not os.path.exists(path):
        return 0
    with open(path, "r", encoding="utf-8") as f:
        return sum(1 for _ in f)


def save_status_atomic(wi_dir, status):
    """原子写(§4.1):temp + fsync + os.replace;读方永不观察半写。"""
    path = os.path.join(wi_dir, "status.yaml")
    tmp = os.path.join(wi_dir, f".status.yaml.tmp.{os.getpid()}.{secrets.token_hex(4)}")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(dump_yaml(status))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def append_event(wi_dir, record):
    """events.jsonl flock 追加(§4.2):seq=追加前行数+1,读取与追加同一临界区。"""
    path = os.path.join(wi_dir, "events.jsonl")
    lock_path = path + ".lock"
    meta_json = json.dumps(record.get("meta", {}), ensure_ascii=False)
    if DENY_RE.search(meta_json):
        raise StoreError("事件 meta 含疑似 secret, 拒绝写入")
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        if fcntl is not None and hasattr(fcntl, "flock"):
            fcntl.flock(fd, fcntl.LOCK_EX)
        seq = count_lines(path) + 1  # 读取行数 + 追加 同一临界区(§4.2)
        record = dict(record)
        record["seq"] = seq
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())
    finally:
        if fcntl is not None and hasattr(fcntl, "flock"):
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
    return seq


def with_workitem_lock(wi_dir, fn):
    """本地互斥(§4.3):flock sidecar 串行化同一 work-item 的读-改-写。"""
    if fcntl is None or not hasattr(fcntl, "flock"):
        return fn()  # 降级:非 POSIX 无 flock(§4.2)
    fd = os.open(os.path.join(wi_dir, ".lock"), os.O_CREAT | os.O_RDWR, 0o644)
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        return fn()
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def is_expired(status, now):
    """逻辑锁过期判定(§4.3):未设或已过期 → 视为空闲。"""
    exp = status.get("lock_expires_at")
    if exp is None:
        return True
    try:
        return parse_iso(exp) <= now
    except (TypeError, ValueError):
        return True


def is_locked(status, now):
    return bool(status.get("locked_by")) and not is_expired(status, now)


def acquire_lock(wi_dir, owner, ttl_s, now):
    """跨设备逻辑锁(§4.3):他人持有未过期 → Locked;否则覆盖并记录 lock 事件。"""
    def _do():
        s = load_status(wi_dir)
        if is_locked(s, now):
            raise Locked(s["locked_by"])
        s["locked_by"] = owner
        s["lock_expires_at"] = (now + timedelta(seconds=ttl_s)).isoformat(timespec="seconds")
        save_status_atomic(wi_dir, s)
        append_event(wi_dir, {"ts": now.isoformat(timespec="seconds"), "event": "lock",
                              "from": s["state"], "to": s["state"], "actor": owner,
                              "guard": None, "reason": None,
                              "meta": {"owner": owner, "ttl_s": ttl_s}})
        return s
    return with_workitem_lock(wi_dir, _do)


def load_status(wi_dir):
    """读 status.yaml;state 非受控词表 → fail-closed(§1.3)。"""
    path = os.path.join(wi_dir, "status.yaml")
    if not os.path.isfile(path):
        raise WorkitemError(f"work-item 缺失 status.yaml: {wi_dir}")
    try:
        status = parse_yaml(read_file(path), path)
    except StoreError:
        raise
    if not isinstance(status, dict):
        raise StoreError(f"{path}: 顶层必须是映射")
    if status.get("state") not in STATES:
        raise StoreError(f"{path}: 非法 state {status.get('state')!r}")
    return status


def read_decision(wi_dir):
    path = os.path.join(wi_dir, "decision.yaml")
    if not os.path.isfile(path):
        return {}
    try:
        d = parse_yaml(read_file(path), path)
    except StoreError:
        raise
    return d if isinstance(d, dict) else {}


def read_verify(wi_dir):
    path = os.path.join(wi_dir, "executor", "verify.json")
    if not os.path.isfile(path):
        return {}
    try:
        v = json.loads(read_file(path))
    except (ValueError, OSError):
        raise StoreError(f"verify.json 解析失败: {path}")
    return v if isinstance(v, dict) else {}


def build_ctx(wi_dir, event, overrides, cfg):
    """ctx 构造(§5.2):store 层读工件 → 纯守卫消费。"""
    c = {"takeover_after_same_defect": cfg["gates"].get("takeover_after_same_defect", 2)}
    if event in ("review", "translate", "feedback", "takeover"):
        d = read_decision(wi_dir)
        c["verdict"] = d.get("verdict")
        c["defect_type"] = d.get("primary_defect_type")
        c["decision_present"] = file_nonempty(os.path.join(wi_dir, "decision.yaml"))
        st = load_status(wi_dir)
        c["same_defect_count"] = st.get("same_defect_count", 0)
        c["primary_defect_type"] = st.get("primary_defect_type")
    if event in ("verify", "verify_fail"):
        v = read_verify(wi_dir)
        c["tests_pass"] = v.get("tests_pass")
        c["diff_match"] = v.get("diff_match")
        c["error_table_match"] = v.get("error_table_match")
        c["route"] = v.get("route")
    if event == "design":
        c["brief_nonempty"] = file_nonempty(os.path.join(wi_dir, "brief.md"))
    if event == "review":
        c["design_nonempty"] = file_nonempty(os.path.join(wi_dir, "design.md"))
    if event == "execute":
        c["taskbook_nonempty"] = file_nonempty(os.path.join(wi_dir, "taskbook.md"))
    c.update(overrides)
    return c


def apply_effects(status, effects):
    for k, v in effects.items():
        if v == "+1":
            status[k] = int(status.get(k, 0)) + 1
        else:
            status[k] = v
    return status


def resolve_event(from_state, to, explicit_event, status, wi_dir, cfg):
    """(from,to)→event 消歧(§3.3)。返回 event 或 None(形状非法)。"""
    if explicit_event:
        return explicit_event
    unique = {
        ("created", "designed"): "design",
        ("designed", "reviewed"): "review",
        ("reviewed", "designed"): "feedback",
        ("translated", "executed"): "execute",
        ("executed", "verified"): "verify",
        ("verified", "accepted"): "accept",
        ("accepted", "retrospected"): "retro",
    }
    if (from_state, to) in unique:
        return unique[(from_state, to)]
    if (from_state, to) == ("reviewed", "translated"):
        d = read_decision(wi_dir)
        t = cfg["gates"].get("takeover_after_same_defect", 2)
        if d.get("verdict") == "reject" and status.get("same_defect_count", 0) >= t:
            return "takeover"
        return "translate"
    if from_state == "executed" and to in ("designed", "executed"):
        return "verify_fail"
    return None


# ---------------------------------------------------------------------------
# config 加载(§7.1:defaults + user + env,只提取 workitem/gates)
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


def load_config():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    defaults_path = os.environ.get("COLLABFLOW_DEFAULTS") or os.path.join(
        script_dir, "..", "config", "defaults.yaml")
    defaults = parse_yaml(read_file(defaults_path), defaults_path)
    merged = dict(defaults)
    user_path = os.environ.get("COLLABFLOW_CONFIG") or os.path.expanduser(
        "~/.config/collabflow/config.yaml")
    if os.path.isfile(user_path):
        merged = _merge(merged, parse_yaml(read_file(user_path), user_path))
    workitem = dict(merged.get("workitem") or {})
    gates = dict(merged.get("gates") or {})
    executor = dict(merged.get("executor") or {})
    task = dict(merged.get("task") or {})  # flow-task-ledger:入队侧补参读取 default_priority/seed

    # env 覆盖(§7.1):环境已设即覆盖
    ttl = _env_int("FLOW_LOCK_TTL_S")
    if ttl is not None:
        workitem["lock_ttl_s"] = ttl
    takeover = _env_int("FLOW_TAKEOVER_AFTER_SAME_DEFECT")
    if takeover is not None:
        gates["takeover_after_same_defect"] = takeover
    limit = _env_int("FLOW_ITERATION_LIMIT")
    if limit is not None:
        gates["iteration_limit"] = limit
    if os.environ.get("FLOW_PLANE_ID"):
        workitem["plane_id"] = os.environ["FLOW_PLANE_ID"]
    if os.environ.get("FLOW_EXECUTOR"):
        executor["default"] = os.environ["FLOW_EXECUTOR"]
    expected_s = _env_int("FLOW_DESIGN_EXPECTED_S")
    if expected_s is not None:
        workitem["design_expected_seconds"] = expected_s
    return {"workitem": workitem, "gates": gates, "executor": executor, "task": task}


def data_dir():
    return os.environ.get("FLOW_DATA_DIR") or os.path.join(os.getcwd(), ".flow")


def workitems_dir():
    return os.path.join(data_dir(), "workitems")


def actor(cfg):
    return f"flow:{cfg['workitem'].get('plane_id', 'control')}"


def resolve_wi_dir(wi_id, cfg):
    if not ID_RE.fullmatch(wi_id):
        raise UsageError(f"非法 id: {wi_id}")
    max_len = int(cfg["workitem"].get("id_max_len", 64))
    if len(wi_id) > max_len:
        raise UsageError(f"id 超长(>{max_len}): {wi_id}")
    wi_dir = os.path.join(workitems_dir(), wi_id)
    if not os.path.isdir(wi_dir) or not os.path.isfile(os.path.join(wi_dir, "status.yaml")):
        raise WorkitemError(f"work-item 不存在: {wi_id}")
    return wi_dir


# ---------------------------------------------------------------------------
# 共享转移辅助(P3):锁内 读→判→transition→append_event→save_status_atomic
# ---------------------------------------------------------------------------

def _do_transition(wi_dir, from_state, to, event, overrides, meta, cfg, force=False):
    """共享转移辅助(§P3 §2.1):复用 cmd_transition 的读-判-转移-落盘路径。

    调用方负责加 with_workitem_lock 与输出/退出码;本函数在锁内完成:
    读 status → 判逻辑锁 → build_ctx → transition() 纯判定 → append_event(seq 镜像)
    → save_status_atomic。from_state 为 None 时不校验前置状态(交由 transition 判定形状)。
    返回 {"ok": True, "from", "to", "event", "guard", "seq"} 或
         {"ok": False, "reason", "detail", "guard"}。
    """
    status = load_status(wi_dir)
    now = now_dt()
    if is_locked(status, now):
        raise Locked(status["locked_by"])
    actual = status["state"]
    if from_state is not None and actual != from_state:
        raise UsageError(f"前置状态不符: 期望 {from_state}, 实际 {actual}")
    ctx = build_ctx(wi_dir, event, overrides, cfg)
    if event == "verify_fail" and ctx.get("route") is None:
        ctx["route"] = "design" if to == "designed" else "impl"
    r = transition(actual, event, ctx, force=force)
    if not r["ok"]:
        return {"ok": False, "reason": r["reason"], "detail": r.get("detail"),
                "guard": r.get("guard")}
    apply_effects(status, r["effects"])
    status["state"] = r["to"]
    status["updated_at"] = now.isoformat(timespec="seconds")
    status["locked_by"] = None
    status["lock_expires_at"] = None
    meta_full = dict(meta or {})
    if force:
        meta_full["forced"] = True
    if event in ("translate", "feedback", "takeover"):
        meta_full["verdict"] = ctx.get("verdict")
    if event == "verify_fail":
        meta_full["route"] = ctx.get("route")
    seq = append_event(wi_dir, {"ts": now.isoformat(timespec="seconds"), "event": event,
                                "from": actual, "to": r["to"], "actor": actor(cfg),
                                "guard": r["guard"], "reason": None, "meta": meta_full})
    status["event_seq"] = seq
    save_status_atomic(wi_dir, status)
    return {"ok": True, "from": actual, "to": r["to"], "event": event,
            "guard": r["guard"], "seq": seq}


def _atomic_write(path, text):
    """原子写(P3):temp + fsync + os.replace,读方永不观察半写。"""
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


def workdir():
    """执行面工作目录(§P3 §2.1):FLOW_WORKDIR 可覆盖,默认 cwd(项目仓根)。"""
    return os.environ.get("FLOW_WORKDIR") or os.getcwd()


# ---------------------------------------------------------------------------
# executor 适配层(P3 §3):load_executor_spec / run_executor
# ---------------------------------------------------------------------------

def executors_dir():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, "..", "executors")


def _current_os():
    if sys.platform.startswith("linux"):
        return "linux"
    if sys.platform == "darwin":
        return "darwin"
    if sys.platform.startswith("win"):
        return "windows"
    return sys.platform


def load_executor_spec(name, exec_dir=None):
    """读并校验 executor spec(§3.1);任何非法 → StoreError(fail-closed,exit 2)。

    校验: id 合法 / runtime.os 匹配当前平台 / invoke.sandbox == workspace-write
    (read-only 对 executor 非法) / invoke.binary 非空 / invoke.timeout_s 正整数。
    """
    base = exec_dir or executors_dir()
    spec_path = os.path.join(base, name, "spec.yaml")
    if not os.path.isfile(spec_path):
        raise StoreError(f"执行器 spec 不存在: {name}")
    try:
        spec = parse_yaml(read_file(spec_path), spec_path)
    except StoreError:
        raise
    if not isinstance(spec, dict):
        raise StoreError(f"{spec_path}: 顶层必须是映射")
    sid = spec.get("id")
    if not isinstance(sid, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", sid):
        raise StoreError(f"{spec_path}: 非法 id {sid!r}")
    rt = spec.get("runtime") or {}
    spec_os = rt.get("os")
    if not isinstance(spec_os, str) or not spec_os:
        raise StoreError(f"{spec_path}: 缺少 runtime.os")
    if spec_os != _current_os():
        raise StoreError(f"执行器 {name} 声明 os={spec_os} 与当前平台 {_current_os()} 不匹配")
    inv = spec.get("invoke") or {}
    if inv.get("sandbox") != "workspace-write":
        raise StoreError(f"执行器 {name} sandbox 必须 workspace-write(read-only 非法)")
    if not isinstance(inv.get("binary"), str) or not inv["binary"]:
        raise StoreError(f"{spec_path}: 缺少 invoke.binary")
    to = inv.get("timeout_s")
    if not isinstance(to, int) or isinstance(to, bool) or to <= 0:
        raise StoreError(f"{spec_path}: invoke.timeout_s 必须是正整数")
    return spec


def run_executor(wrapper, taskbook_path, workdir_path, out_dir, timeout, model=None):
    """调用 executor wrapper(§3.2 契约):<wrapper> --taskbook --workdir --out --timeout [--model NAME]。

    返回退出码(0/1/124/125 透传);wrapper 运行期间不持有 workitem 锁。
    外层超时 = 执行器 timeout + 60s 缓冲(防 wrapper 自身挂起,如 git 卡死)。
    model 非空时追加 --model(模型决策交给 wrapper:CLI > 任务书阈值 > 默认)。
    """
    cmd = [wrapper, "--taskbook", taskbook_path, "--workdir", workdir_path,
           "--out", out_dir, "--timeout", str(timeout)]
    if model:
        cmd += ["--model", model]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=timeout + 60)
    except subprocess.TimeoutExpired:
        return 124
    except OSError as e:
        raise UsageError(f"执行器 wrapper 调用失败: {wrapper}: {e.strerror or e}")
    return proc.returncode


# ---------------------------------------------------------------------------
# verify --auto 辅助(P3 §4):测试命令 / diff scope / 错误表
# ---------------------------------------------------------------------------

def parse_flow_block(wi_dir):
    """解析 taskbook.md 的 ```flow 前置块;无/不可解析 → None(触发回退,fail-closed)。"""
    path = os.path.join(wi_dir, "taskbook.md")
    if not os.path.isfile(path):
        return None
    text = read_file(path)
    m = re.search(r"```flow\s*\n(.*?)```", text, re.DOTALL)
    if not m:
        return None
    try:
        node = parse_yaml(m.group(1), f"{path}:```flow")
    except StoreError:
        return None
    return node if isinstance(node, dict) else None


def _probe_test_command(wdir):
    """惯例探测(§4.1 第 4 层):只读文件保守匹配,绝不执行 make/npm(审查补充意见 2)。"""
    makefile = os.path.join(wdir, "Makefile")
    if os.path.isfile(makefile):
        text = read_file(makefile)
        if re.search(r"^test\s*:", text, re.M):
            return "make test"
        if re.search(r"^check\s*:", text, re.M):
            return "make check"
    pkg = os.path.join(wdir, "package.json")
    if os.path.isfile(pkg):
        try:
            data = json.loads(read_file(pkg))
        except (ValueError, OSError):
            data = None
        if isinstance(data, dict) and isinstance(data.get("scripts"), dict) \
                and data["scripts"].get("test"):
            return "npm test"
    for fname in ("pytest.ini", "pyproject.toml", "setup.cfg"):
        p = os.path.join(wdir, fname)
        if os.path.isfile(p) and re.search(r"pytest|\[tool\.pytest", read_file(p)):
            return "pytest"
    if os.path.isdir(os.path.join(wdir, "tests")):
        return "python3 -m unittest discover tests"
    return None


def resolve_test_command(wi_dir, wdir, cli_command=None, result=None):
    """测试命令分层解析(§4.1):--test-command > result.json > 前置块 > 惯例 > 无。

    返回 {"command": str|None, "source": str|None, "reason": str|None};
    无来源 → reason="command_unresolved"(fail-closed)。test_command 必须是非空字符串。
    """
    if isinstance(cli_command, str) and cli_command:
        return {"command": cli_command, "source": "cli", "reason": None}
    if isinstance(result, dict) and isinstance(result.get("test_command"), str) \
            and result["test_command"]:
        return {"command": result["test_command"], "source": "result.json", "reason": None}
    fb = parse_flow_block(wi_dir)
    if isinstance(fb, dict) and isinstance(fb.get("test_command"), str) \
            and fb["test_command"]:
        return {"command": fb["test_command"], "source": "taskbook", "reason": None}
    detected = _probe_test_command(wdir)
    if detected:
        return {"command": detected, "source": "convention", "reason": None}
    return {"command": None, "source": None, "reason": "command_unresolved"}


def _scope_from_node(node):
    if not isinstance(node, dict):
        return [], [], False
    allow = node.get("allow") or []
    deny = node.get("deny") or []
    if not isinstance(allow, list) or not isinstance(deny, list):
        raise StoreError("diff_scope.allow/deny 必须是序列")
    allow = [str(x) for x in allow]
    deny = [str(x) for x in deny]
    return allow, deny, bool(allow or deny)


def _resolve_scope(wi_dir, cli_scope_file=None):
    """scope 解析优先级(§4.2):--scope FILE > 前置块 diff_scope > 无声明。"""
    if cli_scope_file:
        if not os.path.isfile(cli_scope_file):
            raise UsageError(f"--scope 文件不存在: {cli_scope_file}")
        try:
            node = parse_yaml(read_file(cli_scope_file), cli_scope_file)
        except StoreError as e:
            raise UsageError(f"--scope 文件解析失败: {e}")
        return _scope_from_node(node)
    fb = parse_flow_block(wi_dir)
    if isinstance(fb, dict) and isinstance(fb.get("diff_scope"), dict):
        return _scope_from_node(fb["diff_scope"])
    return [], [], False


def _collect_changed_files(result):
    """changed = changed_files ∪ untracked_files(过滤 .flow/ .git/)。"""
    diff = result.get("diff") or {}
    files = list(diff.get("changed_files") or [])
    for f in (diff.get("untracked_files") or []):
        if f not in files:
            files.append(f)
    out = []
    for f in files:
        s = str(f).lstrip("/")
        if s.startswith(".flow/") or s.startswith(".git/") or s == "":
            continue
        out.append(s)
    return out


def _matches_any(path, patterns):
    """路径匹配：pattern 以 / 结尾时匹配该目录整棵子树（fnmatch 对目录前缀不生效）。"""
    for p in patterns:
        if p.endswith("/"):
            if path == p.rstrip("/") or path.startswith(p):
                return True
        elif fnmatch.fnmatch(path, p):
            return True
    return False


def check_diff_scope(wi_dir, result, cli_scope_file=None):
    """diff 范围核对(§4.2):deny 命中 → out_of_scope;allow 存在则必须匹配。

    返回 {"match", "scope_verdict", "reason", "out_of_scope", "changed_files",
          "allow", "deny"}。
    """
    changed = _collect_changed_files(result)
    try:
        allow, deny, declared = _resolve_scope(wi_dir, cli_scope_file)
    except UsageError:
        raise
    except StoreError:
        raise
    if not declared:
        return {"match": False, "scope_verdict": "undeclared", "reason": "scope_undeclared",
                "out_of_scope": [], "changed_files": changed, "allow": allow, "deny": deny}
    out_of_scope = []
    for f in changed:
        if _matches_any(f, deny):
            out_of_scope.append(f)
            continue
        if allow and not _matches_any(f, allow):
            out_of_scope.append(f)
    match = len(out_of_scope) == 0
    verdict = "in_scope" if match else "out_of_scope"
    return {"match": match, "scope_verdict": verdict,
            "reason": None if match else "out_of_scope", "out_of_scope": out_of_scope,
            "changed_files": changed, "allow": allow, "deny": deny}


def _parse_error_table(text):
    """解析 design.md 的 markdown 错误处理表(§4.3):首列 E\\d+ 识别行。

    对分隔行/列数不一致容忍;整表无 E 行 → None(error_table_missing)。
    """
    rows = []
    for line in text.splitlines():
        s = line.strip()
        if not s.startswith("|"):
            continue
        cells = [c.strip() for c in s.strip("|").split("|")]
        if not cells:
            continue
        eid = cells[0]
        if not re.fullmatch(r"E\d+", eid):
            continue
        handle = cells[2] if len(cells) > 2 else ""
        test = cells[3] if len(cells) > 3 else ""
        scenario = cells[1] if len(cells) > 1 else ""
        rows.append({"id": eid, "scenario": scenario, "handle": handle, "test": test})
    return rows if rows else None


def check_error_table(wi_dir):
    """错误处理表核对(§4.3):结构覆盖自动(处理+测试覆盖非空)。

    返回 {"match", "reason", "total", "covered", "uncovered", "human_review"}。
    """
    path = os.path.join(wi_dir, "design.md")
    empty = {"match": False, "reason": "error_table_missing", "total": 0,
             "covered": 0, "uncovered": [], "human_review": []}
    if not os.path.isfile(path):
        return empty
    rows = _parse_error_table(read_file(path))
    if rows is None:
        return empty
    uncovered = [r["id"] for r in rows if not (r["handle"].strip() and r["test"].strip())]
    covered = len(rows) - len(uncovered)
    match = len(uncovered) == 0
    return {"match": match, "reason": None if match else "uncovered", "total": len(rows),
            "covered": covered, "uncovered": uncovered, "human_review": []}


def _derive_route(tests_result, diff_result, errors_result):
    """route 推导(§4.4 表):--route 未显式时按失败条件推导。"""
    if not tests_result.get("pass"):
        if tests_result.get("reason") == "command_unresolved":
            return "design"
        return "impl"
    if not diff_result.get("match"):
        if diff_result.get("reason") == "scope_undeclared":
            return "design"
        return "impl"
    if not errors_result.get("match"):
        return "design"
    return "impl"


def _git_available(wdir):
    try:
        proc = subprocess.run(["git", "-C", wdir, "rev-parse", "HEAD"],
                              capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        return False
    return proc.returncode == 0


def _run_tests(wdir, command):
    """在 workdir 运行测试命令;返回 {"pass", "exit_code", "output_tail", "reason"}。"""
    try:
        proc = subprocess.run(command, shell=True, cwd=wdir,
                              capture_output=True, text=True)
    except OSError as e:
        return {"pass": False, "exit_code": None, "output_tail": str(e), "reason": "command_failed"}
    rc = proc.returncode
    tail = ((proc.stdout or "") + (proc.stderr or ""))[-2000:]
    reason = "timeout" if rc == 124 else (None if rc == 0 else "test_failed")
    return {"pass": rc == 0, "exit_code": rc, "output_tail": tail, "reason": reason}


def _write_verify_md(wi_dir, verify):
    """写 verify.md(人读结论,§P3 §1.2/§4.4)。"""
    d = verify.get("details") or {}
    t = d.get("tests") or {}
    df = d.get("diff") or {}
    et = d.get("error_table") or {}
    lines = [
        "# 测试门结论",
        "",
        f"- tests_pass: {verify.get('tests_pass')}",
        f"- diff_match: {verify.get('diff_match')}",
        f"- error_table_match: {verify.get('error_table_match')}",
        f"- route: {verify.get('route')}",
        f"- checked_at: {verify.get('checked_at')}",
        "",
        "## 测试",
        f"- command: {t.get('command')}",
        f"- exit_code: {t.get('exit_code')}",
        f"- reason: {t.get('reason')}",
        "",
        "## diff",
        f"- scope_verdict: {df.get('scope_verdict')}",
        f"- out_of_scope: {df.get('out_of_scope')}",
        f"- changed_files: {df.get('changed_files')}",
        "",
        "## 错误表",
        f"- total: {et.get('total')}  covered: {et.get('covered')}  uncovered: {et.get('uncovered')}",
        "",
        "## 人工复核（advisory）",
        "- 语义正确性归 critic：处理是否正确、测试是否真正触发该错误，请人工复核。",
        "",
    ]
    _atomic_write(os.path.join(wi_dir, "verify.md"), "".join(lines))


# ---------------------------------------------------------------------------
# 输出辅助
# ---------------------------------------------------------------------------

def emit(obj):
    print(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), file=sys.stdout)


def emit_err(obj):
    print(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), file=sys.stderr)


def fail(code, error, detail=None, json_mode=False):
    if json_mode:
        obj = {"status": "failed", "error": error}
        if detail is not None:
            obj["detail"] = detail
        emit_err(obj)
    else:
        msg = f"错误: {error}"
        if detail is not None:
            msg += f" ({detail})"
        print(msg, file=sys.stderr)
    return code


def scan_args(args, value_opts):
    """拆 (positional, opts);value_opts 为带值选项名集合,其余 --x 视为 flag。"""
    positional = []
    opts = {}
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--":
            positional.extend(args[i + 1:])
            break
        if a.startswith("--"):
            body = a[2:]
            if "=" in body:
                k, v = body.split("=", 1)
                opts[k] = v
                i += 1
            elif body in value_opts:
                if i + 1 >= len(args):
                    raise UsageError(f"--{body} 缺少值")
                opts[body] = args[i + 1]
                i += 2
            else:
                opts[body] = True
                i += 1
        else:
            positional.append(a)
            i += 1
    return positional, opts


def _parse_bool(s):
    if s in ("true", "True", "1", "yes", "on"):
        return True
    if s in ("false", "False", "0", "no", "off"):
        return False
    raise UsageError(f"非布尔值: {s}")


# ---------------------------------------------------------------------------
# 命令实现(§3)
# ---------------------------------------------------------------------------

def cmd_new(args, cfg):
    pos, opts = scan_args(args, {"brief"})
    if not pos:
        raise UsageError("new 缺少 <id>")
    wi_id = pos[0]
    if len(pos) > 1:
        raise UsageError("new 多余参数")
    if not ID_RE.fullmatch(wi_id):
        raise UsageError(f"非法 id: {wi_id}")
    if len(wi_id) > int(cfg["workitem"].get("id_max_len", 64)):
        raise UsageError(f"id 超长: {wi_id}")
    json_mode = bool(opts.get("json"))
    force = bool(opts.get("force"))
    wi_dir = os.path.join(workitems_dir(), wi_id)

    if os.path.exists(wi_dir):
        if not force:
            raise UsageError(f"work-item 已存在: {wi_id}")
        shutil.rmtree(wi_dir)
    os.makedirs(wi_dir, exist_ok=True)

    # brief.md(§3.2)
    brief_path = os.path.join(wi_dir, "brief.md")
    if opts.get("brief"):
        src = opts["brief"]
        if not os.path.isfile(src):
            shutil.rmtree(wi_dir)
            raise UsageError(f"--brief 文件不存在: {src}")
        shutil.copyfile(src, brief_path)
    else:
        with open(brief_path, "w", encoding="utf-8") as f:
            f.write(f"# {wi_id}\n\n(待补充简报)\n")

    # status.yaml(§1.3)
    now = now_iso()
    status = {
        "schema_version": 1,
        "id": wi_id,
        "state": "created",
        "iteration": 0,
        "same_defect_count": 0,
        "primary_defect_type": None,
        "takeover": False,
        "re_execute_count": 0,
        "process_version": "1.0.0",
        "created_at": now,
        "updated_at": now,
        "event_seq": 1,
        "locked_by": None,
        "lock_expires_at": None,
    }
    save_status_atomic(wi_dir, status)
    append_event(wi_dir, {"ts": now, "event": "create", "from": None, "to": "created",
                          "actor": actor(cfg), "guard": None, "reason": None, "meta": {}})
    if json_mode:
        emit({"status": "ok", "id": wi_id, "path": wi_dir, "state": "created"})
    else:
        print(f"已创建 work-item: {wi_id} ({wi_dir})  state=created")
    return 0


def cmd_status(args, cfg):
    pos, opts = scan_args(args, set())
    if not pos:
        raise UsageError("status 缺少 <id>")
    wi_id = pos[0]
    json_mode = bool(opts.get("json"))
    wi_dir = resolve_wi_dir(wi_id, cfg)
    status = load_status(wi_dir)
    if json_mode:
        obj = {"id": wi_id, "state": status["state"], "iteration": status.get("iteration", 0),
               "same_defect_count": status.get("same_defect_count", 0),
               "takeover": status.get("takeover", False),
               "locked_by": status.get("locked_by"), "lock_expires_at": status.get("lock_expires_at"),
               "event_seq": status.get("event_seq"), "updated_at": status.get("updated_at")}
        if status.get("async") is not None:   # 存在时透出(增量输出,不碰现有字段)
            obj["async"] = status["async"]
        emit(obj)
    else:
        for k in ("id", "state", "iteration", "same_defect_count", "takeover",
                  "locked_by", "lock_expires_at", "event_seq", "updated_at"):
            print(f"{k}: {status.get(k)}")
    return 0


def cmd_transition(args, cfg):
    pos, opts = scan_args(args, {"event", "route"})
    if len(pos) < 2:
        raise UsageError("transition 缺少 <id> <to>")
    wi_id, to = pos[0], pos[1]
    if len(pos) > 2:
        raise UsageError("transition 多余参数")
    json_mode = bool(opts.get("json"))
    force = bool(opts.get("force"))
    explicit_event = opts.get("event")
    route_arg = opts.get("route")
    if route_arg is not None and route_arg not in ROUTES:
        raise UsageError(f"非法 --route: {route_arg}")
    if to not in STATES:
        raise UsageError(f"非法目标状态: {to}")
    wi_dir = resolve_wi_dir(wi_id, cfg)

    def _do():
        status = load_status(wi_dir)
        now = now_dt()
        if is_locked(status, now):
            raise Locked(status["locked_by"])
        from_state = status["state"]
        ev = resolve_event(from_state, to, explicit_event, status, wi_dir, cfg)
        if ev is None:
            raise UsageError(f"illegal_transition: {from_state} → {to}")
        overrides = {}
        if route_arg is not None:
            overrides["route"] = route_arg
        if opts.get("approve"):
            overrides["approve_confirmed"] = True
        res = _do_transition(wi_dir, from_state, to, ev, overrides, {}, cfg, force=force)
        if not res["ok"]:
            if res["reason"].startswith("guard_failed:"):
                if json_mode:
                    emit_err({"status": "failed", "id": wi_id, "from": from_state,
                              "to": to, "error": res["reason"], "detail": res.get("detail")})
                else:
                    print(f"错误: {res['reason']} ({res.get('detail')})", file=sys.stderr)
                return 1
            return fail(2, res["reason"], res.get("detail"), json_mode)
        if json_mode:
            emit({"status": "ok", "id": wi_id, "from": res["from"], "to": res["to"],
                  "event": res["event"], "guard": res["guard"], "event_seq": res["seq"]})
        else:
            print(f"{wi_id}: {res['from']} → {res['to']} (event={res['event']}, guard={res['guard']}, seq={res['seq']})")
        return 0

    try:
        return with_workitem_lock(wi_dir, _do)
    except Locked as e:
        return fail(2, f"锁被他人持有: {e}", None, json_mode)
    except UsageError as e:
        return fail(2, str(e), None, json_mode)
    except StoreError as e:
        return fail(2, str(e), None, json_mode)


def cmd_list(args, cfg):
    pos, opts = scan_args(args, {"state"})
    json_mode = bool(opts.get("json"))
    state_filter = opts.get("state")
    wd = workitems_dir()
    items = []
    if os.path.isdir(wd):
        for name in sorted(os.listdir(wd)):
            if not ID_RE.fullmatch(name):
                continue
            st_path = os.path.join(wd, name, "status.yaml")
            if not os.path.isfile(st_path):
                continue
            try:
                st = parse_yaml(read_file(st_path), st_path)
            except StoreError:
                continue
            if not isinstance(st, dict) or st.get("state") not in STATES:
                continue
            if state_filter and st.get("state") != state_filter:
                continue
            items.append({"id": name, "state": st.get("state"), "updated_at": st.get("updated_at")})
    items.sort(key=lambda x: x.get("updated_at") or "", reverse=True)
    if json_mode:
        emit({"items": items})
    else:
        for it in items:
            print(f"{it['id']}\t{it['state']}\t{it.get('updated_at')}")
    return 0


def cmd_log(args, cfg):
    pos, opts = scan_args(args, {"limit"})
    if not pos:
        raise UsageError("log 缺少 <id>")
    wi_id = pos[0]
    json_mode = bool(opts.get("json"))
    wi_dir = resolve_wi_dir(wi_id, cfg)
    events = []
    path = os.path.join(wi_dir, "events.jsonl")
    if os.path.isfile(path):
        for line in read_file(path).splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except ValueError:
                continue
    if opts.get("limit"):
        try:
            limit = int(opts["limit"])
        except ValueError:
            raise UsageError(f"非法 --limit: {opts['limit']}")
        events = events[-limit:]
    if json_mode:
        emit({"id": wi_id, "events": events})
    else:
        for e in events:
            print(f"{e.get('seq')}\t{e.get('event')}\t{e.get('from')}→{e.get('to')}\t{e.get('ts')}")
    return 0


def cmd_lock(args, cfg):
    pos, opts = scan_args(args, {"owner", "ttl"})
    if not pos:
        raise UsageError("lock 缺少 <id>")
    wi_id = pos[0]
    json_mode = bool(opts.get("json"))
    wi_dir = resolve_wi_dir(wi_id, cfg)
    owner = opts.get("owner") or actor(cfg)
    try:
        ttl_s = int(opts.get("ttl", cfg["workitem"].get("lock_ttl_s", 3600)))
    except (TypeError, ValueError):
        return fail(2, f"lock ttl 必须是整数秒: {opts.get('ttl')}", None, json_mode)
    if ttl_s < 0:
        return fail(2, f"lock ttl 不能为负数: {ttl_s}", None, json_mode)
    # ttl=0 合法:立即过期锁(测试制造过期场景用,见 C14)
    now = now_dt()
    try:
        s = acquire_lock(wi_dir, owner, ttl_s, now)
    except Locked as e:
        return fail(2, f"锁被他人持有: {e}", None, json_mode)
    if json_mode:
        emit({"status": "ok", "id": wi_id, "locked_by": owner,
              "lock_expires_at": s["lock_expires_at"]})
    else:
        print(f"{wi_id}: 已加锁 owner={owner} expires={s['lock_expires_at']}")
    return 0


def cmd_unlock(args, cfg):
    pos, opts = scan_args(args, {"owner"})
    if not pos:
        raise UsageError("unlock 缺少 <id>")
    wi_id = pos[0]
    json_mode = bool(opts.get("json"))
    force = bool(opts.get("force"))
    wi_dir = resolve_wi_dir(wi_id, cfg)
    owner = opts.get("owner") or actor(cfg)

    def _do():
        s = load_status(wi_dir)
        if s.get("locked_by") and s["locked_by"] != owner and not force:
            raise Locked(s["locked_by"])
        prev = s.get("locked_by")
        s["locked_by"] = None
        s["lock_expires_at"] = None
        save_status_atomic(wi_dir, s)
        append_event(wi_dir, {"ts": now_iso(), "event": "unlock", "from": s["state"],
                              "to": s["state"], "actor": owner, "guard": None, "reason": None,
                              "meta": {"owner": owner, "forced": force, "prev": prev}})
        return s
    try:
        s = with_workitem_lock(wi_dir, _do)
    except Locked as e:
        return fail(2, f"锁持有者不匹配: {e}", None, json_mode)
    if json_mode:
        emit({"status": "ok", "id": wi_id, "locked_by": None})
    else:
        print(f"{wi_id}: 已解锁")
    return 0


ARTIFACTS = {
    "brief": "brief.md", "design": "design.md", "review": "review.md",
    "decision": "decision.yaml", "taskbook": "taskbook.md", "status": "status.yaml",
    "events": "events.jsonl", "verify": "verify.md", "retro": "retro.jsonl",
    "verify.json": "executor/verify.json",
}


def cmd_show(args, cfg):
    pos, opts = scan_args(args, set())
    if len(pos) < 2:
        raise UsageError("show 缺少 <id> <artifact>")
    wi_id, artifact = pos[0], pos[1]
    wi_dir = resolve_wi_dir(wi_id, cfg)
    rel = ARTIFACTS.get(artifact)
    if rel is None:
        raise UsageError(f"未知工件: {artifact}")
    path = os.path.join(wi_dir, rel)
    if not os.path.isfile(path):
        raise WorkitemError(f"工件不存在: {artifact}")
    if rel.endswith(".yaml"):
        sys.stdout.write(dump_yaml(parse_yaml(read_file(path), path)))
    else:
        sys.stdout.write(read_file(path))
    return 0


def cmd_guard(args, cfg):
    pos, opts = scan_args(args, {"gate"})
    if not pos:
        raise UsageError("guard 缺少 <id>")
    wi_id = pos[0]
    json_mode = bool(opts.get("json"))
    gate = opts.get("gate")
    if gate not in ("quality", "test"):
        raise UsageError("--gate 必须是 quality 或 test")
    wi_dir = resolve_wi_dir(wi_id, cfg)
    if gate == "quality":
        d = read_decision(wi_dir)
        verdict = d.get("verdict")
        ok, reason = evaluate_guard("quality_pass", {"verdict": verdict})
        result = {"id": wi_id, "gate": "quality", "pass": ok, "verdict": verdict, "reason": reason}
    else:
        v = read_verify(wi_dir)
        ctx = {"tests_pass": v.get("tests_pass"), "diff_match": v.get("diff_match"),
               "error_table_match": v.get("error_table_match"), "route": v.get("route")}
        ok, reason = evaluate_guard("test_gate_pass", ctx)
        result = {"id": wi_id, "gate": "test", "pass": ok, "reason": reason}
    if json_mode:
        emit(result)
    else:
        print(f"{wi_id} gate={gate} pass={result['pass']} reason={result.get('reason')}")
    return 0


def cmd_verify(args, cfg):
    pos, opts = scan_args(args, {"tests", "diff", "errors", "route", "test-command", "scope"})
    if not pos:
        raise UsageError("verify 缺少 <id>")
    wi_id = pos[0]
    json_mode = bool(opts.get("json"))
    wi_dir = resolve_wi_dir(wi_id, cfg)
    if opts.get("auto"):
        return _cmd_verify_auto(wi_id, wi_dir, opts, json_mode, cfg)
    tests = _parse_bool(opts["tests"]) if "tests" in opts else True
    diff = _parse_bool(opts["diff"]) if "diff" in opts else True
    errors = _parse_bool(opts["errors"]) if "errors" in opts else True
    route = opts.get("route")
    if route is not None and route not in ROUTES:
        raise UsageError(f"非法 --route: {route}")
    all_pass = tests and diff and errors
    if all_pass:
        route = None
    else:
        if route is None:
            raise UsageError("测试门失败时必须指定 --route design|impl")
    os.makedirs(os.path.join(wi_dir, "executor"), exist_ok=True)
    v = {"schema_version": 1, "tests_pass": tests, "diff_match": diff,
         "error_table_match": errors, "route": route, "checked_at": now_iso(), "details": {}}
    with open(os.path.join(wi_dir, "executor", "verify.json"), "w", encoding="utf-8") as f:
        json.dump(v, f, ensure_ascii=False, separators=(",", ":"))
    if json_mode:
        emit({"id": wi_id, "verify": v, "gate_pass": all_pass})
    else:
        print(f"{wi_id}: 测试门 {'通过' if all_pass else '失败'} route={route}")
    return 0


def cmd_decision(args, cfg):
    pos, opts = scan_args(args, {"verdict", "defect-type", "summary"})
    if not pos:
        raise UsageError("decision 缺少 <id>")
    wi_id = pos[0]
    json_mode = bool(opts.get("json"))
    verdict = opts.get("verdict")
    if verdict not in VERDICTS:
        raise UsageError("--verdict 必须是 pass|reject|takeover")
    defect_type = opts.get("defect-type")
    summary = opts.get("summary")
    if verdict in ("reject", "takeover") and defect_type is None:
        raise UsageError(f"{verdict} 时必须指定 --defect-type")
    if defect_type is not None and defect_type not in DEFECT_TYPES:
        raise UsageError(f"非法 --defect-type: {defect_type}")
    wi_dir = resolve_wi_dir(wi_id, cfg)
    d = {"schema_version": 1, "verdict": verdict, "primary_defect_type": defect_type,
         "reviewer": None}
    if summary is not None:
        d["summary"] = summary
    if verdict in ("reject", "takeover"):
        d["defects"] = [{"type": defect_type, "detail": summary or "", "caught_by": "review"}]
    with open(os.path.join(wi_dir, "decision.yaml"), "w", encoding="utf-8") as f:
        f.write(dump_yaml(d))
    if json_mode:
        emit({"status": "ok", "id": wi_id, "verdict": verdict,
              "primary_defect_type": defect_type})
    else:
        print(f"{wi_id}: decision verdict={verdict} defect_type={defect_type}")
    return 0


# ---------------------------------------------------------------------------
# P3 命令:design / execute / verify --auto(§P3 §2)
# ---------------------------------------------------------------------------

def _invoke_design(flow_config, wdir, brief_path, out_dir):
    """调用 flow-config → dsh-design(pro/read-only/回验/redact 继承),同步与异步共用。

    返回 {"rc", "info", "error", "detail", "stdout", "stderr"}:
      rc=0 且 info=dict(成功 JSON 输出);rc=2 仅用于「无法调用 flow-config」(OSError);
      rc=124 硬超时;其余 rc 为 dsh 真实退出码(1/2),error="dsh-design 失败"。
    """
    cmd = [flow_config, "-d", wdir, "-o", out_dir, "--json", brief_path]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True)
    except OSError as e:
        return {"rc": 2, "info": None, "error": f"无法调用 flow-config: {e}",
                "detail": None, "stdout": "", "stderr": ""}
    if proc.returncode == 124:
        return {"rc": 124, "info": None, "error": "设计超时",
                "detail": None, "stdout": proc.stdout, "stderr": proc.stderr}
    if proc.returncode != 0:
        detail = (proc.stderr or "").strip().splitlines()
        return {"rc": proc.returncode, "info": None, "error": "dsh-design 失败",
                "detail": detail[-1] if detail else None,
                "stdout": proc.stdout, "stderr": proc.stderr}
    try:
        info = json.loads(proc.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return {"rc": 1, "info": None, "error": "dsh-design 输出非 JSON",
                "detail": None, "stdout": proc.stdout, "stderr": proc.stderr}
    if not isinstance(info, dict):
        return {"rc": 1, "info": None, "error": "dsh-design 输出非 JSON 对象",
                "detail": None, "stdout": proc.stdout, "stderr": proc.stderr}
    return {"rc": 0, "info": info, "error": None, "detail": None,
            "stdout": proc.stdout, "stderr": proc.stderr}


# ---------------------------------------------------------------------------
# design 异步支持(--async / --check;async-design-support 方案)
# ---------------------------------------------------------------------------

def _pid_alive(pid):
    """探活:仅一处 os.kill(pid, 0);进程不存在/无权限/非法 → False。"""
    try:
        os.kill(int(pid), 0)
        return True
    except (ProcessLookupError, PermissionError, ValueError, TypeError):
        return False


def async_phase(status, result, now, pid_alive):
    """async 设计纯判定(零文件 I/O):result=None 表示 design-async-result.json 缺失。

    status 需含 async 块(pid/started_at/expected_seconds);pid_alive 由调用方算好传入。
    完成判定只看 result.json(单真相源);pid 存活仅用于区分「运行中 vs 崩溃」。
    返回 {"phase", "elapsed_seconds", "over_expected", "alarm", "remaining_seconds"}。
    """
    a = status.get("async") or {}
    started = a.get("started_at")
    raw_expected = a.get("expected_seconds")
    try:
        expected = int(raw_expected) if raw_expected is not None else None
    except (TypeError, ValueError):
        raise StoreError("status.async.expected_seconds 非法")
    if started:
        try:
            started_dt = parse_iso(started)
        except (TypeError, ValueError):
            raise StoreError("status.async.started_at 非法")
        elapsed = max(0, int((now - started_dt).total_seconds()))
    else:
        elapsed = 0
    remaining = max(0, (expected or 0) - elapsed)
    over = bool(expected) and elapsed > expected

    if result is not None:
        ec = result.get("exit_code")
        if ec == 0:
            phase = "done_ok"
        elif ec == 124:
            phase = "done_timeout"
        else:
            phase = "done_failed"
    elif pid_alive:
        phase = "over_expected" if over else "running"
    else:
        phase = "crashed"
    alarm = "timeout" if phase in ("over_expected", "done_timeout") else None
    return {"phase": phase, "elapsed_seconds": elapsed, "over_expected": over,
            "alarm": alarm, "remaining_seconds": remaining}


def _read_result(wi_dir):
    """读 design-async-result.json:缺失 → None;损坏 → StoreError(fail-closed)。"""
    path = os.path.join(wi_dir, "design-async-result.json")
    if not os.path.isfile(path):
        return None
    try:
        data = json.loads(read_file(path))
    except ValueError:
        raise StoreError(f"{path}: design-async-result.json 损坏(非 JSON)")
    if not isinstance(data, dict):
        raise StoreError(f"{path}: design-async-result.json 顶层必须是对象")
    return data


def _append_log(wi_dir, text):
    """worker 追加审计日志 design-async.log(始终保留)。"""
    if not text:
        return
    path = os.path.join(wi_dir, "design-async.log")
    with open(path, "a", encoding="utf-8", errors="replace") as f:
        f.write(text)
        if not text.endswith("\n"):
            f.write("\n")


def spawn_worker(wi_dir, brief_path, wdir, flow_config, log_path, started_at, expected_s):
    """起后台 worker(--async-worker,setssid 孤儿)并返回 Popen 句柄;不 wait。

    时序:先 spawn 得真实 pid,由调用方锁内写 status.async 标记;worker 与宿主
    生命周期解耦。expected_seconds/started_at 经 env FLOW_ASYNC_* 传 worker。
    """
    env = dict(os.environ)
    env["FLOW_ASYNC_STARTED_AT"] = started_at
    env["FLOW_ASYNC_EXPECTED_S"] = str(expected_s)
    log_fd = open(log_path, "ab")
    try:
        popen_kw = {}
        if os.name == "posix":
            popen_kw["start_new_session"] = True
        proc = subprocess.Popen(
            [sys.executable, os.path.abspath(__file__), "--async-worker",
             wi_dir, brief_path, wdir, flow_config],
            stdin=subprocess.DEVNULL, stdout=log_fd, stderr=subprocess.STDOUT,
            env=env, **popen_kw)
    finally:
        log_fd.close()
    return proc


def _cmd_design_async(wi_id, wi_dir, brief_path, json_mode, cfg, opts):
    """--async:后台起 worker + 锁内写 async 标记 + 立即返回 exit 0。"""
    expected_raw = opts.get("expected")
    if expected_raw is not None:
        try:
            expected = int(expected_raw)
        except (TypeError, ValueError):
            raise UsageError(f"--expected 必须是正整数: {expected_raw}")
    else:
        try:
            expected = int(cfg["workitem"].get("design_expected_seconds", 480))
        except (TypeError, ValueError):
            expected = 480
    if expected <= 0:
        raise UsageError("--expected 必须是正整数")

    status = load_status(wi_dir)
    a = status.get("async") or {}
    result_path = os.path.join(wi_dir, "design-async-result.json")
    in_flight = (a.get("pid") is not None and a.get("finished_at") is None
                 and not os.path.isfile(result_path) and _pid_alive(a["pid"]))
    if in_flight:
        return fail(2, "已有进行中的 async 设计(先 design --check)", None, json_mode)

    flow_config = os.path.join(os.path.dirname(os.path.abspath(__file__)), "flow-config")
    wdir = workdir()
    started_at = now_iso()
    log_path = os.path.join(wi_dir, "design-async.log")
    proc = spawn_worker(wi_dir, brief_path, wdir, flow_config, log_path,
                        started_at, expected)

    def _do():
        s = load_status(wi_dir)
        if is_locked(s, now_dt()):
            raise Locked(s["locked_by"])
        # 锁内复查 async 状态（TOCTOU 防御：并发 --async 都过了锁外检查时，这里拦截后到者）
        a = s.get("async") or {}
        still_in_flight = (a.get("pid") is not None and a.get("finished_at") is None
                           and not os.path.isfile(result_path) and _pid_alive(a["pid"]))
        if still_in_flight:
            raise InFlight()
        s["async"] = {"pid": proc.pid, "started_at": started_at,
                      "expected_seconds": expected, "finished_at": None, "exit_code": None}
        save_status_atomic(wi_dir, s)
        return s
    try:
        s = with_workitem_lock(wi_dir, _do)
    except InFlight:
        return fail(2, "已有进行中的 async 设计(先 design --check)", None, json_mode)
    except Locked as e:
        # 极端竞态:spawn 后锁被他人持有。worker 为孤儿会完成并写 result.json(审计留档),
        # 但无 async 标记 → 用户可 --async 重跑(fail-safe,不杀 worker)。
        return fail(2, f"锁被他人持有: {e}", None, json_mode)

    if json_mode:
        emit({"status": "ok", "id": wi_id, "state": s["state"],
              "async": s["async"], "log": log_path})
    else:
        print(f"{wi_id}: async 设计已启动 pid={proc.pid} expected={expected}s "
              f"(log: {log_path})")
    return 0


def _cmd_design_check(wi_id, wi_dir, json_mode, cfg):
    """--check:幂等查询 → 完成则落盘 design.md/design-result.json 并锁内转 designed。"""
    status = load_status(wi_dir)
    if status["state"] == "designed":
        if json_mode:
            emit({"status": "ok", "id": wi_id, "state": "designed",
                  "no_transition": True, "message": "already designed"})
        else:
            print(f"{wi_id}: already designed")
        return 0
    if status["state"] != "created":
        return fail(2, f"design 前置状态不符: {status['state']}", None, json_mode)
    a = status.get("async") or {}
    if a.get("pid") is None:
        return fail(2, "无进行中的 async 设计(先 design --async)", None, json_mode)
    pid = a["pid"]

    try:
        result = _read_result(wi_dir)
    except StoreError as e:
        return fail(1, str(e), None, json_mode)
    ph = async_phase(status, result, now_dt(), _pid_alive(pid))
    log_path = os.path.join(wi_dir, "design-async.log")

    if ph["phase"] == "running":
        if json_mode:
            emit({"status": "running", "id": wi_id, "state": status["state"],
                  "async": a, "pid": pid, "log": log_path,
                  "elapsed_seconds": ph["elapsed_seconds"],
                  "remaining_seconds": ph["remaining_seconds"],
                  "expected_seconds": a.get("expected_seconds")})
        else:
            print(f"{wi_id}: async 设计运行中 pid={pid} "
                  f"elapsed={ph['elapsed_seconds']}s "
                  f"remaining={ph['remaining_seconds']}s (log: {log_path})")
        return 3
    if ph["phase"] == "over_expected":
        if json_mode:
            emit({"status": "running", "alarm": "timeout", "over_expected": True,
                  "id": wi_id, "state": status["state"], "async": a, "pid": pid,
                  "log": log_path, "elapsed_seconds": ph["elapsed_seconds"],
                  "expected_seconds": a.get("expected_seconds")})
        else:
            print(f"{wi_id}: async 设计已超预期时长仍在运行 "
                  f"elapsed={ph['elapsed_seconds']}s "
                  f"expected={a.get('expected_seconds')}s (log: {log_path})",
                  file=sys.stderr)
        return 124
    if ph["phase"] == "done_timeout":
        return fail(124, "设计超时", None, json_mode)
    if ph["phase"] in ("done_failed", "crashed"):
        if result is None:
            err = "后台进程异常退出(无完成记录)"
        else:
            err = result.get("error") or "dsh-design 失败"
        return fail(1, err, None, json_mode)

    # done_ok:落盘 design.md + design-result.json → 锁内转 designed(与同步同源)
    design_path = result.get("design")
    if not design_path or not os.path.isfile(design_path):
        return fail(1, "dsh-design 未产出方案文件", None, json_mode)
    content = read_file(design_path)
    if not content.strip():
        return fail(1, "dsh-design 产出空方案", None, json_mode)
    _atomic_write(os.path.join(wi_dir, "design.md"), content)
    design_result = {"schema_version": 1, "adapter": "dsh-designer",
                     "model": result.get("model"), "session": result.get("session"),
                     "usage": result.get("usage"), "duration_s": result.get("duration_s"),
                     "source": os.path.basename(design_path), "checked_at": now_iso()}
    _atomic_write(os.path.join(wi_dir, "design-result.json"),
                  json.dumps(design_result, ensure_ascii=False, separators=(",", ":")))
    shutil.rmtree(os.path.join(wi_dir, ".design-async"), ignore_errors=True)

    meta = {"adapter": "dsh-designer", "model": result.get("model")}

    def _do():
        s = load_status(wi_dir)
        if s["state"] != "created":
            return {"ok": True, "from": s["state"], "to": s["state"], "event": "design",
                    "guard": None, "seq": None, "no_transition": True}
        sa = s.setdefault("async", {})
        sa["finished_at"] = result.get("finished_at")
        sa["exit_code"] = 0
        save_status_atomic(wi_dir, s)
        return _do_transition(wi_dir, "created", "designed", "design", {}, meta, cfg)

    try:
        res = with_workitem_lock(wi_dir, _do)
    except Locked as e:
        return fail(2, f"锁被他人持有: {e}", None, json_mode)
    if not res["ok"]:
        return fail(1 if res["reason"].startswith("guard_failed:") else 2,
                    res["reason"], res.get("detail"), json_mode)
    if json_mode:
        emit({"status": "ok", "id": wi_id, "from": res["from"], "to": res["to"],
              "event": "design", "guard": res.get("guard"),
              "design": os.path.relpath(os.path.join(wi_dir, "design.md"), workdir()),
              "model": result.get("model"), "usage": result.get("usage"),
              "duration_s": result.get("duration_s"), "async": load_status(wi_dir).get("async")})
    else:
        print(f"{wi_id}: async 设计完成 → designed (model={result.get('model')})")
    return 0


def _translate_task_add_error(proc, json_mode):
    """flow task add 非零退出 → 映射退出码(§0.3):duplicate_workitem→2(detail 含已有 task id);
    写盘失败→1;其余用法/前置/StoreError→2;stderr 不可解析→1(fail-closed)。"""
    lines = (proc.stderr or "").strip().splitlines()
    text = lines[-1] if lines else ""
    error, detail = None, None
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            error = obj.get("error")
            detail = obj.get("detail")
    except (ValueError, TypeError):
        pass
    if error == "duplicate_workitem":
        return fail(2, "duplicate_workitem", detail or "同 workitem 已有非终态任务", json_mode)
    if error is not None and str(error).startswith("写盘失败"):
        return fail(1, f"入队失败: {error}", detail, json_mode)
    if error is not None:
        return fail(2, f"入队被拒: {error}", detail, json_mode)
    return fail(1, "flow task add 失败", text[:200] or None, json_mode)


def _enqueue_workitem_op(cfg, sub, wi_id, inner_argv, kind, json_mode, state_before,
                         timeout=None):
    """M2 默认 async-first:构造 --sync 命令串 → flow task add 入队 → 返回 task_id。

    命令串只拼 flow 绝对路径 + 白名单 id/选项(shlex.quote),不拼数据路径
    (FLOW_DATA_DIR/FLOW_WORKDIR/DSH_* 经 env 继承透传,runner 及 sh -c 子进程天然继承)。
    --sync 子命令是断递归关键:runner 执行同步分支,不再触发入队。
    退出码:0 入队成功 / 1 子进程不可调用或写盘失败 / 2 重复入队、state 守卫等前置。
    """
    flow_bin = os.path.join(os.path.dirname(os.path.abspath(__file__)), "flow")
    command = " ".join(shlex.quote(a) for a in ([flow_bin] + inner_argv))
    if DENY_RE.search(command):
        return fail(2, "入队命令含疑似 secret, 拒绝入队", None, json_mode)
    # flow-task-ledger 门禁(§1.6):补 --priority/--why/--expected-seconds,否则新门禁打断
    # async-first 入队(E25);命令串仍命中 FLOW_WORKITEM_RE(白名单),无需 --force。
    # _SEED_FALLBACK 为 cfg 缺 task 块(测试/旧调用)时的硬编码兜底,与 config/defaults.yaml 对齐。
    # 2026-08-22 收窄:仅 design/execute 入队(verify/review/batch 已不入队)。
    task_cfg = cfg.get("task") or {}
    _SEED_FALLBACK = {"design": 480, "execute": 1800}
    cmd = [flow_bin, "task", "add", "--command", command,
           "--workitem", wi_id, "--kind", kind,
           "--priority", str(task_cfg.get("default_priority") or "P2"),
           "--why", f"workitem {sub} {wi_id}"]
    if kind == "execute":
        cmd += ["--expected-seconds", str(timeout + 115)]  # 外层安全网 ≥ 内层 timeout+缓冲
    else:
        seed = (task_cfg.get("expected_seconds_seed") or {}).get(kind)
        cmd += ["--expected-seconds", str(seed if seed else _SEED_FALLBACK.get(kind, 480))]
    cmd += ["--json"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True)
    except OSError as e:
        return fail(1, f"无法调用 flow task add: {e}", None, json_mode)
    if proc.returncode != 0:
        return _translate_task_add_error(proc, json_mode)
    try:
        info = json.loads(proc.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return fail(1, "flow task add 输出非 JSON", None, json_mode)
    if not isinstance(info, dict) or not info.get("id"):
        return fail(1, "flow task add 输出缺 task id", None, json_mode)
    task_id = info["id"]
    if json_mode:
        emit({"status": "ok", "id": wi_id, "state": state_before, "queued": True,
              "task_id": task_id, "event": f"events/{task_id}.jsonl",
              "expected_seconds": info.get("expected_seconds")})
    else:
        print(f"{wi_id}: {sub} 已入队 task={task_id} "
              f"(后台执行;flow task status {task_id} 查询)")
    return 0


def async_worker_main(argv):
    """内部隐藏入口(--async-worker):后台 worker 契约,用户不可见(不出现在 USAGE)。

    argv = [wi_dir, brief_path, wdir, flow_config];expected_seconds/started_at 经
    env FLOW_ASYNC_* 传入。本进程退出码恒 0(记录写盘成功),设计结果由
    design-async-result.json 的 exit_code 承载,由 --check 判定。
    """
    wi_dir, brief_path, wdir, flow_config = argv
    out_dir = os.path.join(wi_dir, ".design-async")
    shutil.rmtree(out_dir, ignore_errors=True)
    os.makedirs(out_dir, exist_ok=True)
    r = _invoke_design(flow_config, wdir, brief_path, out_dir)
    _append_log(wi_dir, (r["stdout"] or "") + (r["stderr"] or ""))
    try:
        expected = int(os.environ.get("FLOW_ASYNC_EXPECTED_S") or 480)
    except ValueError:
        expected = 480
    rec = {"schema_version": 1, "id": os.path.basename(wi_dir),
           "pid": os.getpid(),
           "started_at": os.environ.get("FLOW_ASYNC_STARTED_AT") or now_iso(),
           "expected_seconds": expected,
           "exit_code": r["rc"], "finished_at": now_iso(),
           "design": (r["info"] or {}).get("design"),
           "model": (r["info"] or {}).get("model"),
           "session": (r["info"] or {}).get("session"),
           "usage": (r["info"] or {}).get("usage"),
           "duration_s": (r["info"] or {}).get("duration_s"),
           "turns": (r["info"] or {}).get("turns"),
           "error": r["error"]}
    _atomic_write(os.path.join(wi_dir, "design-async-result.json"),
                  json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
    return 0


def cmd_design(args, cfg):
    pos, opts = scan_args(args, {"expected"})
    if not pos:
        raise UsageError("design 缺少 <id>")
    wi_id = pos[0]
    if len(pos) > 1:
        raise UsageError("design 多余参数")
    json_mode = bool(opts.get("json"))
    force = bool(opts.get("force"))
    async_mode = bool(opts.get("async"))
    check_mode = bool(opts.get("check"))
    sync_mode = bool(opts.get("sync"))
    if async_mode and check_mode:
        raise UsageError("--async 与 --check 互斥")
    if sync_mode and (async_mode or check_mode):
        raise UsageError("--sync 与 --async/--check 互斥")
    if opts.get("expected") is not None and not async_mode:
        raise UsageError("--expected 仅用于 --async")
    wi_dir = resolve_wi_dir(wi_id, cfg)
    if check_mode:
        return _cmd_design_check(wi_id, wi_dir, json_mode, cfg)

    status = load_status(wi_dir)
    if not force and status["state"] != "created":
        return fail(2, f"前置状态不符: design 需要 state=created 实际 {status['state']}", None, json_mode)
    brief_path = os.path.join(wi_dir, "brief.md")
    if not file_nonempty(brief_path):
        return fail(2, "brief.md 为空或缺失", None, json_mode)
    if async_mode:
        return _cmd_design_async(wi_id, wi_dir, brief_path, json_mode, cfg, opts)
    if not sync_mode:
        # ── M2 默认 async-first:入队后台(命令串 = flow 绝对路径 + --sync 子命令) ──
        return _enqueue_workitem_op(
            cfg, "design", wi_id,
            ["workitem", "design", wi_id, "--sync"]
            + (["--force"] if force else []) + ["--json"],
            "design", json_mode, status["state"])

    # ── 同步路径(行为与现状逐分支一致;仅把 flow-config 调用提取为 _invoke_design) ──
    flow_config = os.path.join(os.path.dirname(os.path.abspath(__file__)), "flow-config")
    wdir = workdir()
    tmp_out = tempfile.mkdtemp(prefix="flow-design-")
    r = _invoke_design(flow_config, wdir, brief_path, tmp_out)
    if r["rc"] == 2 and r["error"].startswith("无法调用 flow-config"):
        shutil.rmtree(tmp_out, ignore_errors=True)
        return fail(2, r["error"], None, json_mode)
    if r["rc"] == 124:
        shutil.rmtree(tmp_out, ignore_errors=True)
        return fail(124, "设计超时", None, json_mode)
    if r["rc"] != 0:
        shutil.rmtree(tmp_out, ignore_errors=True)
        return fail(1, r["error"], r.get("detail"), json_mode)
    info = r["info"]
    design_src = info.get("design")
    if not design_src or not os.path.isfile(design_src):
        shutil.rmtree(tmp_out, ignore_errors=True)
        return fail(1, "dsh-design 未产出方案文件", None, json_mode)
    content = read_file(design_src)
    if not content.strip():
        shutil.rmtree(tmp_out, ignore_errors=True)
        return fail(1, "dsh-design 产出空方案", None, json_mode)

    # 原子落盘 design.md + design-result.json(审查补充意见 1:force 重跑同步覆盖)
    _atomic_write(os.path.join(wi_dir, "design.md"), content)
    design_result = {"schema_version": 1, "adapter": "dsh-designer",
                     "model": info.get("model"), "session": info.get("session"),
                     "usage": info.get("usage"), "duration_s": info.get("duration_s"),
                     "source": os.path.basename(design_src), "checked_at": now_iso()}
    _atomic_write(os.path.join(wi_dir, "design-result.json"),
                  json.dumps(design_result, ensure_ascii=False, separators=(",", ":")))
    shutil.rmtree(tmp_out, ignore_errors=True)

    meta = {"adapter": "dsh-designer", "model": info.get("model")}

    def _do():
        s = load_status(wi_dir)
        if s["state"] != "created":
            # force 重跑且已非 created:仅覆盖文件,不转移
            return {"ok": True, "from": s["state"], "to": s["state"], "event": "design",
                    "guard": None, "seq": None, "no_transition": True}
        return _do_transition(wi_dir, "created", "designed", "design", {}, meta, cfg, force=force)

    try:
        res = with_workitem_lock(wi_dir, _do)
    except Locked as e:
        return fail(2, f"锁被他人持有: {e}", None, json_mode)
    if not res["ok"]:
        return fail(1 if res["reason"].startswith("guard_failed:") else 2,
                    res["reason"], res.get("detail"), json_mode)
    design_rel = os.path.relpath(os.path.join(wi_dir, "design.md"), wdir)
    if json_mode:
        emit({"status": "ok", "id": wi_id, "from": res["from"], "to": res["to"],
              "event": "design", "guard": res.get("guard"), "design": design_rel,
              "model": info.get("model"), "usage": info.get("usage"),
              "duration_s": info.get("duration_s")})
    else:
        print(f"{wi_id}: design → {design_rel} (model={info.get('model')})")
    return 0


def cmd_execute(args, cfg):
    pos, opts = scan_args(args, {"executor", "timeout", "model"})
    if not pos:
        raise UsageError("execute 缺少 <id>")
    wi_id = pos[0]
    if len(pos) > 1:
        raise UsageError("execute 多余参数")
    json_mode = bool(opts.get("json"))
    force = bool(opts.get("force"))
    sync_mode = bool(opts.get("sync"))
    executor_name = opts.get("executor") or cfg["executor"].get("default", "reasonix")
    wi_dir = resolve_wi_dir(wi_id, cfg)

    status = load_status(wi_dir)
    if not force and status["state"] != "translated":
        return fail(2, f"前置状态不符: execute 需要 state=translated 实际 {status['state']}", None, json_mode)
    taskbook_path = os.path.join(wi_dir, "taskbook.md")
    if not file_nonempty(taskbook_path):
        return fail(2, "taskbook.md 为空或缺失", None, json_mode)
    wdir = workdir()
    if not _git_available(wdir):
        return fail(2, "需要 git", None, json_mode)

    try:
        spec = load_executor_spec(executor_name)
    except StoreError as e:
        return fail(2, str(e), None, json_mode)

    if opts.get("timeout") is not None:
        try:
            timeout = int(opts["timeout"])
        except ValueError:
            return fail(2, f"非法 --timeout: {opts['timeout']}", None, json_mode)
        if timeout <= 0:
            return fail(2, f"--timeout 必须是正整数: {timeout}", None, json_mode)
    else:
        timeout = int(spec["invoke"].get("timeout_s", 1800))

    model = opts.get("model")
    if model is not None and not model:
        return fail(2, "--model 不能为空", None, json_mode)

    out_dir = os.path.join(wi_dir, "executor")
    os.makedirs(out_dir, exist_ok=True)
    wrapper = os.path.join(executors_dir(), executor_name, "wrapper.sh")
    if not os.path.isfile(wrapper):
        return fail(2, f"执行器 wrapper 缺失: {executor_name}", None, json_mode)

    if not sync_mode:
        # ── M2 默认 async-first:入队后台(命令串 = flow 绝对路径 + --sync 子命令) ──
        # 外层 expected-seconds = timeout+115(≥ 内层执行器 timeout + wrapper 缓冲,防抢先杀)
        return _enqueue_workitem_op(
            cfg, "execute", wi_id,
            ["workitem", "execute", wi_id, "--sync", "--executor", executor_name,
             "--timeout", str(timeout)]
            + (["--model", model] if model else [])
            + (["--force"] if force else []) + ["--json"],
            "execute", json_mode, status["state"], timeout=timeout)

    # 调 wrapper(执行器子进程运行期间不持 workitem 锁)
    run_executor(wrapper, taskbook_path, wdir, out_dir, timeout, model=model)

    result_path = os.path.join(out_dir, "result.json")
    if not os.path.isfile(result_path):
        return fail(2, "executor/result.json 缺失", None, json_mode)
    try:
        raw = read_file(result_path)
    except OSError as e:
        return fail(2, f"executor/result.json 不可读: {e}", None, json_mode)
    if DENY_RE.search(raw):
        return fail(2, "executor/result.json 含疑似 secret", None, json_mode)
    try:
        result = json.loads(raw)
    except ValueError as e:
        return fail(2, f"executor/result.json 损坏: {e}", None, json_mode)
    if not isinstance(result, dict):
        return fail(2, "executor/result.json 顶层必须是映射", None, json_mode)

    st = result.get("status")
    if st == "ok" and not os.path.isfile(os.path.join(out_dir, "diff.patch")):
        return fail(2, "executor/diff.patch 缺失", None, json_mode)
    if st == "ok":
        meta = {"executor": executor_name, "duration_s": result.get("duration_s")}

        def _do():
            s = load_status(wi_dir)
            if s["state"] != "translated":
                return {"ok": True, "from": s["state"], "to": s["state"], "event": "execute",
                        "guard": None, "seq": None, "no_transition": True}
            return _do_transition(wi_dir, "translated", "executed", "execute", {}, meta, cfg, force=force)

        try:
            res = with_workitem_lock(wi_dir, _do)
        except Locked as e:
            return fail(2, f"锁被他人持有: {e}", None, json_mode)
        if not res["ok"]:
            return fail(1 if res["reason"].startswith("guard_failed:") else 2,
                        res["reason"], res.get("detail"), json_mode)
        if json_mode:
            emit({"status": "ok", "id": wi_id, "from": res["from"], "to": res["to"],
                  "event": "execute", "guard": res.get("guard"), "executor": executor_name,
                  "exit_code": result.get("exit_code", 0), "diff": result.get("diff")})
        else:
            print(f"{wi_id}: execute → executed (executor={executor_name}, exit={result.get('exit_code')})")
        return 0
    if st == "failed":
        if json_mode:
            emit_err({"status": "failed", "id": wi_id, "error": "执行器失败",
                      "detail": result.get("redacted_logs") or result.get("error")})
        else:
            print("错误: 执行器失败", file=sys.stderr)
        return 1
    if st == "timeout":
        return fail(124, "执行器超时", None, json_mode)
    if st == "partial-complete":
        # 不转移(state 留 translated 可 resume 重跑),exit 124 透传超时语义
        if json_mode:
            emit_err({"status": "failed", "id": wi_id,
                      "error": "执行器部分完成(超时但产出非空)",
                      "hint": "rx --continue 续收尾后重跑 execute,或人工验收 diff 后 verify 走快速路",
                      "detail": result.get("redacted_logs") or ""})
        else:
            print("提示: 执行器超时但产出非空(partial-complete)——可 rx --continue 续收尾后重跑 execute;或人工验收 diff 后 verify 走快速路", file=sys.stderr)
        return 124
    return fail(2, f"非法 result.status: {st!r}", None, json_mode)


def _cmd_verify_auto(wi_id, wi_dir, opts, json_mode, cfg):
    no_transition = bool(opts.get("no-transition"))
    route_override = opts.get("route")
    if route_override is not None and route_override not in ROUTES:
        return fail(2, f"非法 --route: {route_override}", None, json_mode)

    status = load_status(wi_dir)
    if status["state"] != "executed":
        return fail(2, f"前置状态不符: verify --auto 需要 state=executed 实际 {status['state']}（先 flow workitem execute）", None, json_mode)
    result_path = os.path.join(wi_dir, "executor", "result.json")
    diff_path = os.path.join(wi_dir, "executor", "diff.patch")
    if not os.path.isfile(result_path):
        return fail(2, "executor/result.json 缺失", None, json_mode)
    if not os.path.isfile(diff_path):
        return fail(2, "executor/diff.patch 缺失", None, json_mode)
    try:
        result = json.loads(read_file(result_path))
    except (ValueError, OSError) as e:
        return fail(2, f"executor/result.json 损坏: {e}", None, json_mode)
    if not isinstance(result, dict):
        return fail(2, "executor/result.json 顶层必须是映射", None, json_mode)

    wdir = workdir()

    # 1) tests(§4.1)
    tc = resolve_test_command(wi_dir, wdir, cli_command=opts.get("test-command"), result=result)
    if tc["command"] is None:
        tests_result = {"pass": False, "command": None, "exit_code": None,
                        "output_tail": "", "reason": "command_unresolved", "source": None}
    else:
        run = _run_tests(wdir, tc["command"])
        tests_result = {"pass": run["pass"], "command": tc["command"],
                        "exit_code": run.get("exit_code"), "output_tail": run.get("output_tail", ""),
                        "reason": run.get("reason"), "source": tc["source"]}
    tests_pass = tests_result["pass"]

    # 2) diff(§4.2)
    try:
        diff_result = check_diff_scope(wi_dir, result, cli_scope_file=opts.get("scope"))
    except (UsageError, StoreError) as e:
        return fail(2, str(e), None, json_mode)
    diff_match = diff_result["match"]

    # 3) errors(§4.3)
    errors_result = check_error_table(wi_dir)
    error_table_match = errors_result["match"]

    gate_pass = tests_pass and diff_match and error_table_match

    # 4) route 推导(§4.4)
    route = route_override
    if route is None and not gate_pass:
        route = _derive_route(tests_result, diff_result, errors_result)

    # 5) 写 verify.json + verify.md
    verify = {"schema_version": 1, "tests_pass": tests_pass, "diff_match": diff_match,
              "error_table_match": error_table_match, "route": route if not gate_pass else None,
              "checked_at": now_iso(),
              "details": {
                  "tests": {"command": tests_result.get("command"), "exit_code": tests_result.get("exit_code"),
                            "output_tail": tests_result.get("output_tail", ""),
                            "reason": tests_result.get("reason"), "source": tests_result.get("source")},
                  "diff": {"scope_verdict": diff_result["scope_verdict"],
                           "out_of_scope": diff_result["out_of_scope"],
                           "changed_files": diff_result["changed_files"],
                           "allow": diff_result["allow"], "deny": diff_result["deny"],
                           "reason": diff_result["reason"]},
                  "error_table": {"total": errors_result["total"], "covered": errors_result["covered"],
                                  "uncovered": errors_result["uncovered"],
                                  "human_review": errors_result["human_review"],
                                  "reason": errors_result["reason"]},
              }}
    os.makedirs(os.path.join(wi_dir, "executor"), exist_ok=True)
    _atomic_write(os.path.join(wi_dir, "executor", "verify.json"),
                  json.dumps(verify, ensure_ascii=False, separators=(",", ":")))
    _write_verify_md(wi_dir, verify)

    if no_transition:
        if json_mode:
            emit({"status": "ok" if gate_pass else "failed", "id": wi_id,
                  "gate": {"tests_pass": tests_pass, "diff_match": diff_match,
                           "error_table_match": error_table_match}, "no_transition": True})
        else:
            print(f"{wi_id}: 测试门 {'通过' if gate_pass else '失败'}（--no-transition 不转移）")
        return 0 if gate_pass else 1

    if gate_pass:
        def _do():
            return _do_transition(wi_dir, "executed", "verified", "verify", {}, {}, cfg)
        try:
            res = with_workitem_lock(wi_dir, _do)
        except Locked as e:
            return fail(2, f"锁被他人持有: {e}", None, json_mode)
        if not res["ok"]:
            return fail(1 if res["reason"].startswith("guard_failed:") else 2,
                        res["reason"], res.get("detail"), json_mode)
        if json_mode:
            emit({"status": "ok", "id": wi_id,
                  "gate": {"tests_pass": tests_pass, "diff_match": diff_match,
                           "error_table_match": error_table_match},
                  "transition": {"from": res["from"], "to": res["to"], "event": "verify",
                                 "guard": res["guard"]}})
        else:
            print(f"{wi_id}: 测试门通过 → verified")
        return 0

    # 门未过 → 按 route 打回
    to = "designed" if route == "design" else "executed"

    def _do_fail():
        return _do_transition(wi_dir, "executed", to, "verify_fail",
                              {"route": route}, {"route": route}, cfg)
    try:
        res = with_workitem_lock(wi_dir, _do_fail)
    except Locked as e:
        return fail(2, f"锁被他人持有: {e}", None, json_mode)
    if not res["ok"]:
        return fail(1 if res["reason"].startswith("guard_failed:") else 2,
                    res["reason"], res.get("detail"), json_mode)
    if json_mode:
        emit_err({"status": "failed", "id": wi_id,
                  "gate": {"tests_pass": tests_pass, "diff_match": diff_match,
                           "error_table_match": error_table_match},
                  "route": route,
                  "transition": {"from": res["from"], "to": res["to"], "event": "verify_fail"}})
    else:
        print(f"{wi_id}: 门未过 → {res['to']} (route={route})", file=sys.stderr)
    return 1


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

USAGE = """flow workitem <sub> ...

子命令:
  new      <id> [--brief FILE] [--force] [--json]
  status   <id> [--json]
  transition <id> <to> [--event E] [--route design|impl] [--approve] [--guard-check] [--force] [--json]
  list     [--state S] [--json]
  log      <id> [--limit N] [--json]
  lock     <id> [--owner ID] [--ttl SECONDS] [--json]
  unlock   <id> [--owner ID] [--force] [--json]
  show     <id> <artifact>
  guard    <id> --gate quality|test [--json]
  design   <id> [--async [--expected N]] [--check] [--sync] [--force] [--json]
  execute  <id> [--sync] [--executor NAME] [--timeout N] [--model NAME] [--force] [--json]
  verify   <id> [--auto] [--tests b] [--diff b] [--errors b] [--route design|impl] [--test-command CMD] [--scope FILE] [--no-transition] [--json]
  decision <id> --verdict pass|reject|takeover [--defect-type T] [--summary S] [--json]
"""

SUBCOMMANDS = {
    "new": cmd_new, "status": cmd_status, "transition": cmd_transition,
    "list": cmd_list, "log": cmd_log, "lock": cmd_lock, "unlock": cmd_unlock,
    "show": cmd_show, "guard": cmd_guard, "verify": cmd_verify, "decision": cmd_decision,
    "design": cmd_design, "execute": cmd_execute,
}


def main(argv):
    if argv and argv[0] == "--async-worker":   # 内部隐藏入口,仅由 design --async spawn
        return async_worker_main(argv[1:])
    if not argv or argv[0] in ("-h", "--help", "help"):
        sys.stdout.write(USAGE)
        return 0 if argv else 2
    if argv[0] != "workitem":
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
        cfg = load_config()
    except StoreError as e:
        return fail(2, f"配置解析失败: {e}", None, json_mode)
    except OSError as e:
        return fail(2, f"配置不可用: {e}", None, json_mode)
    try:
        return fn(rest[1:], cfg)
    except UsageError as e:
        return fail(2, str(e), None, json_mode)
    except WorkitemError as e:
        return fail(2, str(e), None, json_mode)
    except StoreError as e:
        return fail(2, str(e), None, json_mode)
    except OSError as e:
        return fail(1, f"写盘失败: {e}", None, json_mode)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
