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
退出码: 0 成功 / 1 运行失败(守卫拒绝、写盘失败) / 2 用法或前置错误 / 124 预留。

红线: 本文件零个人标识(actor/owner 用 flow:<plane_id>,不取 hostname/用户名);
      id 白名单正则硬编码;meta 写入前过 deny-list;transition()/evaluate_guard() 零 I/O。
"""

import json
import os
import re
import secrets
import shutil
import sys
from datetime import datetime, timedelta, timezone

try:
    import fcntl
except ImportError:  # 非 POSIX 降级(§4.2)
    fcntl = None

# ---------------------------------------------------------------------------
# 常量(§1.2/§2.1)
# ---------------------------------------------------------------------------

ID_RE = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")   # id 白名单,硬编码不进 config
DENY_RE = re.compile(r"sk-[A-Za-z0-9]{10}")        # meta 疑似 secret(§1.4)

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
    """解析 list-of-mapping;返回 (node, 下一行下标)。"""
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
            raise StoreError(f"{src}:{idx + 1}: 序列项需为 mapping('- key: value')")
        if not rest.startswith(" "):
            raise StoreError(f"{src}:{idx + 1}: 序列项需为 mapping('- key: value')")
        check_rejected(content, src, idx + 1)
        item, idx = _parse_list_item(lines, idx, cur, src, rest[1:])
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
    return {"workitem": workitem, "gates": gates}


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
        emit({"id": wi_id, "state": status["state"], "iteration": status.get("iteration", 0),
              "same_defect_count": status.get("same_defect_count", 0),
              "takeover": status.get("takeover", False),
              "locked_by": status.get("locked_by"), "lock_expires_at": status.get("lock_expires_at"),
              "event_seq": status.get("event_seq"), "updated_at": status.get("updated_at")})
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
        ctx = build_ctx(wi_dir, ev, overrides, cfg)
        if ev == "verify_fail" and ctx.get("route") is None:
            ctx["route"] = "design" if to == "designed" else "impl"
        r = transition(from_state, ev, ctx, force=force)
        if not r["ok"]:
            if r["reason"].startswith("guard_failed:"):
                if json_mode:
                    emit_err({"status": "failed", "id": wi_id, "from": from_state,
                              "to": to, "error": r["reason"], "detail": r.get("detail")})
                else:
                    print(f"错误: {r['reason']} ({r.get('detail')})", file=sys.stderr)
                return 1
            return fail(2, r["reason"], r.get("detail"), json_mode)
        # 应用 effects + 清隐式短锁(§4.3)
        apply_effects(status, r["effects"])
        status["state"] = r["to"]
        status["updated_at"] = now.isoformat(timespec="seconds")
        status["locked_by"] = None
        status["lock_expires_at"] = None
        meta = {}
        if force:
            meta["forced"] = True
        if ev in ("translate", "feedback", "takeover"):
            meta["verdict"] = ctx.get("verdict")
        if ev == "verify_fail":
            meta["route"] = ctx.get("route")
        # 顺序:append_event 先取 seq → 镜像进 status → 原子写(status 含最新 seq)。
        # status 写失败时 events 已追加,由下次 transition 的 seq 漂移检测以 events 为准自愈(§4.2)。
        seq = append_event(wi_dir, {"ts": now.isoformat(timespec="seconds"), "event": ev,
                                    "from": from_state, "to": r["to"], "actor": actor(cfg),
                                    "guard": r["guard"], "reason": None, "meta": meta})
        status["event_seq"] = seq
        save_status_atomic(wi_dir, status)
        if json_mode:
            emit({"status": "ok", "id": wi_id, "from": from_state, "to": to,
                  "event": ev, "guard": r["guard"], "event_seq": seq})
        else:
            print(f"{wi_id}: {from_state} → {to} (event={ev}, guard={r['guard']}, seq={seq})")
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
    pos, opts = scan_args(args, {"tests", "diff", "errors", "route"})
    if not pos:
        raise UsageError("verify 缺少 <id>")
    wi_id = pos[0]
    json_mode = bool(opts.get("json"))
    wi_dir = resolve_wi_dir(wi_id, cfg)
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
  verify   <id> [--tests b] [--diff b] [--errors b] [--route design|impl] [--json]
  decision <id> --verdict pass|reject|takeover [--defect-type T] [--summary S] [--json]
"""

SUBCOMMANDS = {
    "new": cmd_new, "status": cmd_status, "transition": cmd_transition,
    "list": cmd_list, "log": cmd_log, "lock": cmd_lock, "unlock": cmd_unlock,
    "show": cmd_show, "guard": cmd_guard, "verify": cmd_verify, "decision": cmd_decision,
}


def main(argv):
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
