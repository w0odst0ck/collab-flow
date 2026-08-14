#!/usr/bin/env python3
"""flow-config-core.py —— collab-flow 配置加载层核心(P1)。

职责(对应设计方案 20260814-112812-474265110-P1 §2):
  1. 受限 YAML 子集解析(§2.2)—— 零依赖,显式拒绝锚点/标签/块/流式集合/序列等歧义语法;
  2. deep_merge(§2.3)—— user 覆盖 defaults,映射递归合并,b 侧 null 视为未设置;
  3. 校验(§2.4)—— fail-closed,key 只做 ${VAR} 语法校验,绝不物化/打印 key 值;
  4. env 渲染(§2.5)—— 显式映射表,环境已设即跳过(shlex.quote 单引号转义)。

用法: flow-config-core.py render <defaults.yaml> <user-config-path> <out-env-file>
退出码: 0 成功 / 2 任何配置错误(严格区分「缺失=静默回退」与「存在但非法=exit 2」)。

红线: 本文件绝不打印任何 key 的值;prices 默认 null(关闭金额估算);
      version 未知主版本、模型/provider 名注入字符、permission/timeout 违反 → exit 2。
"""

import json
import os
import re
import shlex
import sys

VERSION = 1                     # 受支持的 schema 主版本
KEY_RE = re.compile(r"^[A-Za-z0-9._-]+$")                     # 标量键
ENV_REF_RE = re.compile(r"^\$\{[A-Za-z_][A-Za-z0-9_]*\}$")    # ${VAR} 环境引用
SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")  # 模型/provider 安全字符集
PLAIN_KEY_RE = re.compile(r"^[A-Za-z0-9]{9,}$")               # 纯字母数字长串(疑似明文)
PERMISSIONS = ("read-only", "workspace-write")
PRICE_KEYS = ("input", "output", "cache_read", "reasoning")


class YamlError(Exception):
    """一般 YAML 语法错误(用户配置 → 「配置解析失败」)。"""


class UnsupportedError(YamlError):
    """受限子集之外的不支持语法(→ 「不支持: …」)。"""


class ConfigError(Exception):
    """schema/明文/绑定校验失败(→ 「配置校验失败: …」)。"""


# ---------------------------------------------------------------------------
# 受限 YAML 解析(§2.2)
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
    """显式拒绝受限子集之外的 YAML 语法(fail-closed,杜绝静默错读)。

    ${VAR} 环境引用受支持(§2.2),其内部花括号不视为流式集合。
    """
    for ch in ("&", "*", "|", ">"):
        if ch in content:
            raise UnsupportedError(f"{src}:{lineno}: 不支持的 YAML 语法 '{ch}'")
    if "!!" in content:
        raise UnsupportedError(f"{src}:{lineno}: 不支持的 YAML 标签 '!!'")
    if content.startswith("- "):
        raise UnsupportedError(f"{src}:{lineno}: 不支持的序列 '-' (P1 仅映射)")
    i = 0
    n = len(content)
    while i < n:
        ch = content[i]
        if ch == "$" and i + 1 < n and content[i + 1] == "{":
            j = content.find("}", i + 2)
            if j == -1:
                raise UnsupportedError(f"{src}:{lineno}: 未闭合的环境引用 '${{'")
            i = j + 1
            continue
        if ch in ("{", "}", "[", "]"):
            raise UnsupportedError(f"{src}:{lineno}: 不支持的 YAML 语法 '{ch}'(流式集合)")
        i += 1


def parse_scalar(raw, src, lineno):
    """受限标量:null/true/false/int/float/引号字符串/裸字符串(含 ${VAR})。"""
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
    return s                                   # 裸字符串(含 ~/... 路径与 ${VAR})


def parse_block(lines, idx, indent, src):
    """递归解析缩进映射块;返回 (node, 下一行下标)。"""
    node = {}
    n = len(lines)
    while idx < n:
        raw = lines[idx]
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            idx += 1
            continue
        cur = leading_spaces(raw)
        if "\t" in raw[:cur]:
            raise YamlError(f"{src}:{idx + 1}: 不支持 tab 缩进(统一空格)")
        if cur < indent:
            break
        if cur > indent:
            raise YamlError(f"{src}:{idx + 1}: 意外缩进(层级跳变)")
        content = raw[cur:]
        check_rejected(content, src, idx + 1)
        colon = find_key_colon(content)
        if colon is None:
            raise YamlError(f"{src}:{idx + 1}: 缺少 ':'(非映射行)")
        key = content[:colon].strip()
        if not KEY_RE.fullmatch(key):
            raise YamlError(f"{src}:{idx + 1}: 非法键名 '{key}'")
        if key in node:
            raise YamlError(f"{src}:{idx + 1}: 重复键 '{key}'")
        rest = split_comment(content[colon + 1:]).strip()
        if rest == "":
            idx += 1
            if idx < n and leading_spaces(lines[idx]) > cur:
                child, idx = parse_block(lines, idx, leading_spaces(lines[idx]), src)
                node[key] = child
            else:
                node[key] = None                # 裸空值 = null
        else:
            check_rejected(rest, src, idx + 1)
            node[key] = parse_scalar(rest, src, idx + 1)
            idx += 1
    return node, idx


def parse_yaml(text, src):
    """入口:文件级指令检查 + 顶层映射解析。"""
    lines = text.split("\n")
    if lines and lines[0].startswith("\ufeff"):
        lines[0] = lines[0][1:]
    for i, ln in enumerate(lines):
        ls = ln.lstrip()
        if ls.startswith("%"):
            raise UnsupportedError(f"{src}:{i + 1}: 不支持的文档指令 '%'")
        if ls.startswith("---") or ls.startswith("..."):
            raise UnsupportedError(f"{src}:{i + 1}: 不支持的文档分隔符")
    node, idx = parse_block(lines, 0, 0, src)
    for j in range(idx, len(lines)):
        if lines[j].strip():
            raise YamlError(f"{src}:{j + 1}: 意外内容(缩进或结构错误)")
    return node


def read_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


# ---------------------------------------------------------------------------
# 合并(§2.3)
# ---------------------------------------------------------------------------

def deep_merge(a, b):
    """映射递归合并;标量叶子 b 覆盖 a;b 侧 null 视为「未设置」→ 保留 a。"""
    if isinstance(b, dict):
        out = dict(a) if isinstance(a, dict) else {}
        for k, v in b.items():
            if isinstance(v, dict) and isinstance(out.get(k), dict):
                out[k] = deep_merge(out[k], v)
            elif v is None:
                continue                       # b 侧 null 不覆盖
            else:
                out[k] = v
        return out
    return b if b is not None else a


# ---------------------------------------------------------------------------
# 校验(§2.4, fail-closed)
# ---------------------------------------------------------------------------

def _is_number(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def validate(m):
    """merged 配置校验;任何违反 → ConfigError → exit 2。"""
    v = m.get("version")
    if v is None or isinstance(v, bool) or v != VERSION:
        raise ConfigError(f"不支持的配置版本 {v!r}(仅支持主版本 {VERSION})")

    roles = m.get("roles")
    if not isinstance(roles, dict) or not isinstance(roles.get("designer"), dict):
        raise ConfigError("缺少 roles.designer(必须是映射)")
    d = roles["designer"]

    model = d.get("model")
    if not isinstance(model, dict):
        raise ConfigError("缺少 roles.designer.model(必须是映射)")
    for field, label in (("pro", "pro 模型"), ("flash", "flash 模型"), ("provider", "provider")):
        val = model.get(field)
        if not isinstance(val, str) or not SAFE_NAME_RE.fullmatch(val):
            raise ConfigError(
                f"非法{label}名 {val!r}(仅允许 [A-Za-z0-9][A-Za-z0-9._-]{{0,63}},防注入)")

    to = d.get("timeout_s")
    if not isinstance(to, int) or isinstance(to, bool) or to <= 0:
        raise ConfigError(f"timeout_s 必须是正整数: {to!r}")

    perm = d.get("permission")
    if perm not in PERMISSIONS:
        raise ConfigError(f"非法 permission {perm!r}(仅 {PERMISSIONS[0]}/{PERMISSIONS[1]})")

    patch = d.get("patch")
    if not isinstance(patch, dict) or not isinstance(patch.get("path"), str) or not patch["path"]:
        raise ConfigError("roles.designer.patch.path 必须是非空字符串")

    key = d.get("key")
    if not isinstance(key, dict):
        raise ConfigError("缺少 roles.designer.key(必须是映射)")
    # 明文拒绝:key.* 任何值匹配 ^sk- 或长度>8 的纯字母数字串 → 拒绝(绝不物化 key 值)
    for kk, vv in key.items():
        if isinstance(vv, str) and (vv.startswith("sk-") or PLAIN_KEY_RE.fullmatch(vv)):
            raise ConfigError(f"禁止明文 key 写入: {kk}(只允许 ${{VAR}} 环境引用)")
    ref = key.get("env_ref")
    if not isinstance(ref, str) or not ENV_REF_RE.fullmatch(ref):
        raise ConfigError(f"key.env_ref 必须是 ${{VAR}} 环境引用: {ref!r}")
    if ref != "${DEEPSEEK_API_KEY}":
        raise ConfigError(f"P1 仅支持 ${{DEEPSEEK_API_KEY}} 绑定: {ref}")
    src = key.get("source")
    if not isinstance(src, str) or not src:
        raise ConfigError("key.source 必须是非空字符串")

    prices = d.get("prices")
    if prices is not None:
        if not isinstance(prices, dict):
            raise ConfigError(f"prices 必须是 null 或映射: {prices!r}")
        for kk in PRICE_KEYS:
            vv = prices.get(kk)
            if not _is_number(vv):
                raise ConfigError(f"prices.{kk} 必须是数值: {vv!r}")

    paths = m.get("paths")
    if not isinstance(paths, dict):
        raise ConfigError("缺少 paths(必须是映射)")
    for field in ("dsh_bin", "dsh_home"):
        pv = paths.get(field)
        if not isinstance(pv, str) or not pv:
            raise ConfigError(f"paths.{field} 必须是非空字符串")
    ss = paths.get("stats_script")
    if ss is not None and not isinstance(ss, str):
        raise ConfigError("paths.stats_script 必须是 null 或字符串")


# ---------------------------------------------------------------------------
# env 渲染(§2.5 显式映射表)
# ---------------------------------------------------------------------------

def expand_tilde(v, home):
    """~ 展开(仅 ~ 与 ~/ 前缀;~user 形式不支持,保持原样即被字符串使用)。"""
    if v == "~":
        return home
    if v.startswith("~/"):
        return home + v[1:]
    return v


def render_env(m, outfile):
    """按导出规则写 env 赋值文件:null/空不导出;环境已设即跳过;shlex.quote。"""
    home = os.environ.get("HOME", "")
    d = m["roles"]["designer"]
    envmap = [
        ("DSH_DESIGN_PRO_MODEL", d["model"]["pro"], False),
        ("DSH_DESIGN_FLASH_MODEL", d["model"]["flash"], False),
        ("DSH_DESIGN_PROVIDER", d["model"]["provider"], False),
        ("DSH_DESIGN_TIMEOUT", d["timeout_s"], False),
        ("DSH_DESIGN_PERMISSION", d["permission"], False),
        ("DSH_DESIGN_PRO_PATCH", d["patch"]["path"], True),
        ("DSH_DESIGN_KEY_FILE", d["key"]["source"], True),
        ("DSH_DESIGN_PRICES", d["prices"], False),
        ("DSH_BIN", m["paths"]["dsh_bin"], False),
        ("DSH_HOME", m["paths"]["dsh_home"], True),
        ("DSH_DESIGN_STATS", m["paths"]["stats_script"], False),
    ]
    lines = []
    for var, val, tilde in envmap:
        if val is None or val == "":
            continue                             # 空值不导出,保留 dsh-design 内部默认
        if var in os.environ and os.environ[var] != "":
            continue                             # 环境已设 → env 优先,不覆盖
        if var == "DSH_DESIGN_PRICES":
            val = json.dumps(val, ensure_ascii=False, separators=(",", ":"))
        elif tilde:
            val = expand_tilde(str(val), home)
        else:
            val = str(val)
        if re.search(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", val):
            raise ConfigError(f"配置值含控制字符, 拒绝导出: {var}")
        lines.append(f"export {var}={shlex.quote(val)}")
    with open(outfile, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + ("\n" if lines else ""))


# ---------------------------------------------------------------------------
# 主流程(§2.3 伪代码 1-3 步;第 4 步 exec 由 flow-config 完成)
# ---------------------------------------------------------------------------

def main(argv):
    if len(argv) != 4 or argv[0] != "render":
        print("用法: flow-config-core.py render <defaults.yaml> <user-config-path> <out-env-file>",
              file=sys.stderr)
        return 2
    _, defaults_path, user_path, out_file = argv

    # 1) 解析 defaults:自身损坏/不可读 → exit 2,默认档不可缺失(E8)
    try:
        defaults = parse_yaml(read_file(defaults_path), defaults_path)
    except OSError as e:
        print(f"默认配置不可用: {defaults_path}: {e.strerror or e}", file=sys.stderr)
        return 2
    except YamlError as e:
        print(f"默认配置不可用: {defaults_path}: {e}", file=sys.stderr)
        return 2
    except RecursionError:
        print(f"默认配置不可用: {defaults_path}: 缩进层级过深", file=sys.stderr)
        return 2
    if not isinstance(defaults, dict):
        print(f"默认配置不可用: {defaults_path}: 顶层必须是映射", file=sys.stderr)
        return 2

    # 2) 解析 user:缺失 → 静默回退(E1);存在但非法/不可读 → exit 2(E2/E3/E7)
    merged = defaults
    if os.path.exists(user_path):
        if not os.path.isfile(user_path):
            print(f"配置不可读: {user_path}: 不是普通文件", file=sys.stderr)
            return 2
        try:
            user_text = read_file(user_path)
        except OSError as e:
            print(f"配置不可读: {user_path}: {e.strerror or e}", file=sys.stderr)
            return 2
        try:
            user = parse_yaml(user_text, user_path)
        except UnsupportedError as e:
            print(f"不支持: {e}", file=sys.stderr)
            return 2
        except YamlError as e:
            print(f"配置解析失败: {e}", file=sys.stderr)
            return 2
        except RecursionError:
            print(f"配置解析失败: {user_path}: 缩进层级过深", file=sys.stderr)
            return 2
        if not isinstance(user, dict):
            print(f"配置解析失败: {user_path}: 顶层必须是映射", file=sys.stderr)
            return 2
        merged = deep_merge(defaults, user)

    # 3) 校验 + 渲染(E4/E5/E6/E11)
    try:
        validate(merged)
    except ConfigError as e:
        print(f"配置校验失败: {e}", file=sys.stderr)
        return 2
    try:
        render_env(merged, out_file)
    except ConfigError as e:
        print(f"配置渲染失败: {e}", file=sys.stderr)
        return 2
    except OSError as e:
        print(f"无法写出环境文件 {out_file}: {e.strerror or e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
